import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone

import requests
import psycopg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

DATABASE_URL = os.environ.get("DATABASE_URL")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "10"))

BL_API_URL = os.environ.get("BL_API_URL", "https://api.baselinker.com/connector.php")
BL_TOKEN = os.environ.get("BL_TOKEN", "")
SYNC_SECRET = os.environ.get("SYNC_SECRET", "")

STATUS_MAP = json.loads(os.environ.get("STATUS_MAP_JSON", json.dumps({
    "NUEVOS": ["Nuevos pedidos"],
    "RECEPCION": ["Agendados", "A distribuir", "error de etiqueta"],
    "PREPARACION": ["Turbo", "Flex", "Colecta", "ME1", "Recolectando"],
    "EMBALADO": ["Puesto 1", "Puesto 2", "Puesto 3", "Puesto 4", "Puesto 5", "Puesto 6", "Embalado"],
    "DESPACHO": ["Mercado Envíos", "Logística propia", "Blitzz", "Zeta", "Eliazar", "Federico"],
    "ENVIADO": ["Enviado", "Envidado"],
    "ENTREGADO": ["Entregado"],
    "CERRADOS": ["Cancelado", "Full", "Pedidos antiguos"]
})))


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurado")
    return psycopg.connect(DATABASE_URL)


@app.on_event("startup")
def ensure_schema():
    # Crea tablas/índices sin que tengas que correr SQL a mano
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS order_events (
              id BIGSERIAL PRIMARY KEY,
              order_id TEXT NOT NULL,
              status TEXT NOT NULL,
              event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS orders_current (
              order_id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS sync_state (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );
            """)
            cur.execute("""
            INSERT INTO sync_state(key, value)
            VALUES ('last_sync_ts','0')
            ON CONFLICT (key) DO NOTHING;
            """)

            # Índices para que /api/metrics no se arrastre
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_event_ts ON order_events(event_ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_status ON order_events(status);")


def bl_call(method: str, params: dict):
    if not BL_TOKEN:
        raise RuntimeError("BL_TOKEN no configurado")

    r = requests.post(
        BL_API_URL,
        headers={"X-BLToken": BL_TOKEN},
        data={"method": method, "parameters": json.dumps(params)},
        timeout=12,  # clave para evitar 502
    )
    r.raise_for_status()
    out = r.json()
    if out.get("status") != "SUCCESS":
        raise RuntimeError(str(out))
    return out


def get_last_sync_ts(cur) -> int:
    cur.execute("SELECT value FROM sync_state WHERE key='last_sync_ts'")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def set_last_sync_ts(cur, ts: int):
    cur.execute("""
    INSERT INTO sync_state(key, value)
    VALUES ('last_sync_ts', %s)
    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
    """, (str(ts),))


def run_sync_job():
    """
    Job real (pesado). Corre en background para que /sync responda rápido.
    """
    # 1) mapa status_id -> nombre
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    status_map = {int(s["id"]): s.get("name", str(s["id"])) for s in status_list if "id" in s}

    with get_conn() as conn:
        with conn.cursor() as cur:
            last_ts = get_last_sync_ts(cur)

    # overlap para no perder cambios por clock skew
    from_ts = max(0, last_ts - 3600)  # 1h
    now_ts = int(time.time())

    changed = 0
    events = 0
    loops = 0

    while True:
        loops += 1
        # getOrders max 100 por respuesta; hacemos loop avanzando from_ts
        data = bl_call("getOrders", {
            "date_confirmed_from": from_ts,
            "get_unconfirmed_orders": True
        })
        orders = data.get("orders", [])

        if not orders:
            break

        # Para avanzar el cursor temporal, buscamos el máximo date_confirmed si existe
        # (si no existe, hacemos un solo loop)
        max_date = from_ts

        with get_conn() as conn:
            with conn.cursor() as cur:
                for o in orders:
                    order_id = str(o.get("order_id") or o.get("id") or "")
                    if not order_id:
                        continue

                    raw_status_id = o.get("order_status_id")
                    if raw_status_id is None:
                        # fallback (por si viene nombre)
                        status_name = o.get("order_status_name") or o.get("order_status") or o.get("status")
                        if not status_name:
                            continue
                    else:
                        try:
                            status_name = status_map.get(int(raw_status_id), str(raw_status_id))
                        except Exception:
                            status_name = str(raw_status_id)

                    # avance por fecha confirmada si viene
                    dc = o.get("date_confirmed") or o.get("date_add") or 0
                    try:
                        dc_int = int(dc)
                        if dc_int > max_date:
                            max_date = dc_int
                    except Exception:
                        pass

                    # dedupe por pedido
                    cur.execute("SELECT status FROM orders_current WHERE order_id=%s", (order_id,))
                    prev = cur.fetchone()
                    prev_status = prev[0] if prev else None

                    cur.execute("""
                        INSERT INTO orders_current(order_id, status, updated_ts)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT(order_id) DO UPDATE
                        SET status=EXCLUDED.status, updated_ts=EXCLUDED.updated_ts
                    """, (order_id, status_name))

                    if prev_status != status_name:
                        changed += 1
                        cur.execute(
                            "INSERT INTO order_events(order_id, status, event_ts) VALUES (%s, %s, NOW())",
                            (order_id, status_name)
                        )
                        events += 1

                # guardamos watermark al final de cada batch
                set_last_sync_ts(cur, now_ts)

        # si vinieron menos de 100, terminamos
        if len(orders) < 100:
            break

        # si podemos avanzar por fecha, avanzamos 1 segundo para no repetir infinito
        if max_date > from_ts:
            from_ts = max_date + 1
        else:
            # no podemos avanzar: cortamos para no loop infinito
            break

        if loops >= 10:
            # corte de seguridad
            break


@app.post("/sync")
def sync(request: Request):
    # Seguridad
    if SYNC_SECRET:
        incoming = request.headers.get("X-Sync-Secret")
        if incoming != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # Disparar job en background (evita 502)
    t = threading.Thread(target=run_sync_job, daemon=True)
    t.start()

    return {"ok": True, "queued": True}


@app.get("/api/metrics")
def metrics():
    now = datetime.now(timezone.utc)
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    late_cutoff = now - timedelta(hours=24)

    q = """
    WITH latest AS (
      SELECT DISTINCT ON (order_id)
        order_id, status, event_ts
      FROM order_events
      ORDER BY order_id, event_ts DESC
    ),
    current AS (
      SELECT
        order_id,
        status,
        event_ts,
        CASE
          WHEN status = ANY(%(nuevos)s) THEN 'NUEVOS'
          WHEN status = ANY(%(recep)s) THEN 'RECEPCION'
          WHEN status = ANY(%(prep)s) THEN 'PREPARACION'
          WHEN status = ANY(%(pack)s) THEN 'EMBALADO'
          WHEN status = ANY(%(desp)s) THEN 'DESPACHO'
          WHEN status = ANY(%(env)s) THEN 'ENVIADO'
          WHEN status = ANY(%(ent)s) THEN 'ENTREGADO'
          WHEN status = ANY(%(cerr)s) THEN 'CERRADOS'
          ELSE 'OTROS'
        END AS bucket
      FROM latest
    ),
    shipped_today AS (
      SELECT COUNT(*) AS c
      FROM order_events
      WHERE status = ANY(%(env)s)
        AND event_ts >= %(today_start)s
    )
    SELECT
      (SELECT COUNT(*) FROM current WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION')) AS en_preparacion,
      (SELECT COUNT(*) FROM current WHERE bucket='EMBALADO') AS embalados,
      (SELECT COUNT(*) FROM current WHERE bucket='DESPACHO') AS en_despacho,
      (SELECT c FROM shipped_today) AS despachados_hoy,
      (SELECT COUNT(*) FROM current
        WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION','EMBALADO','DESPACHO')
          AND event_ts < %(late_cutoff)s
      ) AS atrasados_24h,
      (SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (NOW() - event_ts)))/60.0, 0)
        FROM current
        WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION','EMBALADO','DESPACHO')
      ) AS avg_age_min
    ;
    """

    params = {
        "nuevos": STATUS_MAP["NUEVOS"],
        "recep": STATUS_MAP["RECEPCION"],
        "prep": STATUS_MAP["PREPARACION"],
        "pack": STATUS_MAP["EMBALADO"],
        "desp": STATUS_MAP["DESPACHO"],
        "env": STATUS_MAP["ENVIADO"],
        "ent": STATUS_MAP["ENTREGADO"],
        "cerr": STATUS_MAP["CERRADOS"],
        "late_cutoff": late_cutoff,
        "today_start": today_start,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            row = cur.fetchone()

    return JSONResponse({
        "en_preparacion": int(row[0] or 0),
        "embalados": int(row[1] or 0),
        "en_despacho": int(row[2] or 0),
        "despachados_hoy": int(row[3] or 0),
        "atrasados_24h": int(row[4] or 0),
        "avg_age_min": float(row[5] or 0.0),
        "refresh_seconds": REFRESH_SECONDS,
        "server_time_utc": now.isoformat()
    })


# Front
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()