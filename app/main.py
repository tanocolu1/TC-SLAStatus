import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import psycopg

app = FastAPI()

# ===============================
# ENV VARIABLES
# ===============================

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


# ===============================
# DB CONNECTION
# ===============================

def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurado")
    return psycopg.connect(DATABASE_URL)


# ===============================
# METRICS ENDPOINT
# ===============================

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
        AND event_ts::timestamptz >= %(today_start)s
    )
    SELECT
      (SELECT COUNT(*) FROM current WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION')) AS en_preparacion,
      (SELECT COUNT(*) FROM current WHERE bucket='EMBALADO') AS embalados,
      (SELECT COUNT(*) FROM current WHERE bucket='DESPACHO') AS en_despacho,
      (SELECT c FROM shipped_today) AS despachados_hoy,
      (SELECT COUNT(*) FROM current
        WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION','EMBALADO','DESPACHO')
          AND event_ts::timestamptz < %(late_cutoff)s
      ) AS atrasados_24h,
      (SELECT COALESCE(AVG(EXTRACT(EPOCH FROM (NOW() - event_ts::timestamptz)))/60.0, 0)
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


# ===============================
# SYNC ENDPOINT (API POLLING)
# ===============================

@app.post("/sync")
def sync(request: Request):
    # Seguridad
    if SYNC_SECRET:
        incoming = request.headers.get("X-Sync-Secret")
        if incoming != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    if not BL_TOKEN:
        raise HTTPException(status_code=500, detail="BL_TOKEN not configured")

    headers = {"X-BLToken": BL_TOKEN}

    def bl_call(method: str, params: dict):
        r = requests.post(
            BL_API_URL,
            headers=headers,
            data={"method": method, "parameters": json.dumps(params)},
            timeout=30,
        )
        r.raise_for_status()
        out = r.json()
        if out.get("status") != "SUCCESS":
            raise HTTPException(status_code=500, detail=out)
        return out

    # 1) Descargar lista de estados y armar mapa id->nombre
    # getOrderStatusList devuelve statuses: [{id, name, ...}] :contentReference[oaicite:1]{index=1}
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    status_map = {int(s["id"]): s.get("name", str(s["id"])) for s in status_list if "id" in s}

    # 2) Traer órdenes (por ahora: desde 0; luego optimizamos con watermark)
    seven_days_ago = int(time.time()) - (7 * 24 * 60 * 60)

data = bl_call("getOrders", {
    "date_confirmed_from": seven_days_ago,
    "get_unconfirmed_orders": True
}) # :contentReference[oaicite:2]{index=2}
    orders = data.get("orders", [])

    changed = 0
    events_inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for o in orders:
                order_id = str(o.get("order_id") or o.get("id") or "")
                if not order_id:
                    continue

                # BaseLinker suele traer order_status_id (int)
                raw_status_id = o.get("order_status_id")
                status_name = None

                if raw_status_id is not None:
                    try:
                        status_name = status_map.get(int(raw_status_id), str(raw_status_id))
                    except Exception:
                        status_name = str(raw_status_id)

                # fallback si tu cuenta manda nombre directo
                status_name = status_name or o.get("order_status_name") or o.get("order_status") or o.get("status")
                if not status_name:
                    continue

                # Estado anterior (para dedupe)
                cur.execute("SELECT status FROM orders_current WHERE order_id=%s", (order_id,))
                prev = cur.fetchone()
                prev_status = prev[0] if prev else None

                # Upsert estado actual
                cur.execute(
                    """
                    INSERT INTO orders_current(order_id, status, updated_ts)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT(order_id) DO UPDATE
                    SET status=EXCLUDED.status, updated_ts=EXCLUDED.updated_ts
                    """,
                    (order_id, status_name),
                )

                # Insert evento solo si cambió
                if prev_status != status_name:
                    changed += 1
                    cur.execute(
                        "INSERT INTO order_events(order_id, status, event_ts) VALUES (%s, %s, NOW())",
                        (order_id, status_name),
                    )
                    events_inserted += 1

    return {"ok": True, "orders_received": len(orders), "changed": changed, "events_inserted": events_inserted}
@app.get("/internal-sync")
def internal_sync():
    return sync(Request({"type": "http"}))