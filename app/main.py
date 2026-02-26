import os
import json
import time
from datetime import datetime, timedelta, timezone

import requests
import psycopg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# ===============================
# ENV
# ===============================
DATABASE_URL = os.environ.get("DATABASE_URL")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "10"))

BL_API_URL = os.environ.get("BL_API_URL", "https://api.baselinker.com/connector.php")
BL_TOKEN = os.environ.get("BL_TOKEN", "")
SYNC_SECRET = os.environ.get("SYNC_SECRET", "")

# Sync config
SYNC_WINDOW_DAYS = int(os.environ.get("SYNC_WINDOW_DAYS", "30"))      # "30 días reales"
MAX_SYNC_LOOPS = int(os.environ.get("MAX_SYNC_LOOPS", "120"))         # corte seguridad (batches de 100)

STATUS_MAP = json.loads(
    os.environ.get(
        "STATUS_MAP_JSON",
        json.dumps(
            {
                "NUEVOS": ["Nuevos pedidos"],
                "RECEPCION": ["Agendados", "A distribuir", "error de etiqueta"],
                "PREPARACION": ["Turbo", "Flex", "Colecta", "ME1", "Recolectando"],
                "EMBALADO": [
                    "Puesto 1",
                    "Puesto 2",
                    "Puesto 3",
                    "Puesto 4",
                    "Puesto 5",
                    "Puesto 6",
                    "Embalado",
                ],
                "DESPACHO": ["Mercado Envíos", "Logística propia", "Blitzz", "Zeta", "Eliazar", "Federico"],
                "ENVIADO": ["Enviado", "Enviado"],
                "ENTREGADO": ["Entregado"],
                "CERRADOS": ["Cancelado", "Full", "Pedidos antiguos"],
            }
        ),
    )
)

PACKER_STATUSES = ["Puesto 1", "Puesto 2", "Puesto 3", "Puesto 4", "Puesto 5", "Puesto 6"]


# ===============================
# DB
# ===============================
def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurado")
    # evita cuelgues largos -> 502
    return psycopg.connect(DATABASE_URL, connect_timeout=5)


# ===============================
# BASE API
# ===============================
def bl_call(method: str, params: dict):
    if not BL_TOKEN:
        raise RuntimeError("BL_TOKEN no configurado")

    r = requests.post(
        BL_API_URL,
        headers={"X-BLToken": BL_TOKEN},
        data={"method": method, "parameters": json.dumps(params)},
        timeout=10,
    )
    r.raise_for_status()
    out = r.json()
    if out.get("status") != "SUCCESS":
        raise RuntimeError(str(out))
    return out


# ===============================
# STARTUP: only ensure tables (NO indexes)
# ===============================
@app.on_event("startup")
def ensure_schema():
    # Importante: nada pesado en startup
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_current (
                  order_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_meta (
                  order_id TEXT PRIMARY KEY,
                  date_confirmed TIMESTAMPTZ NULL,
                  date_add TIMESTAMPTZ NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_items (
                  order_product_id BIGINT PRIMARY KEY,
                  order_id TEXT NOT NULL,
                  sku TEXT NULL,
                  name TEXT NULL,
                  quantity INT NOT NULL DEFAULT 0,
                  price_brutto DOUBLE PRECISION NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_kpi_snapshot (
                  id BIGSERIAL PRIMARY KEY,
                  snapshot_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  en_preparacion INT NOT NULL,
                  embalados INT NOT NULL,
                  en_despacho INT NOT NULL,
                  despachados_hoy INT NOT NULL,
                  atrasados_24h INT NOT NULL,
                  avg_age_min DOUBLE PRECISION NOT NULL
                );
            """)


# ===============================
# SYNC (paginado 100/batch) + SNAPSHOT
# ===============================
@app.post("/sync")
def sync(request: Request):
    if SYNC_SECRET:
        incoming = request.headers.get("X-Sync-Secret")
        if incoming != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # status_id -> name
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    status_map = {}
    for s in status_list:
        try:
            sid = int(s.get("id"))
            status_map[sid] = s.get("name", str(sid))
        except Exception:
            continue

    # ventana 30 días (configurable)
    from_ts = int(time.time()) - (SYNC_WINDOW_DAYS * 24 * 60 * 60)

    all_orders = []
    loops = 0

    while True:
        loops += 1
        data = bl_call(
            "getOrders",
            {"date_confirmed_from": from_ts, "get_unconfirmed_orders": True},
        )
        orders = data.get("orders", [])
        if not orders:
            break

        all_orders.extend(orders)

        # avanzar cursor temporal usando el máximo date_confirmed/date_add del batch
        max_ts = from_ts
        for o in orders:
            dc = o.get("date_confirmed") or 0
            da = o.get("date_add") or 0
            try:
                max_ts = max(max_ts, int(dc), int(da))
            except Exception:
                pass

        # si vinieron menos de 100, probablemente no hay más
        if len(orders) < 100:
            break

        # si no avanza, cortamos para no loop infinito
        if max_ts <= from_ts:
            break

        from_ts = max_ts + 1

        if loops >= MAX_SYNC_LOOPS:
            break

    changed = 0
    events_inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for o in all_orders:
                order_id = str(o.get("order_id") or o.get("id") or "")
                if not order_id:
                    continue

                # status
                raw_status_id = o.get("order_status_id")
                status_name = None
                if raw_status_id is not None:
                    try:
                        status_name = status_map.get(int(raw_status_id), str(raw_status_id))
                    except Exception:
                        status_name = str(raw_status_id)

                status_name = status_name or o.get("order_status_name") or o.get("order_status") or o.get("status")
                if not status_name:
                    continue

                # fechas
                dc = o.get("date_confirmed")
                da = o.get("date_add")
                date_confirmed = datetime.fromtimestamp(int(dc), tz=timezone.utc) if dc else None
                date_add = datetime.fromtimestamp(int(da), tz=timezone.utc) if da else None

                # orders_meta
                cur.execute(
                    """
                    INSERT INTO orders_meta(order_id, date_confirmed, date_add, updated_ts)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT(order_id) DO UPDATE
                    SET date_confirmed=EXCLUDED.date_confirmed,
                        date_add=EXCLUDED.date_add,
                        updated_ts=EXCLUDED.updated_ts
                    """,
                    (order_id, date_confirmed, date_add),
                )

                # dedupe status por pedido
                cur.execute("SELECT status FROM orders_current WHERE order_id=%s", (order_id,))
                prev = cur.fetchone()
                prev_status = prev[0] if prev else None

                cur.execute(
                    """
                    INSERT INTO orders_current(order_id, status, updated_ts)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT(order_id) DO UPDATE
                    SET status=EXCLUDED.status, updated_ts=EXCLUDED.updated_ts
                    """,
                    (order_id, status_name),
                )

                if prev_status != status_name:
                    changed += 1
                    cur.execute(
                        "INSERT INTO order_events(order_id, status, event_ts) VALUES (%s, %s, NOW())",
                        (order_id, status_name),
                    )
                    events_inserted += 1

                # items
                for p in (o.get("products") or []):
                    opid = p.get("order_product_id")
                    if opid is None:
                        continue
                    try:
                        opid_int = int(opid)
                    except Exception:
                        continue

                    sku = p.get("sku") or ""
                    name = p.get("name") or ""
                    qty = int(p.get("quantity") or 0)
                    price = float(p.get("price_brutto") or 0.0)

                    cur.execute(
                        """
                        INSERT INTO order_items(order_product_id, order_id, sku, name, quantity, price_brutto, updated_ts)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT(order_product_id) DO UPDATE
                        SET order_id=EXCLUDED.order_id,
                            sku=EXCLUDED.sku,
                            name=EXCLUDED.name,
                            quantity=EXCLUDED.quantity,
                            price_brutto=EXCLUDED.price_brutto,
                            updated_ts=EXCLUDED.updated_ts
                        """,
                        (opid_int, order_id, sku, name, qty, price),
                    )

            # snapshot KPI
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
            )
            SELECT
              (SELECT COUNT(*) FROM current WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION')) AS en_preparacion,
              (SELECT COUNT(*) FROM current WHERE bucket='EMBALADO') AS embalados,
              (SELECT COUNT(*) FROM current WHERE bucket='DESPACHO') AS en_despacho,
              (SELECT COUNT(*) FROM order_events WHERE status = ANY(%(env)s) AND event_ts::timestamptz >= %(today_start)s) AS despachados_hoy,
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

            cur.execute(q, params)
            kpi = cur.fetchone()

            cur.execute(
                """
                INSERT INTO orders_kpi_snapshot(
                  en_preparacion, embalados, en_despacho, despachados_hoy, atrasados_24h, avg_age_min
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                kpi,
            )

    return {
        "ok": True,
        "orders_received": len(all_orders),
        "changed": changed,
        "events_inserted": events_inserted,
        "window_days": SYNC_WINDOW_DAYS,
        "loops": loops,
    }


# ===============================
# METRICS (snapshot)
# ===============================
@app.get("/api/metrics")
def metrics():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  en_preparacion,
                  embalados,
                  en_despacho,
                  despachados_hoy,
                  atrasados_24h,
                  avg_age_min,
                  snapshot_ts
                FROM orders_kpi_snapshot
                ORDER BY snapshot_ts DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()

    if not row:
        now = datetime.now(timezone.utc)
        return JSONResponse(
            {
                "en_preparacion": 0,
                "embalados": 0,
                "en_despacho": 0,
                "despachados_hoy": 0,
                "atrasados_24h": 0,
                "avg_age_min": 0.0,
                "refresh_seconds": REFRESH_SECONDS,
                "server_time_utc": now.isoformat(),
            }
        )

    return JSONResponse(
        {
            "en_preparacion": int(row[0] or 0),
            "embalados": int(row[1] or 0),
            "en_despacho": int(row[2] or 0),
            "despachados_hoy": int(row[3] or 0),
            "atrasados_24h": int(row[4] or 0),
            "avg_age_min": float(row[5] or 0.0),
            "refresh_seconds": REFRESH_SECONDS,
            "server_time_utc": row[6].isoformat(),
        }
    )


# ===============================
# TOP PRODUCTS (default 30 días)
# ===============================
@app.get("/api/top-products")
def top_products(days: int = 30, limit: int = 10):
    since = datetime.now(timezone.utc) - timedelta(days=days)
    closed = STATUS_MAP["CERRADOS"]

    q = """
    SELECT
      COALESCE(NULLIF(oi.sku,''), '(sin sku)') AS sku,
      COALESCE(NULLIF(oi.name,''), '(sin nombre)') AS name,
      SUM(oi.quantity)::int AS units,
      COUNT(DISTINCT oi.order_id)::int AS orders
    FROM order_items oi
    JOIN orders_meta om ON om.order_id = oi.order_id
    JOIN orders_current oc ON oc.order_id = oi.order_id
    WHERE COALESCE(om.date_confirmed, om.date_add, NOW()) >= %s
      AND oc.status <> ALL(%s)
    GROUP BY 1,2
    ORDER BY units DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (since, closed, limit))
            rows = cur.fetchall()

    return [{"sku": r[0], "name": r[1], "units": r[2], "orders": r[3]} for r in rows]


# ===============================
# TOP PACKERS (ranking)
# ===============================
@app.get("/api/top-packers")
def top_packers(days: int = 1, limit: int = 6):
    since = datetime.now(timezone.utc) - timedelta(days=days)

    q = """
    SELECT
      status,
      COUNT(DISTINCT order_id)::int AS orders_packed
    FROM order_events
    WHERE status = ANY(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY status
    ORDER BY orders_packed DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (PACKER_STATUSES, since, limit))
            rows = cur.fetchall()

    return [{"packer": r[0], "orders_packed": r[1]} for r in rows]


# ===============================
# PACKERS HOURLY (productividad por hora)
# ===============================
@app.get("/api/packers-hourly")
def packers_hourly(hours: int = 24):
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    q = """
    SELECT
      date_trunc('hour', event_ts)::timestamptz AS hour,
      status AS packer,
      COUNT(DISTINCT order_id)::int AS orders_packed
    FROM order_events
    WHERE status = ANY(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY 1,2
    ORDER BY 1 ASC, 2 ASC;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (PACKER_STATUSES, since))
            rows = cur.fetchall()

    return [{"hour": r[0].isoformat(), "packer": r[1], "orders_packed": r[2]} for r in rows]


# ===============================
# FRONT
# ===============================
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()