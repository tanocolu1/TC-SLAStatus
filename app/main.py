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
                "ENVIADO": ["Enviado", "Envidado"],
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
    return psycopg.connect(DATABASE_URL)


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
# STARTUP: ensure schema
# ===============================
@app.on_event("startup")
def ensure_schema():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # (1) Estado actual por pedido (dedupe)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orders_current (
                  order_id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            # (2) Meta por pedido (fechas para top products)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS orders_meta (
                  order_id TEXT PRIMARY KEY,
                  date_confirmed TIMESTAMPTZ NULL,
                  date_add TIMESTAMPTZ NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            # (3) Items (líneas) del pedido
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS order_items (
                  order_product_id BIGINT PRIMARY KEY,
                  order_id TEXT NOT NULL,
                  sku TEXT NULL,
                  name TEXT NULL,
                  quantity INT NOT NULL DEFAULT 0,
                  price_brutto DOUBLE PRECISION NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            # (4) Snapshot KPI (ya la creaste por UI; dejamos por seguridad)
            cur.execute(
                """
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
                """
            )

            # Índices (si ya existen, no pasa nada)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_meta_date_confirmed ON orders_meta(date_confirmed);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_sku ON order_items(sku);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_snapshot_ts ON orders_kpi_snapshot(snapshot_ts);")

            # Índices para order_events (asumiendo que ya existe la tabla)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_event_ts ON order_events(event_ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_status ON order_events(status);")


# ===============================
# SYNC (API polling) + SNAPSHOT
# ===============================
@app.post("/sync")
def sync(request: Request):
    # Seguridad
    if SYNC_SECRET:
        incoming = request.headers.get("X-Sync-Secret")
        if incoming != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # 1) status_id -> name
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    status_map = {}
    for s in status_list:
        try:
            sid = int(s.get("id"))
            status_map[sid] = s.get("name", str(sid))
        except Exception:
            continue

    # 2) ventana operativa (7 días) para evitar 502
    from_ts = int(time.time()) - (7 * 24 * 60 * 60)

    data = bl_call(
        "getOrders",
        {
            "date_confirmed_from": from_ts,
            "get_unconfirmed_orders": True,
        },
    )
    orders = data.get("orders", [])

    changed = 0
    events_inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for o in orders:
                order_id = str(o.get("order_id") or o.get("id") or "")
                if not order_id:
                    continue

                # ---- status ----
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

                # ---- fechas ----
                dc = o.get("date_confirmed")
                da = o.get("date_add")
                date_confirmed = datetime.fromtimestamp(int(dc), tz=timezone.utc) if dc else None
                date_add = datetime.fromtimestamp(int(da), tz=timezone.utc) if da else None

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

                # ---- dedupe status por pedido ----
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

                # ---- items ----
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

            # 3) Snapshot KPI
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

    return {"ok": True, "orders_received": len(orders), "changed": changed, "events_inserted": events_inserted}


# ===============================
# METRICS (reads snapshot)
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
# TOP PRODUCTS (Top 10)
# ===============================
@app.get("/api/top-products")
def top_products(days: int = 7, limit: int = 10):
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
# TOP PACKERS (Top 6)
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
# FRONT
# ===============================
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()