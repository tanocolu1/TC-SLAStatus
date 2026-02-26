import os
import json
import time
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import psycopg
from psycopg_pool import ConnectionPool
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ===============================
# LOGGING estructurado
# ===============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger("bl_sync")

# ===============================
# ENV
# ===============================
DATABASE_URL     = os.environ.get("DATABASE_URL")
REFRESH_SECONDS  = int(os.environ.get("REFRESH_SECONDS", "10"))
BL_API_URL       = os.environ.get("BL_API_URL", "https://api.baselinker.com/connector.php")
BL_TOKEN         = os.environ.get("BL_TOKEN", "")
SYNC_SECRET      = os.environ.get("SYNC_SECRET", "")

STATUS_MAP = json.loads(os.environ.get("STATUS_MAP_JSON", json.dumps({
    "NUEVOS":      ["Nuevos pedidos"],
    "RECEPCION":   ["Agendados", "A distribuir", "error de etiqueta"],
    "PREPARACION": ["Turbo", "Flex", "Colecta", "ME1", "Recolectando"],
    "EMBALADO":    ["Puesto 1", "Puesto 2", "Puesto 3", "Puesto 4", "Puesto 5", "Puesto 6", "Embalado"],
    "DESPACHO":    ["Mercado Envíos", "Logística propia", "Blitzz", "Zeta", "Eliazar", "Federico"],
    "ENVIADO":     ["Enviado", "Envidado"],
    "ENTREGADO":   ["Entregado"],
    "CERRADOS":    ["Cancelado", "Full", "Pedidos antiguos"],
})))

# ===============================
# CONNECTION POOL  (P1)
# Evita abrir una conexión nueva por cada request.
# min_size=2 mantiene conexiones listas; max_size=10 limita el uso en Postgres.
# ===============================
pool: ConnectionPool | None = None

def get_pool() -> ConnectionPool:
    if pool is None:
        raise RuntimeError("Pool no inicializado")
    return pool

# ===============================
# LIFESPAN  (reemplaza @on_event deprecado)
# ===============================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurado")

    pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10, open=True)
    log.info("Connection pool inicializado")

    _ensure_schema()
    log.info("Schema/índices verificados")

    yield

    pool.close()
    log.info("Connection pool cerrado")

app = FastAPI(lifespan=lifespan)

# ===============================
# SCHEMA
# ===============================
def _ensure_schema():
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_current (
                    order_id   TEXT PRIMARY KEY,
                    status     TEXT NOT NULL,
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_events (
                    id         BIGSERIAL PRIMARY KEY,
                    order_id   TEXT NOT NULL,
                    status     TEXT NOT NULL,
                    event_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            # Índices
            cur.execute("CREATE INDEX IF NOT EXISTS idx_oe_order_id ON order_events(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_oe_event_ts ON order_events(event_ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_oe_status   ON order_events(status);")

# ===============================
# BASELINKER  (P1: retry automático)
# 3 intentos con backoff exponencial: 2s → 4s → 8s
# ===============================
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=2, max=10),
    retry=retry_if_exception_type((requests.RequestException, RuntimeError)),
    reraise=True,
)
def bl_call(method: str, params: dict) -> dict:
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
        raise RuntimeError(f"BL error [{method}]: {out}")
    return out

# ===============================
# SYNC LOGIC  (P1: bulk upsert)
# ===============================
def _run_sync() -> dict:
    # 1) Mapa status_id -> nombre
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    status_map: dict[int, str] = {}
    for s in status_list:
        try:
            status_map[int(s["id"])] = s.get("name", str(s["id"]))
        except Exception:
            continue

    # 2) Ventana dinámica basada en último sync exitoso  (P2)
    from_ts: int
    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM sync_state WHERE key='last_sync_ts'")
            row = cur.fetchone()
            if row:
                # Retrocedemos 5 minutos para cubrir race conditions
                from_ts = int(row[0]) - 300
                log.info(f"Sync incremental desde {datetime.fromtimestamp(from_ts, tz=timezone.utc)}")
            else:
                # Primera vez: últimos 7 días
                from_ts = int(time.time()) - (7 * 24 * 60 * 60)
                log.info("Primer sync: ventana de 7 días")

    sync_started_ts = int(time.time())

    data = bl_call("getOrders", {
        "date_confirmed_from": from_ts,
        "get_unconfirmed_orders": True,
    })
    orders = data.get("orders", [])
    log.info(f"BaseLinker devolvió {len(orders)} pedidos")

    # 3) Construir lista de (order_id, status_name) válidos
    rows: list[tuple[str, str]] = []
    for o in orders:
        order_id = str(o.get("order_id") or o.get("id") or "").strip()
        if not order_id:
            continue
        raw_sid = o.get("order_status_id")
        status_name = None
        if raw_sid is not None:
            try:
                status_name = status_map.get(int(raw_sid), str(raw_sid))
            except Exception:
                status_name = str(raw_sid)
        status_name = (
            status_name
            or o.get("order_status_name")
            or o.get("order_status")
            or o.get("status")
        )
        if status_name:
            rows.append((order_id, status_name))

    if not rows:
        return {"ok": True, "orders_received": len(orders), "changed": 0, "events_inserted": 0}

    # 4) Bulk upsert  (P1: una sola operación en vez de N queries)
    #
    # Estrategia:
    #   a) Upsert masivo en orders_current con todos los valores de una vez.
    #   b) INSERT en order_events SOLO para los pedidos cuyo status cambió
    #      (comparado con el valor anterior en orders_current).
    #
    # Usamos un CTE para hacer todo en una única query:
    #
    #   WITH incoming(order_id, status) AS (VALUES ...)
    #   , upserted AS (
    #       INSERT INTO orders_current ...
    #       ON CONFLICT DO UPDATE ...
    #       RETURNING order_id, status,
    #                 (xmax = 0) AS is_insert,          -- nuevo pedido
    #                 status IS DISTINCT FROM excluded.status  -- cambió
    #   )
    #   INSERT INTO order_events(order_id, status)
    #   SELECT order_id, status FROM upserted
    #   WHERE is_insert OR changed
    #
    # Nota: en psycopg3 usamos %s y pasamos lista de tuplas.

    changed = 0
    events_inserted = 0

    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            # Construir VALUES placeholder
            values_placeholder = ", ".join(["(%s, %s)"] * len(rows))
            flat_values = [v for pair in rows for v in pair]

            cur.execute(f"""
                WITH incoming(order_id, new_status) AS (
                    VALUES {values_placeholder}
                ),
                prev AS (
                    SELECT oc.order_id, oc.status AS old_status
                    FROM orders_current oc
                    JOIN incoming i ON i.order_id = oc.order_id
                ),
                upserted AS (
                    INSERT INTO orders_current(order_id, status, updated_ts)
                    SELECT order_id, new_status, NOW() FROM incoming
                    ON CONFLICT(order_id) DO UPDATE
                        SET status = EXCLUDED.status,
                            updated_ts = EXCLUDED.updated_ts
                    RETURNING order_id, status
                )
                INSERT INTO order_events(order_id, status, event_ts)
                SELECT u.order_id, u.status, NOW()
                FROM upserted u
                LEFT JOIN prev p ON p.order_id = u.order_id
                WHERE p.old_status IS NULL                    -- pedido nuevo
                   OR p.old_status IS DISTINCT FROM u.status -- status cambió
                RETURNING order_id
            """, flat_values)

            events_inserted = cur.rowcount
            changed = events_inserted  # cada evento = un cambio

            # 5) Guardar timestamp de este sync exitoso
            cur.execute("""
                INSERT INTO sync_state(key, value) VALUES ('last_sync_ts', %s)
                ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
            """, (str(sync_started_ts),))

    log.info(f"Sync ok — recibidos={len(orders)} eventos={events_inserted}")
    return {
        "ok": True,
        "orders_received": len(orders),
        "changed": changed,
        "events_inserted": events_inserted,
    }

# ===============================
# ENDPOINT /sync  (P2: no bloqueante)
# Devuelve 202 de inmediato y corre el sync en background.
# El cron/scheduler recibe respuesta rápida aunque BL tarde.
# ===============================
_sync_running = False  # flag simple para evitar solapamiento

@app.post("/sync", status_code=202)
def sync(request: Request, background_tasks: BackgroundTasks):
    if SYNC_SECRET:
        if request.headers.get("X-Sync-Secret") != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    global _sync_running
    if _sync_running:
        return JSONResponse({"ok": False, "detail": "sync already running"}, status_code=409)

    def _task():
        global _sync_running
        _sync_running = True
        try:
            result = _run_sync()
            log.info(f"Background sync completado: {result}")
        except Exception as e:
            log.exception(f"Background sync falló: {e}")
        finally:
            _sync_running = False

    background_tasks.add_task(_task)
    return {"ok": True, "detail": "sync started in background"}

# ===============================
# HEALTH  (P3: observabilidad básica)
# ===============================
@app.get("/health")
def health():
    try:
        with get_pool().connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        db_ok = True
    except Exception as e:
        log.error(f"Health check DB falló: {e}")
        db_ok = False

    status = "ok" if db_ok else "degraded"
    return JSONResponse(
        {"status": status, "db": db_ok, "sync_running": _sync_running},
        status_code=200 if db_ok else 503,
    )

# ===============================
# METRICS  (P3: lee orders_current directamente — más rápido)
# ===============================
@app.get("/api/metrics")
def metrics():
    now         = datetime.now(timezone.utc)
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    late_cutoff = now - timedelta(hours=24)

    # Construir lookup status -> bucket a partir de STATUS_MAP
    status_to_bucket: dict[str, str] = {}
    for bucket, statuses in STATUS_MAP.items():
        for s in statuses:
            status_to_bucket[s] = bucket

    # Todos los statuses activos por bucket (para el ANY de Postgres)
    def statuses_for(*buckets: str) -> list[str]:
        result = []
        for b in buckets:
            result.extend(STATUS_MAP.get(b, []))
        return result

    active_statuses = statuses_for("NUEVOS", "RECEPCION", "PREPARACION", "EMBALADO", "DESPACHO")

    q = """
    SELECT
        COUNT(*) FILTER (WHERE status = ANY(%(prep_all)s))         AS en_preparacion,
        COUNT(*) FILTER (WHERE status = ANY(%(pack)s))             AS embalados,
        COUNT(*) FILTER (WHERE status = ANY(%(desp)s))             AS en_despacho,
        0                                                           AS despachados_hoy,
        COUNT(*) FILTER (
            WHERE status = ANY(%(active)s) AND updated_ts < %(late_cutoff)s
        )                                                           AS atrasados_24h,
        COALESCE(AVG(
            EXTRACT(EPOCH FROM (NOW() - updated_ts)) / 60.0
        ) FILTER (WHERE status = ANY(%(active)s)), 0)              AS avg_age_min
    FROM orders_current;
    """

    # despachados_hoy requiere leer order_events (necesitamos la fecha del evento ENVIADO de hoy)
    q_shipped = """
    SELECT COUNT(DISTINCT order_id)
    FROM order_events
    WHERE status = ANY(%(env)s)
      AND event_ts >= %(today_start)s;
    """

    with get_pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(q, {
                "prep_all":   statuses_for("NUEVOS", "RECEPCION", "PREPARACION"),
                "pack":       STATUS_MAP["EMBALADO"],
                "desp":       STATUS_MAP["DESPACHO"],
                "active":     active_statuses,
                "late_cutoff": late_cutoff,
            })
            row = cur.fetchone()

            cur.execute(q_shipped, {
                "env":        STATUS_MAP["ENVIADO"],
                "today_start": today_start,
            })
            shipped_row = cur.fetchone()

    return JSONResponse({
        "en_preparacion":   int(row[0] or 0),
        "embalados":        int(row[1] or 0),
        "en_despacho":      int(row[2] or 0),
        "despachados_hoy":  int(shipped_row[0] or 0),
        "atrasados_24h":    int(row[4] or 0),
        "avg_age_min":      float(row[5] or 0.0),
        "refresh_seconds":  REFRESH_SECONDS,
        "server_time_utc":  now.isoformat(),
    })

# ===============================
# FRONT
# ===============================
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()