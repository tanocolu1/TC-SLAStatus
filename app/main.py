import asyncio
import logging
import os
import json
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo
from functools import lru_cache

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("uvicorn.error")

# ===============================
# ENV
# ===============================
DATABASE_URL = os.environ.get("DATABASE_URL")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "10"))

BL_API_URL = os.environ.get("BL_API_URL", "https://api.baselinker.com/connector.php")
BL_TOKEN = os.environ.get("BL_TOKEN", "")
SYNC_SECRET = os.environ.get("SYNC_SECRET", "")

SYNC_WINDOW_DAYS = int(os.environ.get("SYNC_WINDOW_DAYS", "30"))
TZ_NAME = os.environ.get("TZ_NAME", "America/Argentina/Buenos_Aires")
LOCAL_TZ = ZoneInfo(TZ_NAME)
MAX_SYNC_LOOPS  = int(os.environ.get("MAX_SYNC_LOOPS", "120"))
AUTO_SYNC_EVERY = int(os.environ.get("AUTO_SYNC_EVERY", "300"))  # segundos entre syncs (default 5 min)

if not SYNC_SECRET:
    logger.warning("SYNC_SECRET no está configurado — el endpoint /sync es público.")

STATUS_MAP: dict[str, list[str]] = json.loads(
    os.environ.get(
        "STATUS_MAP_JSON",
        json.dumps(
            {
                "NUEVOS": ["Nuevos pedidos"],
                "RECEPCION": ["Agendados", "A distribuir", "error de etiqueta"],
                "PREPARACION": ["Turbo", "Flex", "Colecta", "Recolectando"],
                "EXCLUIDOS": ["ME1"],
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
                # FIX: eliminado duplicado "Enviado" que aparecía dos veces en la lista original
                "ENVIADO": ["Enviado"],
                "ENTREGADO": ["Entregado"],
                "CERRADOS": ["Cancelado", "Full", "Pedidos antiguos"],
            }
        ),
    )
)

# FIX: derivado de STATUS_MAP para no desincronizarse si se agrega un puesto nuevo
PACKER_STATUSES: list[str] = [s for s in STATUS_MAP["EMBALADO"] if s.startswith("Puesto")]

# Buckets considerados "activos" (pedidos en vuelo)
ACTIVE_BUCKETS = ["NUEVOS", "RECEPCION", "PREPARACION", "EMBALADO", "DESPACHO"]

# Estados completamente excluidos de KPIs, tops y embaladores
EXCLUDED_STATUSES: list[str] = STATUS_MAP.get("EXCLUIDOS", [])


# ===============================
# HTTP SESSION con retry automático
# ===============================
def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,           # 1 s, 2 s, 4 s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

_http = _make_session()


# ===============================
# DB
# ===============================
def get_conn() -> psycopg.Connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurado")
    return psycopg.connect(DATABASE_URL, connect_timeout=5)


# ===============================
# HTML estático con inyección de secret
# ===============================
@lru_cache(maxsize=1)
def _load_index_html_raw() -> str:
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()

def _render_index_html() -> str:
    """Reemplaza el placeholder del secret antes de servir el HTML."""
    return _load_index_html_raw().replace("__SYNC_SECRET__", SYNC_SECRET, 1)


# ===============================
# STARTUP via lifespan (on_event está deprecado en FastAPI moderno)
# ===============================
@asynccontextmanager
async def lifespan(_app: FastAPI):
    try:
        _ensure_schema()
    except Exception as exc:
        logger.error("Error al inicializar el schema: %s", exc)
        raise

    task = asyncio.create_task(_auto_sync_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _auto_sync_loop() -> None:
    """Llama a _run_sync() cada AUTO_SYNC_EVERY segundos en background."""
    logger.info("Auto-sync arrancado: cada %d segundos.", AUTO_SYNC_EVERY)
    try:
        result = await asyncio.to_thread(_run_sync)
        logger.info("Auto-sync inicial OK: %s", result)
    except Exception as exc:
        logger.error("Auto-sync inicial falló: %s", exc)

    last_cleanup_date = None

    while True:
        await asyncio.sleep(AUTO_SYNC_EVERY)

        # Limpieza a medianoche ARG
        now_local = datetime.now(LOCAL_TZ)
        today_date = now_local.date()
        if last_cleanup_date != today_date and now_local.hour == 0:
            try:
                deleted = await asyncio.to_thread(_cleanup_bulk_events)
                last_cleanup_date = today_date
                logger.info("Limpieza bulk events: %d borrados", deleted)
            except Exception as exc:
                logger.error("Error en limpieza bulk events: %s", exc)

        try:
            result = await asyncio.to_thread(_run_sync)
            logger.info("Auto-sync OK: %s", result)
        except Exception as exc:
            logger.error("Auto-sync falló: %s", exc)


def _cleanup_bulk_events() -> int:
    """Borra eventos del sync inicial: rows donde 10+ pedidos tienen el mismo status
    en el mismo segundo (bulk insert, no cambio real)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM order_events
                WHERE id IN (
                    SELECT oe.id FROM order_events oe
                    WHERE (
                        SELECT COUNT(*) FROM order_events oe2
                        WHERE oe2.status = oe.status
                          AND date_trunc('second', oe2.event_ts) = date_trunc('second', oe.event_ts)
                    ) >= 10
                )
            """)
            deleted = cur.rowcount
        conn.commit()
    return deleted



def _ensure_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            # FIX: order_events se define acá. En la versión anterior solo se creaban
            # índices sobre ella sin haber creado la tabla, lo que reventaba el startup.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_events (
                  id       BIGSERIAL PRIMARY KEY,
                  order_id TEXT NOT NULL,
                  status   TEXT NOT NULL,
                  event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_current (
                  order_id   TEXT PRIMARY KEY,
                  status     TEXT NOT NULL,
                  updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_meta (
                  order_id       TEXT PRIMARY KEY,
                  date_confirmed TIMESTAMPTZ NULL,
                  date_add       TIMESTAMPTZ NULL,
                  updated_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_items (
                  order_product_id BIGINT PRIMARY KEY,
                  order_id         TEXT NOT NULL,
                  sku              TEXT NULL,
                  name             TEXT NULL,
                  quantity         INT NOT NULL DEFAULT 0,
                  price_brutto     DOUBLE PRECISION NULL,
                  updated_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders_kpi_snapshot (
                  id              BIGSERIAL PRIMARY KEY,
                  snapshot_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  en_preparacion  INT NOT NULL,
                  embalados       INT NOT NULL,
                  en_despacho     INT NOT NULL,
                  despachados_hoy INT NOT NULL,
                  atrasados_24h   INT NOT NULL,
                  avg_age_min     DOUBLE PRECISION NOT NULL
                );
            """)
            # Índices
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);")
            # Migrar event_ts de TEXT a TIMESTAMPTZ si la columna todavía es TEXT
            cur.execute("""
                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'order_events'
                      AND column_name = 'event_ts'
                      AND data_type = 'text'
                  ) THEN
                    ALTER TABLE order_events
                      ALTER COLUMN event_ts TYPE TIMESTAMPTZ
                      USING event_ts::timestamptz;
                  END IF;
                END $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_event_ts  ON order_events(event_ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_status    ON order_events(status);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_meta_date_conf  ON orders_meta(date_confirmed);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_sku        ON order_items(sku);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_snapshot_ts        ON orders_kpi_snapshot(snapshot_ts);")
            # Migrar: agregar status_since a orders_current si no existe
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'orders_current' AND column_name = 'status_since'
                  ) THEN
                    ALTER TABLE orders_current ADD COLUMN status_since TIMESTAMPTZ;
                    UPDATE orders_current SET status_since = updated_ts WHERE status_since IS NULL;
                  END IF;
                END $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_current_status_since ON orders_current(status_since);")



app = FastAPI(lifespan=lifespan)


# ===============================
# BASE API
# ===============================
def bl_call(method: str, params: dict) -> dict:
    if not BL_TOKEN:
        raise RuntimeError("BL_TOKEN no configurado")
    r = _http.post(
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
# SYNC (paginado 100/batch) + SNAPSHOT
# ===============================
def _run_sync() -> dict:
    """Lógica de sync pura, sin HTTP context. Usada por el background loop y el endpoint manual."""
    # 1) status_id -> name
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    id_to_name: dict[int, str] = {}
    for s in status_list:
        try:
            id_to_name[int(s["id"])] = s.get("name", str(s["id"]))
        except Exception:
            continue

    # 2) Paginación temporal
    from_ts = int(time.time()) - (SYNC_WINDOW_DAYS * 24 * 3600)
    all_orders: list[dict] = []
    loops = 0

    while loops < MAX_SYNC_LOOPS:
        loops += 1
        data = bl_call(
            "getOrders",
            {"date_confirmed_from": from_ts, "get_unconfirmed_orders": True},
        )
        batch = data.get("orders", [])
        if not batch:
            break

        all_orders.extend(batch)

        if len(batch) < 100:
            break   # última página

        # avanzar cursor al máximo timestamp del batch
        max_ts = from_ts
        for o in batch:
            for field in ("date_confirmed", "date_add"):
                try:
                    max_ts = max(max_ts, int(o.get(field) or 0))
                except Exception:
                    pass

        if max_ts <= from_ts:
            break   # no avanzó → cortar para no loop infinito

        from_ts = max_ts + 1

    # 3) Persistencia: preparar datos en memoria, luego batch upsert + commit cada 200 pedidos
    changed = 0
    events_inserted = 0
    errors = 0

    BATCH_SIZE = 200

    # Pre-cargar todos los estados actuales en memoria para evitar un SELECT por pedido
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT order_id, status FROM orders_current")
            current_statuses: dict[str, str] = {r[0]: r[1] for r in cur.fetchall()}

    def _process_batch(batch_orders: list) -> tuple[int, int, int]:
        """Upsert un batch de pedidos en una sola transacción. Devuelve (changed, events, errors)."""
        b_changed = b_events = b_errors = 0

        meta_rows   = []
        current_rows = []
        event_rows  = []
        item_rows   = []

        for o in batch_orders:
            order_id = str(o.get("order_id") or o.get("id") or "")
            if not order_id:
                continue

            try:
                # --- status ---
                raw_sid = o.get("order_status_id")
                status_name: str | None = None
                if raw_sid is not None:
                    try:
                        status_name = id_to_name.get(int(raw_sid), str(raw_sid))
                    except Exception:
                        status_name = str(raw_sid)
                status_name = (
                    status_name
                    or o.get("order_status_name")
                    or o.get("order_status")
                    or o.get("status")
                )
                if not status_name:
                    continue

                # --- fechas ---
                dc = o.get("date_confirmed")
                da = o.get("date_add")
                date_confirmed = datetime.fromtimestamp(int(dc), tz=timezone.utc) if dc else None
                date_add       = datetime.fromtimestamp(int(da), tz=timezone.utc) if da else None

                meta_rows.append((order_id, date_confirmed, date_add))
                current_rows.append((order_id, status_name))

                prev_status = current_statuses.get(order_id)
                if prev_status != status_name:
                    b_changed += 1
                    event_rows.append((order_id, status_name))
                    current_statuses[order_id] = status_name  # actualizar cache local

                # --- items ---
                for p in (o.get("products") or []):
                    opid = p.get("order_product_id")
                    if opid is None:
                        continue
                    try:
                        opid_int = int(opid)
                    except Exception:
                        continue
                    item_rows.append((
                        opid_int, order_id,
                        p.get("sku") or "",
                        p.get("name") or "",
                        int(p.get("quantity") or 0),
                        float(p.get("price_brutto") or 0.0),
                    ))

            except Exception as exc:
                logger.error("Error preparando order_id=%s: %s", order_id, exc)
                b_errors += 1

        if not meta_rows:
            return b_changed, b_events, b_errors

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO orders_meta(order_id, date_confirmed, date_add, updated_ts)
                        VALUES (%s, %s, %s, NOW())
                        ON CONFLICT(order_id) DO UPDATE
                        SET date_confirmed = EXCLUDED.date_confirmed,
                            date_add       = EXCLUDED.date_add,
                            updated_ts     = EXCLUDED.updated_ts
                        """,
                        meta_rows,
                    )
                    cur.executemany(
                        """
                        INSERT INTO orders_current(order_id, status, updated_ts, status_since)
                        VALUES (%s, %s, NOW(), NOW())
                        ON CONFLICT(order_id) DO UPDATE
                        SET updated_ts   = EXCLUDED.updated_ts,
                            status_since = CASE
                              WHEN orders_current.status <> EXCLUDED.status THEN NOW()
                              ELSE COALESCE(orders_current.status_since, EXCLUDED.updated_ts)
                            END,
                            status     = EXCLUDED.status
                        """,
                        current_rows,
                    )
                    if event_rows:
                        cur.executemany(
                            "INSERT INTO order_events(order_id, status, event_ts) VALUES (%s, %s, NOW())",
                            event_rows,
                        )
                        b_events += len(event_rows)
                    if item_rows:
                        cur.executemany(
                            """
                            INSERT INTO order_items(
                              order_product_id, order_id, sku, name,
                              quantity, price_brutto, updated_ts
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT(order_product_id) DO UPDATE
                            SET order_id     = EXCLUDED.order_id,
                                sku          = EXCLUDED.sku,
                                name         = EXCLUDED.name,
                                quantity     = EXCLUDED.quantity,
                                price_brutto = EXCLUDED.price_brutto,
                                updated_ts   = EXCLUDED.updated_ts
                            """,
                            item_rows,
                        )
                conn.commit()
        except Exception as exc:
            logger.error("Error en batch upsert: %s", exc)
            b_errors += len(meta_rows)

        return b_changed, b_events, b_errors

    # Procesar en batches de BATCH_SIZE
    for i in range(0, len(all_orders), BATCH_SIZE):
        bc, be, berr = _process_batch(all_orders[i : i + BATCH_SIZE])
        changed         += bc
        events_inserted += be
        errors          += berr

    # 4) Snapshot KPI — una sola vez, después de que todos los batches se grabaron
    now         = datetime.now(timezone.utc)
    now_local   = datetime.now(LOCAL_TZ)
    today_start = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ)
    late_cutoff = now - timedelta(hours=24)

    kpi_sql = """
    WITH current_active AS (
      -- Usar orders_current directamente: refleja el estado real de cada pedido hoy
      SELECT
        oc.order_id,
        oc.status,
        oc.updated_ts AS event_ts
      FROM orders_current oc
      WHERE oc.status <> ALL(%(excluidos)s)
    ),
    bucketed AS (
      SELECT
        order_id,
        event_ts,
        CASE
          WHEN status = ANY(%(nuevos)s) THEN 'NUEVOS'
          WHEN status = ANY(%(recep)s)  THEN 'RECEPCION'
          WHEN status = ANY(%(prep)s)   THEN 'PREPARACION'
          WHEN status = ANY(%(pack)s)   THEN 'EMBALADO'
          WHEN status = ANY(%(desp)s)   THEN 'DESPACHO'
          WHEN status = ANY(%(env)s)    THEN 'ENVIADO'
          WHEN status = ANY(%(ent)s)    THEN 'ENTREGADO'
          WHEN status = ANY(%(cerr)s)   THEN 'CERRADOS'
          ELSE 'OTROS'
        END AS bucket
      FROM current_active
    )
    SELECT
      (SELECT COUNT(*) FROM bucketed
       WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION'))           AS en_preparacion,
      (SELECT COUNT(*) FROM bucketed WHERE bucket = 'EMBALADO')        AS embalados,
      (SELECT COUNT(*) FROM bucketed WHERE bucket = 'DESPACHO')        AS en_despacho,
      (SELECT COUNT(DISTINCT order_id) FROM order_events
       WHERE status = ANY(%(env)s)
         AND status <> ALL(%(excluidos)s)
         AND event_ts::timestamptz >= %(today_start)s)                 AS despachados_hoy,
      (SELECT COUNT(*) FROM bucketed
       WHERE bucket = ANY(%(active_buckets)s)
         AND event_ts < %(late_cutoff)s)                               AS atrasados_24h,
      (SELECT COALESCE(
         AVG(EXTRACT(EPOCH FROM (NOW() - event_ts)) / 60.0), 0
       ) FROM bucketed
       WHERE bucket = ANY(%(active_buckets)s))                        AS avg_age_min
    """

    kpi_params = {
        "nuevos":         STATUS_MAP["NUEVOS"],
        "recep":          STATUS_MAP["RECEPCION"],
        "prep":           STATUS_MAP["PREPARACION"],
        "pack":           STATUS_MAP["EMBALADO"],
        "desp":           STATUS_MAP["DESPACHO"],
        "env":            STATUS_MAP["ENVIADO"],
        "ent":            STATUS_MAP["ENTREGADO"],
        "cerr":           STATUS_MAP["CERRADOS"],
        "excluidos":      EXCLUDED_STATUSES or ["__never__"],
        "active_buckets": ACTIVE_BUCKETS,
        "late_cutoff":    late_cutoff,
        "today_start":    today_start,
    }

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(kpi_sql, kpi_params)
                kpi = cur.fetchone()
                cur.execute(
                    """
                    INSERT INTO orders_kpi_snapshot(
                      snapshot_ts, en_preparacion, embalados, en_despacho,
                      despachados_hoy, atrasados_24h, avg_age_min
                    ) VALUES (NOW(), %s, %s, %s, %s, %s, %s)
                    """,
                    kpi,
                )
            conn.commit()
            logger.info("Snapshot KPI grabado: %s", kpi)
    except Exception as exc:
        logger.error("Error guardando snapshot KPI: %s", exc)

    return {
        "ok":              True,
        "orders_received": len(all_orders),
        "changed":         changed,
        "events_inserted": events_inserted,
        "errors":          errors,
        "window_days":     SYNC_WINDOW_DAYS,
        "loops":           loops,
    }


@app.post("/sync")
def sync(request: Request):
    """Endpoint manual — útil para forzar un sync desde fuera o para crons externos."""
    if SYNC_SECRET:
        if request.headers.get("X-Sync-Secret") != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")
    return _run_sync()


# ===============================
# DIAGNÓSTICO (temporal)
# ===============================
@app.post("/api/debug/backfill-enviados")
def backfill_enviados():
    """Inserta eventos de hoy para pedidos en Enviado que no tienen evento de hoy."""
    from datetime import datetime
    now_local   = datetime.now(LOCAL_TZ)
    today_start = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ)

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Pedidos actualmente en Enviado sin evento de hoy
            cur.execute("""
                INSERT INTO order_events(order_id, status, event_ts)
                SELECT oc.order_id, oc.status, NOW()
                FROM orders_current oc
                WHERE oc.status = ANY(%s)
                  AND oc.status <> ALL(%s)
                  AND NOT EXISTS (
                    SELECT 1 FROM order_events oe
                    WHERE oe.order_id = oc.order_id
                      AND oe.status = oc.status
                      AND oe.event_ts::timestamptz >= %s
                  )
                RETURNING order_id
            """, (STATUS_MAP["ENVIADO"], EXCLUDED_STATUSES or ["__never__"], today_start))
            inserted = cur.rowcount
        conn.commit()

    return JSONResponse({"inserted": inserted})


@app.post("/api/debug/limpiar-enviados")
def limpiar_enviados_iniciales():
    """Borra eventos 'Enviado' del sync inicial (antes de las 08:00 ARG de hoy)."""
    from datetime import datetime
    now_local    = datetime.now(LOCAL_TZ)
    today_8am    = datetime(now_local.year, now_local.month, now_local.day, 8, 0, 0, tzinfo=LOCAL_TZ)
    today_start  = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM order_events
                WHERE status = ANY(%s)
                  AND event_ts::timestamptz >= %s
                  AND event_ts::timestamptz < %s
            """, (STATUS_MAP["ENVIADO"], today_start, today_8am))
            to_delete = cur.fetchone()[0]

            cur.execute("""
                DELETE FROM order_events
                WHERE status = ANY(%s)
                  AND event_ts::timestamptz >= %s
                  AND event_ts::timestamptz < %s
            """, (STATUS_MAP["ENVIADO"], today_start, today_8am))
        conn.commit()

    return JSONResponse({"deleted": to_delete, "cutoff": today_8am.isoformat()})


@app.get("/api/debug/enviados")
def debug_enviados():
    from datetime import datetime
    now_local   = datetime.now(LOCAL_TZ)
    today_start = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(DISTINCT order_id),
                       MIN(event_ts::timestamptz),
                       MAX(event_ts::timestamptz)
                FROM order_events
                WHERE status = ANY(%s)
                  AND event_ts::timestamptz >= %s
            """, (STATUS_MAP["ENVIADO"], today_start))
            row = cur.fetchone()
            cur.execute("""
                SELECT order_id, event_ts::timestamptz
                FROM order_events
                WHERE status = ANY(%s)
                  AND event_ts::timestamptz >= %s
                ORDER BY event_ts ASC
                LIMIT 5
            """, (STATUS_MAP["ENVIADO"], today_start))
            primeros = cur.fetchall()
    return JSONResponse({
        "today_start_arg": today_start.isoformat(),
        "now_arg": now_local.isoformat(),
        "count": row[0],
        "min_event_ts": row[1].isoformat() if row[1] else None,
        "max_event_ts": row[2].isoformat() if row[2] else None,
        "primeros_5": [{"order_id": r[0], "event_ts": r[1].isoformat()} for r in primeros],
    })
    now = datetime.now(timezone.utc)
    result = {"server_now_utc": now.isoformat()}

    with get_conn() as conn:
        with conn.cursor() as cur:
            # Últimos 3 snapshots
            cur.execute("""
                SELECT id, snapshot_ts, en_preparacion, embalados, en_despacho, despachados_hoy
                FROM orders_kpi_snapshot
                ORDER BY id DESC
                LIMIT 3
            """)
            result["snapshots"] = [
                {"id": r[0], "snapshot_ts": r[1].isoformat(), "en_prep": r[2], "embalados": r[3],
                 "en_despacho": r[4], "despachados_hoy": r[5]}
                for r in cur.fetchall()
            ]

            # Conteo de pedidos por estado
            cur.execute("""
                SELECT status, COUNT(*) AS n
                FROM orders_current
                GROUP BY status
                ORDER BY n DESC
                LIMIT 20
            """)
            result["orders_by_status"] = [
                {"status": r[0], "count": r[1]} for r in cur.fetchall()
            ]

            # Total de eventos
            cur.execute("SELECT COUNT(*) FROM order_events")
            result["total_events"] = cur.fetchone()[0]

            # Total snapshots
            cur.execute("SELECT COUNT(*), MAX(id), MIN(id) FROM orders_kpi_snapshot")
            r = cur.fetchone()
            result["snapshots_total"] = {"count": r[0], "max_id": r[1], "min_id": r[2]}

            # Evento más reciente
            cur.execute("""
                SELECT order_id, status, event_ts::timestamptz
                FROM order_events
                ORDER BY event_ts DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            result["last_event"] = {
                "order_id": row[0], "status": row[1], "event_ts": row[2].isoformat()
            } if row else None

    return JSONResponse(result)


@app.post("/api/debug/cleanup-snapshots")
def cleanup_snapshots():
    """Elimina snapshots duplicados, deja solo el más reciente por id. Solo para diagnóstico."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders_kpi_snapshot")
            before = cur.fetchone()[0]

            cur.execute("""
                DELETE FROM orders_kpi_snapshot
                WHERE id < (SELECT MAX(id) FROM orders_kpi_snapshot)
            """)
            cur.execute("SELECT COUNT(*) FROM orders_kpi_snapshot")
            after = cur.fetchone()[0]
        conn.commit()

    return JSONResponse({"deleted": before - after, "remaining": after})
    """Fuerza el recálculo del snapshot KPI sin correr sync completo. Solo para diagnóstico."""
    now         = datetime.now(timezone.utc)
    now_local   = datetime.now(LOCAL_TZ)
    today_start = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ)
    late_cutoff = now - timedelta(hours=24)

    kpi_sql = """
    WITH current_active AS (
      SELECT
        oc.order_id,
        oc.status,
        oc.updated_ts AS event_ts
      FROM orders_current oc
      WHERE oc.status <> ALL(%(excluidos)s)
    ),
    bucketed AS (
      SELECT
        order_id,
        event_ts,
        CASE
          WHEN status = ANY(%(nuevos)s) THEN 'NUEVOS'
          WHEN status = ANY(%(recep)s)  THEN 'RECEPCION'
          WHEN status = ANY(%(prep)s)   THEN 'PREPARACION'
          WHEN status = ANY(%(pack)s)   THEN 'EMBALADO'
          WHEN status = ANY(%(desp)s)   THEN 'DESPACHO'
          WHEN status = ANY(%(env)s)    THEN 'ENVIADO'
          WHEN status = ANY(%(ent)s)    THEN 'ENTREGADO'
          WHEN status = ANY(%(cerr)s)   THEN 'CERRADOS'
          ELSE 'OTROS'
        END AS bucket
      FROM current_active
    )
    SELECT
      (SELECT COUNT(*) FROM bucketed
       WHERE bucket IN ('NUEVOS','RECEPCION','PREPARACION'))           AS en_preparacion,
      (SELECT COUNT(*) FROM bucketed WHERE bucket = 'EMBALADO')        AS embalados,
      (SELECT COUNT(*) FROM bucketed WHERE bucket = 'DESPACHO')        AS en_despacho,
      (SELECT COUNT(DISTINCT order_id) FROM order_events
       WHERE status = ANY(%(env)s)
         AND status <> ALL(%(excluidos)s)
         AND event_ts::timestamptz >= %(today_start)s)                 AS despachados_hoy,
      (SELECT COUNT(*) FROM bucketed
       WHERE bucket = ANY(%(active_buckets)s)
         AND event_ts < %(late_cutoff)s)                               AS atrasados_24h,
      (SELECT COALESCE(
         AVG(EXTRACT(EPOCH FROM (NOW() - event_ts)) / 60.0), 0
       ) FROM bucketed
       WHERE bucket = ANY(%(active_buckets)s))                        AS avg_age_min
    """

    kpi_params = {
        "nuevos":         STATUS_MAP["NUEVOS"],
        "recep":          STATUS_MAP["RECEPCION"],
        "prep":           STATUS_MAP["PREPARACION"],
        "pack":           STATUS_MAP["EMBALADO"],
        "desp":           STATUS_MAP["DESPACHO"],
        "env":            STATUS_MAP["ENVIADO"],
        "ent":            STATUS_MAP["ENTREGADO"],
        "cerr":           STATUS_MAP["CERRADOS"],
        "excluidos":      EXCLUDED_STATUSES or ["__never__"],
        "active_buckets": ACTIVE_BUCKETS,
        "late_cutoff":    late_cutoff,
        "today_start":    today_start,
    }

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(kpi_sql, kpi_params)
            kpi = cur.fetchone()
            cur.execute(
                """
                INSERT INTO orders_kpi_snapshot(
                  snapshot_ts, en_preparacion, embalados, en_despacho,
                  despachados_hoy, atrasados_24h, avg_age_min
                ) VALUES (NOW(), %s, %s, %s, %s, %s, %s)
                RETURNING id, snapshot_ts
                """,
                kpi,
            )
            inserted = cur.fetchone()
        conn.commit()

    return JSONResponse({
        "ok": True,
        "inserted_id": inserted[0],
        "inserted_ts": inserted[1].isoformat(),
        "snapshot": {
            "en_preparacion":  int(kpi[0]),
            "embalados":       int(kpi[1]),
            "en_despacho":     int(kpi[2]),
            "despachados_hoy": int(kpi[3]),
            "atrasados_24h":   int(kpi[4]),
            "avg_age_min":     round(float(kpi[5]), 1),
            "snapshot_ts":     now.isoformat(),
        }
    })


# ===============================
# METRICS
# ===============================
@app.get("/api/metrics")
def metrics():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT en_preparacion, embalados, en_despacho,
                       despachados_hoy, atrasados_24h, avg_age_min, snapshot_ts
                FROM orders_kpi_snapshot
                ORDER BY id DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()

    now = datetime.now(timezone.utc)
    if not row:
        return JSONResponse({
            "en_preparacion": 0, "embalados": 0, "en_despacho": 0,
            "despachados_hoy": 0, "atrasados_24h": 0, "avg_age_min": 0.0,
            "refresh_seconds": REFRESH_SECONDS,
            "server_time_utc": now.isoformat(),
        })

    return JSONResponse({
        "en_preparacion":  int(row[0] or 0),
        "embalados":       int(row[1] or 0),
        "en_despacho":     int(row[2] or 0),
        "despachados_hoy": int(row[3] or 0),
        "atrasados_24h":   int(row[4] or 0),
        "avg_age_min":     float(row[5] or 0.0),
        "refresh_seconds": REFRESH_SECONDS,
        "server_time_utc": row[6].isoformat(),
    })


# ===============================
# TOP PRODUCTS
# ===============================
@app.get("/api/top-products")
def top_products(days: int = 30, limit: int = 10):
    since = datetime.now(timezone.utc) - timedelta(days=days)

    q = """
    SELECT
      COALESCE(NULLIF(oi.sku,  ''), '(sin sku)')    AS sku,
      COALESCE(NULLIF(oi.name, ''), '(sin nombre)') AS name,
      SUM(oi.quantity)::int            AS units,
      COUNT(DISTINCT oi.order_id)::int AS orders
    FROM order_items oi
    JOIN orders_meta    om ON om.order_id = oi.order_id
    JOIN orders_current oc ON oc.order_id = oi.order_id
    WHERE COALESCE(om.date_confirmed, om.date_add, NOW()) >= %s
      AND oc.status <> ALL(%s)
      AND oc.status <> ALL(%s)
    GROUP BY 1, 2
    ORDER BY units DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (since, STATUS_MAP["CERRADOS"], EXCLUDED_STATUSES or ["__never__"], limit))
            rows = cur.fetchall()

    return [{"sku": r[0], "name": r[1], "units": r[2], "orders": r[3]} for r in rows]


# ===============================
# TOP PACKERS
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
      AND status <> ALL(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY status
    ORDER BY orders_packed DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (PACKER_STATUSES, EXCLUDED_STATUSES or ["__never__"], since, limit))
            rows = cur.fetchall()

    return [{"packer": r[0], "orders_packed": r[1]} for r in rows]


# ===============================
# PACKERS HOURLY
# ===============================
@app.get("/api/packers-hourly")
def packers_hourly(hours: int = 24):
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    q = """
    SELECT
      date_trunc('hour', event_ts::timestamptz) AS hour,
      status                        AS packer,
      COUNT(DISTINCT order_id)::int AS orders_packed
    FROM order_events
    WHERE status = ANY(%s)
      AND status <> ALL(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY 1, 2
    ORDER BY 1 ASC, 2 ASC;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (PACKER_STATUSES, EXCLUDED_STATUSES or ["__never__"], since))
            rows = cur.fetchall()

    return [{"hour": r[0].isoformat(), "packer": r[1], "orders_packed": r[2]} for r in rows]



# ===============================
# PEDIDOS PENDIENTES (listado realtime)
# ===============================
@app.get("/api/pending-orders")
def pending_orders(limit: int = 200):
    active_statuses = (
        STATUS_MAP["NUEVOS"] +
        STATUS_MAP["RECEPCION"] +
        STATUS_MAP["PREPARACION"] +
        STATUS_MAP["EMBALADO"] +
        STATUS_MAP["DESPACHO"]
    )
    q = """
    SELECT
        oc.order_id,
        oc.status,
        oc.updated_ts,
        EXTRACT(EPOCH FROM (NOW() - COALESCE(oc.status_since, oc.updated_ts))) / 60.0 AS mins_in_status,
        COALESCE(
            json_agg(
                json_build_object(
                    'sku',      COALESCE(NULLIF(oi.sku, ''), '(sin sku)'),
                    'name',     COALESCE(NULLIF(oi.name, ''), '(sin nombre)'),
                    'quantity', oi.quantity
                ) ORDER BY oi.name
            ) FILTER (WHERE oi.order_product_id IS NOT NULL),
            '[]'::json
        ) AS products
    FROM orders_current oc
    LEFT JOIN order_items oi ON oi.order_id = oc.order_id
    WHERE oc.status = ANY(%s)
      AND oc.status <> ALL(%s)
    GROUP BY oc.order_id, oc.status, oc.updated_ts
    ORDER BY oc.order_id::bigint DESC
    LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (active_statuses, EXCLUDED_STATUSES or ["__never__"], limit))
            rows = cur.fetchall()
    return [
        {
            "order_id":       r[0],
            "status":         r[1],
            "updated_ts":     r[2].isoformat(),
            "mins_in_status": round(float(r[3]), 0),
            "products":       r[4],
        }
        for r in rows
    ]

# ===============================
# FRONT
# ===============================
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    return _render_index_html()