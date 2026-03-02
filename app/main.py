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
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

logger = logging.getLogger("uvicorn.error")

# ===============================
# ENV
# ===============================
DATABASE_URL = os.environ.get("DATABASE_URL")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "10"))

ML_APP_ID  = os.environ.get("ML_APP_ID", "")
ML_SECRET  = os.environ.get("ML_SECRET", "")
ML_SITE    = os.environ.get("ML_SITE", "MLA")  # Argentina
ML_API_URL = "https://api.mercadolibre.com"
ML_AUTH_URL = "https://auth.mercadolibre.com.ar/authorization"
ML_TOKEN_URL = "https://api.mercadolibre.com/oauth/token"
ML_REDIRECT_URI = os.environ.get("ML_REDIRECT_URI", "https://tc-slastatus-production.up.railway.app/ml/callback")

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

# Horarios de corte — configurable via CUTOFF_CONFIG en Railway
CUTOFF_CONFIG: dict = json.loads(
    os.environ.get("CUTOFF_CONFIG", ''' {"mercadoenvios": {"colectas": [{"nombre": "Colecta 1", "horarios": {"1": {"corte": "08:00", "llegada_desde": "11:00", "llegada_hasta": "13:00"}, "2": {"corte": "13:00", "llegada_desde": "16:00", "llegada_hasta": "18:00"}, "3": {"corte": "13:00", "llegada_desde": "16:00", "llegada_hasta": "18:00"}, "4": {"corte": "13:00", "llegada_desde": "16:00", "llegada_hasta": "18:00"}, "5": {"corte": "13:00", "llegada_desde": "16:00", "llegada_hasta": "18:00"}, "6": {"corte": "10:00", "llegada_desde": "11:00", "llegada_hasta": "14:00"}}}, {"nombre": "Colecta 2", "horarios": {"1": {"corte": "09:00", "llegada_desde": "12:00", "llegada_hasta": "14:00"}}}, {"nombre": "Colecta 3", "horarios": {"1": {"corte": "13:00", "llegada_desde": "16:00", "llegada_hasta": "18:00"}}}]}, "zonas": {"CABA": {"codigo_postal_desde": 1000, "codigo_postal_hasta": 1499, "corte": "16:00"}, "GBA": {"codigo_postal_desde": 1500, "codigo_postal_hasta": 9999, "corte": "14:00"}}} ''')
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
    last_exc = None
    for attempt in range(3):
        try:
            return psycopg.connect(DATABASE_URL, connect_timeout=5)
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                import time as _time
                _time.sleep(1 * (attempt + 1))
    raise last_exc


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

    while True:
        await asyncio.sleep(AUTO_SYNC_EVERY)
        try:
            result = await asyncio.to_thread(_run_sync)
            logger.info("Auto-sync OK: %s", result)
        except Exception as exc:
            logger.error("Auto-sync falló: %s", exc)



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
                  order_id          TEXT PRIMARY KEY,
                  date_confirmed    TIMESTAMPTZ NULL,
                  date_add          TIMESTAMPTZ NULL,
                  delivery_postcode TEXT NULL,
                  delivery_method   TEXT NULL,
                  updated_ts        TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
            # Migrar: agregar user_login a order_events si no existe
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'order_events' AND column_name = 'user_login'
                  ) THEN
                    ALTER TABLE order_events ADD COLUMN user_login TEXT NULL;
                  END IF;
                END $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_user_login ON order_events(user_login);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_meta_date_conf  ON orders_meta(date_confirmed);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id   ON order_items(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_items_sku        ON order_items(sku);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_kpi_snapshot_ts        ON orders_kpi_snapshot(snapshot_ts);")
            # Migrar: agregar delivery_method a orders_meta si no existe
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'orders_meta' AND column_name = 'delivery_method'
                  ) THEN
                    ALTER TABLE orders_meta ADD COLUMN delivery_method TEXT NULL;
                  END IF;
                END $$;
            """)
            # Migrar: agregar ml_order_id a orders_meta si no existe
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'orders_meta' AND column_name = 'ml_order_id'
                  ) THEN
                    ALTER TABLE orders_meta ADD COLUMN ml_order_id TEXT NULL;
                  END IF;
                END $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_meta_ml_order_id ON orders_meta(ml_order_id);")

            # Tabla de tokens ML por cuenta
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ml_tokens (
                  account_id    TEXT PRIMARY KEY,
                  access_token  TEXT NOT NULL,
                  refresh_token TEXT NOT NULL,
                  expires_at    TIMESTAMPTZ NOT NULL,
                  ml_user_id    TEXT NULL,
                  nickname      TEXT NULL,
                  updated_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)

            # Tabla de shipments ML
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ml_shipments (
                  shipment_id      TEXT PRIMARY KEY,
                  order_id         TEXT NULL,
                  ml_order_id      TEXT NULL,
                  status           TEXT NULL,
                  substatus        TEXT NULL,
                  shipping_type    TEXT NULL,
                  service_id       TEXT NULL,
                  date_created     TIMESTAMPTZ NULL,
                  last_updated     TIMESTAMPTZ NULL,
                  receiver_cp      TEXT NULL,
                  logistic_type    TEXT NULL,
                  cut_time         TIMESTAMPTZ NULL,
                  promised_date    TIMESTAMPTZ NULL,
                  raw              JSONB NULL,
                  updated_ts       TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ml_shipments_ml_order_id ON ml_shipments(ml_order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_ml_shipments_order_id    ON ml_shipments(order_id);")

            # Migrar: agregar delivery_postcode a orders_meta si no existe
            cur.execute("""
                DO $$
                BEGIN
                  IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'orders_meta' AND column_name = 'delivery_postcode'
                  ) THEN
                    ALTER TABLE orders_meta ADD COLUMN delivery_postcode TEXT NULL;
                  END IF;
                END $$;
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_orders_meta_postcode ON orders_meta(delivery_postcode);")
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

                # date_in_status: cuándo Baselinker dice que entró al estado actual
                dis = o.get("date_in_status")
                date_in_status = datetime.fromtimestamp(int(dis), tz=timezone.utc) if dis else None

                meta_rows.append((order_id, date_confirmed, date_add))
                user_login = (o.get("user_login") or "").strip() or None
                current_rows.append((order_id, status_name, date_in_status))

                prev_status = current_statuses.get(order_id)
                if prev_status != status_name:
                    b_changed += 1
                    event_ts = date_in_status or datetime.now(timezone.utc)
                    event_rows.append((order_id, status_name, event_ts, user_login))
                    current_statuses[order_id] = status_name

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
                        INSERT INTO orders_meta(order_id, date_confirmed, date_add, delivery_postcode, delivery_method, updated_ts)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT(order_id) DO UPDATE
                        SET date_confirmed    = EXCLUDED.date_confirmed,
                            date_add          = EXCLUDED.date_add,
                            delivery_postcode = EXCLUDED.delivery_postcode,
                            delivery_method   = EXCLUDED.delivery_method,
                            updated_ts        = EXCLUDED.updated_ts
                        """,
                        meta_rows,
                    )
                    cur.executemany(
                        """
                        INSERT INTO orders_current(order_id, status, updated_ts, status_since)
                        VALUES (%s, %s, NOW(), %s)
                        ON CONFLICT(order_id) DO UPDATE
                        SET updated_ts   = EXCLUDED.updated_ts,
                            status_since = CASE
                              WHEN orders_current.status <> EXCLUDED.status
                                THEN COALESCE(EXCLUDED.status_since, NOW())
                              ELSE COALESCE(orders_current.status_since, EXCLUDED.status_since, NOW())
                            END,
                            status     = EXCLUDED.status
                        """,
                        current_rows,
                    )
                    if event_rows:
                        cur.executemany(
                            "INSERT INTO order_events(order_id, status, event_ts, user_login) VALUES (%s, %s, %s, %s)",
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
      (SELECT COUNT(*) FROM orders_current
       WHERE status = ANY(%(prep_all)s)
         AND status <> ALL(%(excluidos)s)
         AND COALESCE(status_since, updated_ts) < %(late_cutoff)s)     AS atrasados_24h,
      (SELECT COALESCE(
         AVG(EXTRACT(EPOCH FROM (NOW() - COALESCE(oc2.status_since, oc2.updated_ts))) / 60.0), 0
       ) FROM orders_current oc2
       WHERE oc2.status = ANY(%(prep_all)s)
         AND oc2.status <> ALL(%(excluidos)s))                         AS avg_age_min
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
        "prep_all":       STATUS_MAP["PREPARACION"] + STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"],
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
async def sync(request: Request, background_tasks: BackgroundTasks):
    """Endpoint manual — dispara sync en background y devuelve inmediatamente."""
    if SYNC_SECRET:
        if request.headers.get("X-Sync-Secret") != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")
    background_tasks.add_task(_run_sync)
    return JSONResponse({"ok": True, "status": "sync iniciado en background"})


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
      (SELECT COUNT(*) FROM orders_current
       WHERE status = ANY(%(prep_all)s)
         AND status <> ALL(%(excluidos)s)
         AND COALESCE(status_since, updated_ts) < %(late_cutoff)s)     AS atrasados_24h,
      (SELECT COALESCE(
         AVG(EXTRACT(EPOCH FROM (NOW() - COALESCE(oc2.status_since, oc2.updated_ts))) / 60.0), 0
       ) FROM orders_current oc2
       WHERE oc2.status = ANY(%(prep_all)s)
         AND oc2.status <> ALL(%(excluidos)s))                         AS avg_age_min
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
        "prep_all":       STATUS_MAP["PREPARACION"] + STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"],
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
    JOIN orders_meta om ON om.order_id = oi.order_id
    JOIN orders_current oc ON oc.order_id = oi.order_id
    WHERE COALESCE(om.date_confirmed, om.date_add, NOW()) >= %s
      AND oc.status <> ALL(%s)
    GROUP BY 1, 2
    ORDER BY units DESC
    LIMIT %s;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (since, EXCLUDED_STATUSES or ["__never__"], limit))
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
# ALERTAS
# ===============================
def _get_zone_cutoff_mins(postcode: str | None) -> int | None:
    """Devuelve los minutos restantes hasta el corte de zona según el CP. None si no aplica."""
    if not postcode:
        return None
    try:
        cp = int("".join(filter(str.isdigit, postcode))[:4])
    except Exception:
        return None
    now_local = datetime.now(LOCAL_TZ)
    for zona, cfg in CUTOFF_CONFIG.get("zonas", {}).items():
        if cfg.get("codigo_postal_desde", 0) <= cp <= cfg.get("codigo_postal_hasta", 99999):
            h, m = map(int, cfg["corte"].split(":"))
            now_mins = now_local.hour * 60 + now_local.minute
            return (h * 60 + m) - now_mins
    return None


@app.get("/api/alerts")
def alerts():
    """Pedidos en preparación hace más de 2hs, priorizados por corte de zona."""
    prep_statuses = STATUS_MAP["PREPARACION"] + STATUS_MAP["NUEVOS"]
    cutoff_2h = datetime.now(timezone.utc) - timedelta(hours=2)

    q = """
    SELECT
        oc.order_id,
        oc.status,
        COALESCE(oc.status_since, oc.updated_ts) AS since,
        EXTRACT(EPOCH FROM (NOW() - COALESCE(oc.status_since, oc.updated_ts))) / 60.0 AS mins,
        COALESCE(
            json_agg(
                json_build_object('name', COALESCE(NULLIF(oi.name,''),'(sin nombre)'), 'quantity', oi.quantity)
                ORDER BY oi.name
            ) FILTER (WHERE oi.order_product_id IS NOT NULL),
            '[]'::json
        ) AS products,
        om.delivery_postcode
    FROM orders_current oc
    LEFT JOIN order_items oi ON oi.order_id = oc.order_id
    LEFT JOIN orders_meta om ON om.order_id = oc.order_id
    WHERE oc.status = ANY(%s)
      AND oc.status <> ALL(%s)
      AND COALESCE(oc.status_since, oc.updated_ts) < %s
    GROUP BY oc.order_id, oc.status, oc.status_since, oc.updated_ts, om.delivery_postcode
    ORDER BY since ASC
    LIMIT 100
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (prep_statuses, EXCLUDED_STATUSES or ["__never__"], cutoff_2h))
            rows = cur.fetchall()

    orders = []
    for r in rows:
        postcode = r[5]
        mins_to_zone_cutoff = _get_zone_cutoff_mins(postcode)

        # Urgencia: crítico si queda poco para el corte de zona
        urgency = "normal"
        if mins_to_zone_cutoff is not None:
            if mins_to_zone_cutoff < 0:
                urgency = "passed_cutoff"
            elif mins_to_zone_cutoff <= 30:
                urgency = "critical"
            elif mins_to_zone_cutoff <= 60:
                urgency = "warning"

        orders.append({
            "order_id":           r[0],
            "status":             r[1],
            "since":              r[2].isoformat(),
            "mins":               round(float(r[3]), 0),
            "products":           r[4],
            "postcode":           postcode,
            "mins_to_zone_cutoff": mins_to_zone_cutoff,
            "urgency":            urgency,
        })

    # Ordenar: críticos primero, luego por tiempo en estado
    orders.sort(key=lambda x: (
        0 if x["urgency"] == "critical" else
        1 if x["urgency"] == "warning" else
        2 if x["urgency"] == "passed_cutoff" else 3,
        x["mins"] * -1
    ))

    return JSONResponse({"count": len(orders), "orders": orders[:50]})


# ===============================
# PRODUCTIVIDAD POR DÍA
# ===============================
@app.get("/api/productivity")
def productivity(days: int = 7):
    """Pedidos despachados por día en los últimos N días."""
    since = datetime.now(LOCAL_TZ) - timedelta(days=days)
    since_utc = since.astimezone(timezone.utc)

    q = """
    SELECT
        (event_ts::timestamptz AT TIME ZONE %s)::date AS day_local,
        COUNT(DISTINCT order_id)::int AS shipped
    FROM order_events
    WHERE status = ANY(%s)
      AND status <> ALL(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY 1
    ORDER BY 1 ASC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (TZ_NAME, STATUS_MAP["ENVIADO"], EXCLUDED_STATUSES or ["__never__"], since_utc))
            rows = cur.fetchall()

    return [{"day": str(r[0]), "shipped": r[1]} for r in rows]


# ===============================
# TIEMPO PROMEDIO POR ETAPA
# ===============================
@app.get("/api/stage-times")
def stage_times(days: int = 7):
    """
    Tiempo promedio que un pedido pasa en cada etapa,
    calculado desde los eventos de los últimos N días.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # Para cada pedido, calculamos el tiempo entre el primer evento de cada bucket
    # y el primer evento del bucket siguiente
    q = """
    WITH events_bucketed AS (
        SELECT
            order_id,
            event_ts::timestamptz AS ts,
            CASE
                WHEN status = ANY(%s) THEN 'PREPARACION'
                WHEN status = ANY(%s) THEN 'EMBALADO'
                WHEN status = ANY(%s) THEN 'DESPACHO'
                WHEN status = ANY(%s) THEN 'ENVIADO'
            END AS bucket
        FROM order_events
        WHERE event_ts::timestamptz >= %s
          AND status <> ALL(%s)
    ),
    first_per_bucket AS (
        SELECT order_id, bucket, MIN(ts) AS first_ts
        FROM events_bucketed
        WHERE bucket IS NOT NULL
        GROUP BY order_id, bucket
    ),
    transitions AS (
        SELECT
            a.order_id,
            a.bucket AS from_bucket,
            b.bucket AS to_bucket,
            EXTRACT(EPOCH FROM (b.first_ts - a.first_ts)) / 60.0 AS mins
        FROM first_per_bucket a
        JOIN first_per_bucket b ON b.order_id = a.order_id AND b.first_ts > a.first_ts
        WHERE (a.bucket = 'PREPARACION' AND b.bucket = 'EMBALADO')
           OR (a.bucket = 'EMBALADO'    AND b.bucket = 'DESPACHO')
           OR (a.bucket = 'DESPACHO'    AND b.bucket = 'ENVIADO')
           OR (a.bucket = 'PREPARACION' AND b.bucket = 'ENVIADO')
    ),
    min_transitions AS (
        SELECT order_id, from_bucket, to_bucket, MIN(mins) AS mins
        FROM transitions
        GROUP BY order_id, from_bucket, to_bucket
    )
    SELECT
        from_bucket,
        to_bucket,
        ROUND(AVG(mins)::numeric, 1)  AS avg_mins,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY mins)::numeric, 1) AS median_mins,
        COUNT(*)::int AS sample
    FROM min_transitions
    WHERE mins > 0 AND mins < 1440  -- ignorar outliers > 24hs
    GROUP BY from_bucket, to_bucket
    ORDER BY from_bucket, to_bucket
    """

    prep_all = STATUS_MAP["PREPARACION"] + STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (
                prep_all,
                STATUS_MAP["EMBALADO"],
                STATUS_MAP["DESPACHO"],
                STATUS_MAP["ENVIADO"],
                since,
                EXCLUDED_STATUSES or ["__never__"],
            ))
            rows = cur.fetchall()

    labels = {
        ("PREPARACION", "EMBALADO"):  "Prep → Embalado",
        ("EMBALADO",    "DESPACHO"):  "Embalado → Despacho",
        ("DESPACHO",    "ENVIADO"):   "Despacho → Enviado",
        ("PREPARACION", "ENVIADO"):   "Prep → Enviado (total)",
    }

    return [
        {
            "stage":      labels.get((r[0], r[1]), f"{r[0]} → {r[1]}"),
            "avg_mins":   float(r[2]),
            "median_mins": float(r[3]),
            "sample":     r[4],
        }
        for r in rows
    ]



# ===============================
# RANKING BUSCADORES
# ===============================
@app.get("/api/top-pickers")
def top_pickers(days: int = 1, limit: int = 10):
    """Ranking de buscadores por pedidos procesados en estados de preparación."""
    since = datetime.now(LOCAL_TZ) - timedelta(days=days)
    since_utc = since.astimezone(timezone.utc)
    picker_statuses = STATUS_MAP["PREPARACION"]  # Colecta, Flex, Turbo, Recolectando

    q = """
    SELECT
        user_login,
        COUNT(DISTINCT order_id)::int AS orders_picked,
        MIN(event_ts)::timestamptz    AS first_event,
        MAX(event_ts)::timestamptz    AS last_event
    FROM order_events
    WHERE status = ANY(%s)
      AND user_login IS NOT NULL
      AND user_login <> ''
      AND event_ts::timestamptz >= %s
    GROUP BY user_login
    ORDER BY orders_picked DESC
    LIMIT %s
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (picker_statuses, since_utc, limit))
            rows = cur.fetchall()

    return [
        {
            "user":          r[0],
            "orders_picked": r[1],
            "first_event":   r[2].isoformat(),
            "last_event":    r[3].isoformat(),
        }
        for r in rows
    ]


# ===============================
# UNIDADES EN PREPARACIÓN
# ===============================
@app.get("/api/units-in-prep")
def units_in_prep():
    """Total de unidades (no pedidos) en estados de preparación."""
    prep_statuses = STATUS_MAP["PREPARACION"] + STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"]
    q = """
    SELECT
        COALESCE(SUM(oi.quantity), 0)::int AS total_units,
        COUNT(DISTINCT oc.order_id)::int   AS total_orders
    FROM orders_current oc
    JOIN order_items oi ON oi.order_id = oc.order_id
    WHERE oc.status = ANY(%s)
      AND oc.status <> ALL(%s)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (prep_statuses, EXCLUDED_STATUSES or ["__never__"]))
            r = cur.fetchone()
    return {"total_units": r[0], "total_orders": r[1]}


# ===============================
# VELOCIDAD DE DESPACHO
# ===============================
@app.get("/api/dispatch-rate")
def dispatch_rate():
    """Pedidos despachados por hora en las últimas 8 horas."""
    since = datetime.now(timezone.utc) - timedelta(hours=8)
    q = """
    SELECT
        date_trunc('hour', event_ts::timestamptz AT TIME ZONE %s) AS hour_local,
        COUNT(DISTINCT order_id)::int AS shipped
    FROM order_events
    WHERE status = ANY(%s)
      AND status <> ALL(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY 1
    ORDER BY 1 ASC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (TZ_NAME, STATUS_MAP["ENVIADO"], EXCLUDED_STATUSES or ["__never__"], since))
            rows = cur.fetchall()
    return [{"hour": r[0].isoformat(), "shipped": r[1]} for r in rows]


# ===============================
# PEDIDOS SIN MOVIMIENTO
# ===============================
@app.get("/api/stalled-orders")
def stalled_orders(hours: int = 4, limit: int = 50):
    """Pedidos activos que no cambiaron de estado en más de X horas."""
    active_statuses = (
        STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"] +
        STATUS_MAP["PREPARACION"] + STATUS_MAP["EMBALADO"] + STATUS_MAP["DESPACHO"]
    )
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    q = """
    SELECT
        oc.order_id,
        oc.status,
        COALESCE(oc.status_since, oc.updated_ts) AS since,
        EXTRACT(EPOCH FROM (NOW() - COALESCE(oc.status_since, oc.updated_ts))) / 60.0 AS mins,
        COALESCE(
            json_agg(json_build_object('name', COALESCE(NULLIF(oi.name,''),'(sin nombre)'), 'quantity', oi.quantity) ORDER BY oi.name)
            FILTER (WHERE oi.order_product_id IS NOT NULL), '[]'::json
        ) AS products
    FROM orders_current oc
    LEFT JOIN order_items oi ON oi.order_id = oc.order_id
    WHERE oc.status = ANY(%s)
      AND oc.status <> ALL(%s)
      AND COALESCE(oc.status_since, oc.updated_ts) < %s
    GROUP BY oc.order_id, oc.status, oc.status_since, oc.updated_ts
    ORDER BY since ASC
    LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (active_statuses, EXCLUDED_STATUSES or ["__never__"], cutoff, limit))
            rows = cur.fetchall()
    return {
        "count": len(rows),
        "hours": hours,
        "orders": [{"order_id": r[0], "status": r[1], "since": r[2].isoformat(),
                    "mins": round(float(r[3]), 0), "products": r[4]} for r in rows]
    }


# ===============================
# COMPARATIVA SEMANAS
# ===============================
@app.get("/api/weekly-comparison")
def weekly_comparison():
    """Pedidos despachados por día: semana actual vs semana anterior."""
    now_local = datetime.now(LOCAL_TZ)
    # Inicio de esta semana (lunes)
    days_since_monday = now_local.weekday()
    this_monday = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ) - timedelta(days=days_since_monday)
    last_monday = this_monday - timedelta(days=7)

    q = """
    SELECT
        (event_ts::timestamptz AT TIME ZONE %s)::date AS day_local,
        COUNT(DISTINCT order_id)::int AS shipped
    FROM order_events
    WHERE status = ANY(%s)
      AND status <> ALL(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY 1
    ORDER BY 1 ASC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (TZ_NAME, STATUS_MAP["ENVIADO"], EXCLUDED_STATUSES or ["__never__"], last_monday))
            rows = cur.fetchall()

    by_date = {str(r[0]): r[1] for r in rows}
    days_es = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    result = []
    for i in range(7):
        this_day  = (this_monday + timedelta(days=i)).date()
        last_day  = (last_monday + timedelta(days=i)).date()
        result.append({
            "weekday":    days_es[i],
            "this_week":  by_date.get(str(this_day), 0),
            "last_week":  by_date.get(str(last_day), 0),
        })
    return result


# ===============================
# HORA PICO
# ===============================
@app.get("/api/peak-hours")
def peak_hours(days: int = 7):
    """Distribución de despachos por hora del día en los últimos N días."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    q = """
    SELECT
        EXTRACT(HOUR FROM event_ts::timestamptz AT TIME ZONE %s)::int AS hour_of_day,
        COUNT(DISTINCT order_id)::int AS shipped
    FROM order_events
    WHERE status = ANY(%s)
      AND status <> ALL(%s)
      AND event_ts::timestamptz >= %s
    GROUP BY 1
    ORDER BY 1 ASC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (TZ_NAME, STATUS_MAP["ENVIADO"], EXCLUDED_STATUSES or ["__never__"], since))
            rows = cur.fetchall()
    by_hour = {r[0]: r[1] for r in rows}
    return [{"hour": h, "shipped": by_hour.get(h, 0)} for h in range(24)]


# ===============================
# TASA DE CANCELACIÓN
# ===============================
@app.get("/api/cancellation-rate")
def cancellation_rate(days: int = 7):
    """Pedidos cancelados vs total por día en los últimos N días."""
    since_utc = datetime.now(timezone.utc) - timedelta(days=days)
    q = """
    SELECT
        (COALESCE(om.date_confirmed, om.date_add) AT TIME ZONE %s)::date AS day_local,
        COUNT(DISTINCT oc.order_id)::int AS total,
        COUNT(DISTINCT CASE WHEN oc.status = ANY(%s) THEN oc.order_id END)::int AS cancelled
    FROM orders_current oc
    JOIN orders_meta om ON om.order_id = oc.order_id
    WHERE COALESCE(om.date_confirmed, om.date_add) >= %s
    GROUP BY 1
    ORDER BY 1 ASC
    """
    cancelled_statuses = ["Cancelado"]
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (TZ_NAME, cancelled_statuses, since_utc))
            rows = cur.fetchall()
    return [{"day": str(r[0]), "total": r[1], "cancelled": r[2],
             "rate": round(r[2]/r[1]*100, 1) if r[1] > 0 else 0} for r in rows]


# ===============================
# PEDIDOS QUE VOLVIERON ATRÁS
# ===============================
@app.get("/api/backtracked-orders")
def backtracked_orders(days: int = 7, limit: int = 50):
    """Pedidos que pasaron de un estado avanzado a uno anterior."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    bucket_order = {
        "NUEVOS": 1, "RECEPCION": 2, "PREPARACION": 3,
        "EMBALADO": 4, "DESPACHO": 5, "ENVIADO": 6, "ENTREGADO": 7
    }

    def bucket_of(status):
        for bucket, statuses in STATUS_MAP.items():
            if status in statuses:
                return bucket
        return None

    q = """
    SELECT order_id, status, event_ts::timestamptz
    FROM order_events
    WHERE event_ts::timestamptz >= %s
      AND status <> ALL(%s)
    ORDER BY order_id, event_ts ASC
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (since, EXCLUDED_STATUSES or ["__never__"]))
            rows = cur.fetchall()

    from collections import defaultdict
    order_events_map = defaultdict(list)
    for r in rows:
        order_events_map[r[0]].append((r[1], r[2]))

    backtracked = []
    for order_id, events in order_events_map.items():
        max_bucket_seen = 0
        for status, ts in events:
            b = bucket_of(status)
            if b is None:
                continue
            level = bucket_order.get(b, 0)
            if level < max_bucket_seen and max_bucket_seen >= 3:
                backtracked.append({
                    "order_id": order_id,
                    "status":   status,
                    "ts":       ts.isoformat(),
                })
                break
            max_bucket_seen = max(max_bucket_seen, level)

    backtracked.sort(key=lambda x: x["ts"], reverse=True)
    return {"count": len(backtracked), "orders": backtracked[:limit]}


# ===============================
# TIEMPO HASTA PRIMER MOVIMIENTO
# ===============================
@app.get("/api/time-to-first-move")
def time_to_first_move(days: int = 7):
    """Tiempo promedio desde confirmación hasta que el pedido entra a preparación."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    prep_all = STATUS_MAP["PREPARACION"] + STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"]
    q = """
    WITH first_prep AS (
        SELECT order_id, MIN(event_ts::timestamptz) AS first_prep_ts
        FROM order_events
        WHERE status = ANY(%s)
          AND event_ts::timestamptz >= %s
        GROUP BY order_id
    )
    SELECT
        ROUND(AVG(EXTRACT(EPOCH FROM (fp.first_prep_ts - COALESCE(om.date_confirmed, om.date_add))) / 60.0)::numeric, 1) AS avg_mins,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
            ORDER BY EXTRACT(EPOCH FROM (fp.first_prep_ts - COALESCE(om.date_confirmed, om.date_add))) / 60.0
        )::numeric, 1) AS median_mins,
        COUNT(*)::int AS sample
    FROM first_prep fp
    JOIN orders_meta om ON om.order_id = fp.order_id
    WHERE COALESCE(om.date_confirmed, om.date_add) IS NOT NULL
      AND fp.first_prep_ts > COALESCE(om.date_confirmed, om.date_add)
      AND EXTRACT(EPOCH FROM (fp.first_prep_ts - COALESCE(om.date_confirmed, om.date_add))) / 60.0 < 1440
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (prep_all, since))
            r = cur.fetchone()
    return {
        "avg_mins":    float(r[0]) if r[0] else 0,
        "median_mins": float(r[1]) if r[1] else 0,
        "sample":      r[2] or 0,
    }


# ===============================
# HORARIOS DE CORTE
# ===============================
@app.get("/api/cutoffs")
def cutoffs():
    """Devuelve los próximos cortes del día con conteo de pedidos pendientes."""
    now_local = datetime.now(LOCAL_TZ)
    weekday   = str(now_local.isoweekday())
    now_time  = now_local.strftime("%H:%M")

    def mins_until(t: str) -> int:
        h, m   = map(int, t.split(":"))
        hn, mn = map(int, now_time.split(":"))
        return (h * 60 + m) - (hn * 60 + mn)

    def cutoff_status(mins: int) -> str:
        if mins < 0:    return "passed"
        if mins <= 30:  return "urgent"
        if mins <= 60:  return "warning"
        return "pending"

    # ── Conteos desde DB ──────────────────────────────────────────────────────
    me_statuses   = ["Mercado Envíos"]
    flex_statuses = [s for s in STATUS_MAP.get("DESPACHO", []) if s not in me_statuses]
    active_statuses = (
        STATUS_MAP["NUEVOS"] + STATUS_MAP["RECEPCION"] +
        STATUS_MAP["PREPARACION"] + STATUS_MAP["EMBALADO"] + STATUS_MAP["DESPACHO"]
    )

    counts = {"me": 0, "caba": 0, "gba": 0}
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Mercado Envíos: todos los pedidos en estado "Mercado Envíos"
                cur.execute("""
                    SELECT COUNT(*) FROM orders_current
                    WHERE status = ANY(%s)
                """, (me_statuses,))
                counts["me"] = cur.fetchone()[0]

                # Flex CABA: transportistas flex con CP 1000-1499
                cur.execute("""
                    SELECT COUNT(*) FROM orders_current oc
                    JOIN orders_meta om ON om.order_id = oc.order_id
                    WHERE oc.status = ANY(%s)
                      AND om.delivery_postcode ~ '^[0-9]+$'
                      AND CAST(LEFT(om.delivery_postcode, 4) AS INT)
                          BETWEEN %s AND %s
                """, (flex_statuses, 1000, 1499))
                counts["caba"] = cur.fetchone()[0]

                # Flex GBA: transportistas flex con CP 1500-9999
                cur.execute("""
                    SELECT COUNT(*) FROM orders_current oc
                    JOIN orders_meta om ON om.order_id = oc.order_id
                    WHERE oc.status = ANY(%s)
                      AND om.delivery_postcode ~ '^[0-9]+$'
                      AND CAST(LEFT(om.delivery_postcode, 4) AS INT)
                          BETWEEN %s AND %s
                """, (flex_statuses, 1500, 9999))
                counts["gba"] = cur.fetchone()[0]
    except Exception as e:
        logger.error("cutoffs count error: %s", e)

    result = {"mercadoenvios": [], "zonas": [], "now": now_local.isoformat()}

    # ── Mercado Envíos colectas ───────────────────────────────────────────────
    for colecta in CUTOFF_CONFIG.get("mercadoenvios", {}).get("colectas", []):
        horario = colecta.get("horarios", {}).get(weekday)
        if not horario:
            continue
        corte         = horario["corte"]
        mins_to_corte = mins_until(corte)
        result["mercadoenvios"].append({
            "nombre":        colecta["nombre"],
            "corte":         corte,
            "llegada_desde": horario["llegada_desde"],
            "llegada_hasta": horario["llegada_hasta"],
            "mins_to_corte": mins_to_corte,
            "status":        cutoff_status(mins_to_corte),
            "pending_count": counts["me"],
        })

    # ── Zonas ─────────────────────────────────────────────────────────────────
    for zona, cfg in CUTOFF_CONFIG.get("zonas", {}).items():
        corte         = cfg["corte"]
        mins_to_corte = mins_until(corte)
        zona_key      = "caba" if zona.upper() == "CABA" else "gba"
        result["zonas"].append({
            "zona":          zona,
            "corte":         corte,
            "cp_desde":      cfg.get("codigo_postal_desde"),
            "cp_hasta":      cfg.get("codigo_postal_hasta"),
            "mins_to_corte": mins_to_corte,
            "status":        cutoff_status(mins_to_corte),
            "pending_count": counts[zona_key],
        })

    return JSONResponse(result)


# ===============================
# MERCADO LIBRE — OAuth + Sync
# ===============================

def _ml_get_tokens(account_id: str) -> dict | None:
    """Lee tokens de la DB para una cuenta."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT access_token, refresh_token, expires_at, ml_user_id, nickname
                FROM ml_tokens WHERE account_id = %s
            """, (account_id,))
            row = cur.fetchone()
    if not row:
        return None
    return {
        "access_token":  row[0],
        "refresh_token": row[1],
        "expires_at":    row[2],
        "ml_user_id":    row[3],
        "nickname":      row[4],
    }


def _ml_save_tokens(account_id: str, data: dict) -> None:
    """Guarda/actualiza tokens en la DB."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 21600) - 300)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ml_tokens(account_id, access_token, refresh_token, expires_at, ml_user_id, updated_ts)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT(account_id) DO UPDATE
                SET access_token  = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    expires_at    = EXCLUDED.expires_at,
                    ml_user_id    = COALESCE(EXCLUDED.ml_user_id, ml_tokens.ml_user_id),
                    updated_ts    = NOW()
            """, (account_id, data["access_token"], data["refresh_token"],
                  expires_at, str(data.get("user_id", ""))))
        conn.commit()


def _ml_refresh_token(account_id: str, refresh_token: str) -> dict | None:
    """Renueva el access_token usando el refresh_token."""
    try:
        r = requests.post(ML_TOKEN_URL, json={
            "grant_type":    "refresh_token",
            "client_id":     ML_APP_ID,
            "client_secret": ML_SECRET,
            "refresh_token": refresh_token,
        }, timeout=15)
        r.raise_for_status()
        data = r.json()
        _ml_save_tokens(account_id, data)
        logger.info("ML token renovado para cuenta %s", account_id)
        return data
    except Exception as e:
        logger.error("Error renovando token ML cuenta %s: %s", account_id, e)
        return None


def _ml_get_valid_token(account_id: str) -> str | None:
    """Devuelve un access_token válido, renovándolo si es necesario."""
    tokens = _ml_get_tokens(account_id)
    if not tokens:
        return None
    # Renovar si expira en menos de 10 minutos
    if tokens["expires_at"] <= datetime.now(timezone.utc) + timedelta(minutes=10):
        data = _ml_refresh_token(account_id, tokens["refresh_token"])
        return data["access_token"] if data else None
    return tokens["access_token"]


def _ml_list_accounts() -> list[str]:
    """Lista todas las cuentas ML autorizadas."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT account_id, nickname FROM ml_tokens ORDER BY account_id")
            rows = cur.fetchall()
    return [{"account_id": r[0], "nickname": r[1]} for r in rows]


def _ml_sync_shipments(account_id: str) -> dict:
    """Sincroniza shipments de ML para una cuenta — últimas 48hs."""
    token = _ml_get_valid_token(account_id)
    if not token:
        return {"error": "no_token", "account_id": account_id}

    headers = {"Authorization": f"Bearer {token}"}
    synced = 0
    errors = 0

    # Traer pedidos activos con ml_order_id
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT om.ml_order_id, oc.order_id
                FROM orders_meta om
                JOIN orders_current oc ON oc.order_id = om.order_id
                WHERE om.ml_order_id IS NOT NULL
                  AND om.ml_order_id <> ''
                  AND oc.status <> ALL(%s)
                ORDER BY om.updated_ts DESC
                LIMIT 500
            """, (["Cancelado", "Full", "Pedidos antiguos", "Entregado"] + EXCLUDED_STATUSES,))
            rows = cur.fetchall()

    for ml_order_id, order_id in rows:
        try:
            # Obtener shipment del pedido ML
            r = requests.get(
                f"{ML_API_URL}/orders/{ml_order_id}/shipments",
                headers=headers, timeout=10
            )
            if r.status_code == 404:
                continue
            r.raise_for_status()
            ship = r.json()

            shipment_id  = str(ship.get("id", ""))
            if not shipment_id:
                continue

            # Extraer datos relevantes
            receiver     = ship.get("receiver_address", {})
            cp           = receiver.get("zip_code", "")
            cut_time     = None
            promised     = None

            # Fechas de SLA
            shipping_option = ship.get("shipping_option", {}) or {}
            estimated = shipping_option.get("estimated_delivery_time", {}) or {}
            if estimated.get("date"):
                try:
                    promised = datetime.fromisoformat(estimated["date"].replace("Z", "+00:00"))
                except Exception:
                    pass

            # Cut time si viene
            if ship.get("cut_time"):
                try:
                    cut_time = datetime.fromisoformat(ship["cut_time"].replace("Z", "+00:00"))
                except Exception:
                    pass

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO ml_shipments(
                            shipment_id, order_id, ml_order_id, status, substatus,
                            shipping_type, service_id, date_created, last_updated,
                            receiver_cp, logistic_type, cut_time, promised_date, raw, updated_ts
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT(shipment_id) DO UPDATE
                        SET status        = EXCLUDED.status,
                            substatus     = EXCLUDED.substatus,
                            last_updated  = EXCLUDED.last_updated,
                            cut_time      = EXCLUDED.cut_time,
                            promised_date = EXCLUDED.promised_date,
                            raw           = EXCLUDED.raw,
                            updated_ts    = NOW()
                    """, (
                        shipment_id, order_id, ml_order_id,
                        ship.get("status"), ship.get("substatus"),
                        ship.get("shipping_type"), str(ship.get("service_id", "")),
                        ship.get("date_created"), ship.get("last_updated"),
                        cp, ship.get("logistic_type"),
                        cut_time, promised,
                        json.dumps(ship),
                    ))
                conn.commit()
            synced += 1
            time.sleep(0.05)  # rate limit gentil

        except Exception as e:
            logger.error("ML shipment error order %s: %s", ml_order_id, e)
            errors += 1

    return {"account_id": account_id, "synced": synced, "errors": errors}


# ── OAuth endpoints ────────────────────────────────────────────────────────────

@app.get("/ml/auth/{account_id}")
def ml_auth(account_id: str):
    """Redirige a ML para autorizar una cuenta."""
    from fastapi.responses import RedirectResponse
    url = (
        f"{ML_AUTH_URL}"
        f"?response_type=code"
        f"&client_id={ML_APP_ID}"
        f"&redirect_uri={ML_REDIRECT_URI}"
        f"&state={account_id}"
    )
    return RedirectResponse(url)


@app.get("/ml/callback")
def ml_callback(code: str, state: str = "default"):
    """Recibe el código OAuth de ML y lo intercambia por tokens."""
    account_id = state
    try:
        r = requests.post(ML_TOKEN_URL, json={
            "grant_type":   "authorization_code",
            "client_id":    ML_APP_ID,
            "client_secret": ML_SECRET,
            "code":         code,
            "redirect_uri": ML_REDIRECT_URI,
        }, timeout=15)
        r.raise_for_status()
        data = r.json()
        _ml_save_tokens(account_id, data)

        # Guardar nickname
        token = data["access_token"]
        user_r = requests.get(f"{ML_API_URL}/users/me", headers={"Authorization": f"Bearer {token}"}, timeout=10)
        if user_r.ok:
            udata = user_r.json()
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE ml_tokens SET nickname=%s, ml_user_id=%s WHERE account_id=%s",
                                (udata.get("nickname"), str(udata.get("id","")), account_id))
                conn.commit()

        return HTMLResponse(f"""
            <html><body style="font-family:sans-serif;padding:40px;background:#0b0f14;color:#e6edf3">
            <h2>✅ Cuenta autorizada</h2>
            <p>Cuenta <b>{account_id}</b> conectada correctamente.</p>
            <p><a href="/" style="color:#58a6ff">← Volver al dashboard</a></p>
            </body></html>
        """)
    except Exception as e:
        logger.error("ML OAuth error: %s", e)
        return HTMLResponse(f"<h2>Error: {e}</h2>", status_code=500)


@app.get("/ml/accounts")
def ml_accounts():
    """Lista las cuentas ML autorizadas."""
    return _ml_list_accounts()


@app.post("/ml/sync")
def ml_sync_all(background_tasks: BackgroundTasks):
    """Dispara sync de shipments para todas las cuentas."""
    def _run():
        accounts = _ml_list_accounts()
        for acc in accounts:
            result = _ml_sync_shipments(acc["account_id"])
            logger.info("ML sync: %s", result)
    background_tasks.add_task(_run)
    return {"ok": True, "status": "ML sync iniciado en background"}


@app.post("/ml/notifications")
async def ml_notifications(request: Request):
    """Recibe notificaciones push de ML y dispara sync del pedido afectado."""
    try:
        body = await request.json()
        topic   = body.get("topic", "")
        res_id  = body.get("resource", "")
        user_id = str(body.get("user_id", ""))
        logger.info("ML notification: topic=%s resource=%s user=%s", topic, res_id, user_id)

        if topic in ("shipments", "orders_v2") and res_id:
            # Buscar la cuenta por ml_user_id
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT account_id FROM ml_tokens WHERE ml_user_id=%s LIMIT 1", (user_id,))
                    row = cur.fetchone()
            if row:
                account_id = row[0]
                token = _ml_get_valid_token(account_id)
                if token:
                    # Sync del recurso específico
                    headers = {"Authorization": f"Bearer {token}"}
                    r = requests.get(f"{ML_API_URL}{res_id}", headers=headers, timeout=10)
                    if r.ok:
                        ship = r.json()
                        logger.info("ML notif sync OK: %s", ship.get("id"))
    except Exception as e:
        logger.error("ML notification error: %s", e)
    return JSONResponse({"status": "ok"})


@app.get("/api/ml-shipments")
def ml_shipments_summary():
    """Resumen de shipments ML — colectas asignadas por cut_time."""
    now_local = datetime.now(LOCAL_TZ)
    today_start = datetime(now_local.year, now_local.month, now_local.day, tzinfo=LOCAL_TZ).astimezone(timezone.utc)
    tomorrow    = today_start + timedelta(days=1)

    q = """
    SELECT
        s.shipment_id,
        s.order_id,
        s.status,
        s.logistic_type,
        s.cut_time AT TIME ZONE %s AS cut_local,
        s.promised_date AT TIME ZONE %s AS promised_local,
        s.receiver_cp,
        oc.status AS current_status
    FROM ml_shipments s
    LEFT JOIN orders_current oc ON oc.order_id = s.order_id
    WHERE s.cut_time >= %s AND s.cut_time < %s
      AND (oc.status IS NULL OR oc.status <> ALL(%s))
    ORDER BY s.cut_time ASC
    LIMIT 200
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (TZ_NAME, TZ_NAME, today_start, tomorrow,
                           EXCLUDED_STATUSES or ["__never__"]))
            rows = cur.fetchall()

    return [
        {
            "shipment_id":    r[0],
            "order_id":       r[1],
            "status":         r[2],
            "logistic_type":  r[3],
            "cut_time":       r[4].isoformat() if r[4] else None,
            "promised_date":  r[5].isoformat() if r[5] else None,
            "receiver_cp":    r[6],
            "current_status": r[7],
        }
        for r in rows
    ]



@app.post("/api/debug/ml-backfill")
async def ml_backfill(background_tasks: BackgroundTasks):
    """Backfill ml_order_id desde Baselinker para todos los pedidos sin él."""
    def _run():
        # Usar misma paginación por cursor de fecha que el sync principal
        from_ts = 1700000000  # desde Nov 2023
        filled  = 0
        loops   = 0
        max_loops = 200

        while loops < max_loops:
            loops += 1
            try:
                data = bl_call("getOrders", {
                    "date_confirmed_from": from_ts,
                    "get_unconfirmed_orders": True,
                })
                orders = data.get("orders", [])
                if not orders:
                    break

                rows = []
                for o in orders:
                    eid = (o.get("extra_field_1") or "").strip()
                    if eid:
                        rows.append((eid, str(o.get("order_id", ""))))

                if rows:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            for ml_id, order_id in rows:
                                cur.execute("""
                                    UPDATE orders_meta SET ml_order_id = %s
                                    WHERE order_id = %s
                                      AND (ml_order_id IS NULL OR ml_order_id = '')
                                """, (ml_id, order_id))
                        conn.commit()
                    filled += len(rows)

                if len(orders) < 100:
                    break

                # Avanzar cursor igual que el sync principal
                max_ts = from_ts
                for o in orders:
                    for field in ("date_confirmed", "date_add"):
                        try:
                            max_ts = max(max_ts, int(o.get(field) or 0))
                        except Exception:
                            pass
                if max_ts <= from_ts:
                    break
                from_ts = max_ts

            except Exception as e:
                logger.error("ML backfill error loop %d: %s", loops, e)
                break

        logger.info("ML backfill completo: %d pedidos actualizados en %d loops", filled, loops)

    background_tasks.add_task(_run)
    return {"ok": True, "status": "ML backfill iniciado en background"}

@app.get("/api/debug/ml")
def debug_ml():
    """Debug: estado de integración ML."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders_meta WHERE ml_order_id IS NOT NULL")
            with_ml = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM orders_meta")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM ml_shipments")
            shipments = cur.fetchone()[0]
            cur.execute("SELECT account_id, nickname, expires_at FROM ml_tokens")
            tokens = cur.fetchall()
    return {
        "orders_with_ml_order_id": with_ml,
        "total_orders": total,
        "ml_shipments": shipments,
        "accounts": [{"account_id": r[0], "nickname": r[1], "expires_at": r[2].isoformat()} for r in tokens]
    }

# ===============================
# FRONT
# ===============================
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def home():
    return _render_index_html()