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

    if SYNC_SECRET:
        incoming = request.headers.get("X-Sync-Secret")
        if incoming != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    if not BL_TOKEN:
        raise HTTPException(status_code=500, detail="BL_TOKEN not configured")

    payload = {
        "method": "getOrders",
        "parameters": json.dumps({
            "date_confirmed_from": 0,
            "get_unconfirmed_orders": True
        })
    }

    headers = {"X-BLToken": BL_TOKEN}

    r = requests.post(BL_API_URL, headers=headers, data=payload, timeout=30)
    data = r.json()

    if data.get("status") != "SUCCESS":
        raise HTTPException(status_code=500, detail=data)

    orders = data.get("orders", [])
    inserted = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            for o in orders:
                order_id = str(o.get("order_id") or o.get("id") or "")
                status = (
                    o.get("order_status")
                    or o.get("status")
                    or o.get("order_status_name")
                    or str(o.get("order_status_id") or "")
                )

                if not order_id or not status:
                    continue

                cur.execute(
                    "INSERT INTO order_events(order_id, status, event_ts) VALUES (%s, %s, NOW())",
                    (order_id, status)
                )
                inserted += 1

    return {
        "ok": True,
        "orders_received": len(orders),
        "events_inserted": inserted
    }


# ===============================
# FRONTEND
# ===============================

app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()