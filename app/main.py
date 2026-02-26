import os
import json
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import psycopg

app = FastAPI()

DATABASE_URL = os.environ.get("DATABASE_URL")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")  # opcional
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "10"))

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

@app.post("/webhook/base")
async def webhook_base(request: Request):
    # Opcional: validar un secret si lo usás
    if WEBHOOK_SECRET:
        incoming = request.headers.get("x-webhook-secret") or request.headers.get("X-Webhook-Secret")
        if incoming != WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    payload = await request.json()

    # Parse tolerante: ajustable si Base manda otro formato
    order_id = None
    status = None

    if isinstance(payload, dict):
        order_id = payload.get("order_id") or payload.get("id") or (payload.get("order") or {}).get("id")
        status = payload.get("status") or (payload.get("order") or {}).get("status") or payload.get("new_status")

    if not order_id or not status:
        raise HTTPException(status_code=400, detail="Payload sin order_id/status. Ajustar parseo al formato real de Base.")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO order_events (order_id, status, event_ts) VALUES (%s, %s, NOW())",
                (str(order_id), str(status))
            )

    return {"ok": True}

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

# Front
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/", response_class=HTMLResponse)
def home():
    with open("app/static/index.html", "r", encoding="utf-8") as f:
        return f.read()