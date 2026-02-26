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


def bl_call(method: str, params: dict):
    if not BL_TOKEN:
        raise RuntimeError("BL_TOKEN no configurado")

    r = requests.post(
        BL_API_URL,
        headers={"X-BLToken": BL_TOKEN},
        data={"method": method, "parameters": json.dumps(params)},
        timeout=10,  # clave para evitar 502
    )
    r.raise_for_status()
    out = r.json()
    if out.get("status") != "SUCCESS":
        raise RuntimeError(str(out))
    return out


@app.on_event("startup")
def ensure_schema():
    # Creamos orders_current si no existe (sin SQL manual tuyo)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS orders_current (
              order_id TEXT PRIMARY KEY,
              status TEXT NOT NULL,
              updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_order_id ON order_events(order_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_event_ts ON order_events(event_ts);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_order_events_status ON order_events(status);")


@app.post("/sync")
def sync(request: Request):
    # Seguridad
    if SYNC_SECRET:
        incoming = request.headers.get("X-Sync-Secret")
        if incoming != SYNC_SECRET:
            raise HTTPException(status_code=401, detail="Unauthorized")

    # 1) Mapa status_id -> nombre
    status_list = bl_call("getOrderStatusList", {}).get("statuses", [])
    status_map = {int(s["id"]): s.get("name", str(s["id"])) for s in status_list