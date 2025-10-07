import os, json, boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from utils_search import query_range, query_latest

TABLE_NAME = os.environ.get("TABLA_TRANSACCION", "TablaTransacciones")
dynamodb = boto3.resource("dynamodb")

INDEX_TRIES = [
    ("GSI_Cliente_Fecha",  "ClienteID",  "FechaHoraISO",   "T"),  # legacy
    ("GSI_IDCliente_Fecha","IDCliente",  "FechaHoraOrden", "#"),  # nuevo
]

def _resp(code, data):
    return {
        "statusCode": code,
        "headers": {"Content-Type":"application/json","Access-Control-Allow-Origin":"*"},
        "body": json.dumps(data, default=str)
    }

def _month_bounds(dt):
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    nm = start.replace(year=start.year+1, month=1) if start.month == 12 else start.replace(month=start.month+1)
    end = nm - timedelta(seconds=1)
    return start, end

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    key = params.get("IDCliente")
    if key is None or str(key).strip() == "":
        return _resp(400, {"ok": False, "msg": "Falta IDCliente"})
    key = int(str(key).strip()) if str(key).strip().isdigit() else str(key).strip()

    fecha = params.get("fecha")  # YYYY-MM
    desde = params.get("desde")  # YYYY-MM-DD
    hasta = params.get("hasta")  # YYYY-MM-DD

    table = dynamodb.Table(TABLE_NAME)

    ini = fin = None
    if not fecha and not (desde and hasta):
        for idx, hattr, rattr, sep in INDEX_TRIES:
            try:
                r = query_latest(table, idx, hattr, key)
                items = r.get("Items", [])
                if items:
                    last = items[0]
                    if rattr == "FechaHoraISO":
                        iso = (last.get(rattr) or "")[:19]
                        dt = datetime.strptime(iso, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                    else:
                        val = last.get(rattr) or ""
                        f, h = val.split(sep)
                        dt = datetime.strptime(f + " " + h, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    start, end = _month_bounds(dt)
                    ini = start.strftime("%Y-%m-%d") + ("T00:00:00" if sep=="T" else "#00:00:00")
                    fin = end.strftime("%Y-%m-%d") + ("T23:59:59" if sep=="T" else "#23:59:59")
                    break
            except ClientError:
                continue
            except Exception:
                continue
    else:
        if fecha:
            try:
                dt = datetime.strptime(fecha + "-01", "%Y-%m-%d").replace(tzinfo=timezone.utc)
                start, end = _month_bounds(dt)
                ini = start.strftime("%Y-%m-%d")
                fin = end.strftime("%Y-%m-%d")
            except Exception:
                return _resp(400, {"ok": False, "msg": "Formato 'fecha' inválido. Use YYYY-MM"})
        else:
            ini = desde
            fin = hasta

    out = []
    for idx, hattr, rattr, sep in INDEX_TRIES:
        try:
            if ini and fin:
                if rattr == "FechaHoraISO":
                    ini_v = ini if "T" in ini else f"{ini}T00:00:00"
                    fin_v = fin if "T" in fin else f"{fin}T23:59:59"
                else:
                    ini_v = ini if "#" in ini else f"{ini}#00:00:00"
                    fin_v = fin if "#" in fin else f"{fin}#23:59:59"
                r = query_range(table, idx, hattr, key, ini_v, fin_v, rattr)
            else:
                continue
            out.extend(r.get("Items", []))
        except ClientError:
            continue

    return _resp(200, {"ok": True, "count": len(out), "data": out, "table": TABLE_NAME})
