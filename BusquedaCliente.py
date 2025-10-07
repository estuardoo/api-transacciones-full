import os, json, boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from utils_search import query_range, query_latest

TABLE_NAME = os.environ.get("TABLA_TRANSACCION", "TablaTransaccion")
dynamodb = boto3.resource("dynamodb")

INDEX_TRIES = [
    ("GSI_Cliente_Fecha",  "ClienteID",  "FechaHoraISO",   "T"),  # legacy
    ("GSI_IDCliente_Fecha","IDCliente",  "FechaHoraOrden", "#"),  # nuevo
]

def _resp(code, data):
    return {"statusCode": code, "headers": {"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}, "body": json.dumps(data)}

def _day_bounds(fecha, sep): return f"{fecha}{sep}00:00:00", f"{fecha}{sep}23:59:59"

def _parse_params(params):
    idv = params.get("IDCliente")
    if not idv: raise ValueError("Falta IDCliente")
    fecha = params.get("fecha"); desde = params.get("desde"); hasta = params.get("hasta")
    return int(idv), fecha, desde, hasta

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    table = dynamodb.Table(TABLE_NAME)
    try:
        idv, fecha, desde, hasta = _parse_params(params)

        if fecha and not (desde or hasta):
            for index_name, hash_attr, range_attr, sep in INDEX_TRIES:
                try:
                    ini, fin = _day_bounds(fecha, sep)
                    q = query_range(table, index_name, hash_attr, idv, ini, fin, range_attr)
                    return _resp(200, {"ok": True, "data": q.get("Items", [])})
                except ClientError:
                    continue
            return _resp(200, {"ok": True, "data": []})

        if desde and hasta:
            for index_name, hash_attr, range_attr, sep in INDEX_TRIES:
                try:
                    ini, _ = _day_bounds(desde, sep); _, fin = _day_bounds(hasta, sep)
                    q = query_range(table, index_name, hash_attr, idv, ini, fin, range_attr)
                    return _resp(200, {"ok": True, "data": q.get("Items", [])})
                except ClientError:
                    continue
            return _resp(200, {"ok": True, "data": []})

        latest_item = None; latest_sep = "#"; latest_hash = "IDCliente"
        for index_name, hash_attr, range_attr, sep in INDEX_TRIES:
            try:
                latest = query_latest(table, index_name, hash_attr, idv)
                items = latest.get("Items", [])
                if items:
                    latest_item = items[0]; latest_sep = sep; latest_hash = hash_attr
                    break
            except ClientError:
                continue
        if not latest_item: return _resp(200, {"ok": True, "data": []})

        fecha_str = latest_item.get("Fecha")
        if not fecha_str:
            for candidate in ["FechaHoraOrden","FechaHoraISO"]:
                if latest_item.get(candidate):
                    fecha_str = str(latest_item[candidate]).split("#")[0].split("T")[0]
                    break
        if not fecha_str: return _resp(200, {"ok": True, "data": [latest_item]})

        dt = datetime.strptime(fecha_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start = dt.replace(day=1)
        nextm = (start.replace(year=start.year+1, month=1, day=1) if start.month==12 else start.replace(month=start.month+1, day=1))
        end = nextm - timedelta(seconds=1)
        ini = start.strftime(f"%Y-%m-%d{latest_sep}00:00:00"); fin = end.strftime(f"%Y-%m-%d{latest_sep}%H:%M:%S")

        idx = next((t for t in INDEX_TRIES if t[1]==latest_hash and t[3]==latest_sep), INDEX_TRIES[0])
        q = query_range(table, idx[0], idx[1], idv, ini, fin, idx[2])
        return _resp(200, {"ok": True, "data": q.get("Items", [])})
    except ValueError as ve:
        return _resp(400, {"ok": False, "msg": str(ve)})
    except ClientError as e:
        return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})
