import os, json, boto3
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

TABLE_NAME = os.environ.get("TABLA_TRANSACCION", "TablaTransaccion")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def _to_jsonable(o):
    if isinstance(o, Decimal):
        return int(o) if (o % 1 == 0) else float(o)
    if isinstance(o, list):
        return [_to_jsonable(x) for x in o]
    if isinstance(o, dict):
        return {k: _to_jsonable(v) for k, v in o.items()}
    return o

def _resp(code, data):
    return {
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(_to_jsonable(data), ensure_ascii=False),
    }

def _to_int(val, name="id"):
    s = str(val).strip().strip('"').strip("'")
    try:
        return int(float(s))
    except Exception:
        raise ValueError(f"{name} inválido: {val!r}")

def _day_bounds(fecha, sep):
    return f"{fecha}{sep}00:00:00", f"{fecha}{sep}23:59:59"

def _month_bounds(yyyy_mm_dd, sep):
    dt = datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start = dt.replace(day=1)
    nextm = (start.replace(year=start.year+1, month=1, day=1) if start.month==12 else start.replace(month=start.month+1, day=1))
    end = nextm - timedelta(seconds=1)
    return start.strftime(f"%Y-%m-%d{sep}00:00:00"), end.strftime(f"%Y-%m-%d{sep}%H:%M:%S")

def _query_range(index_name, hash_attr, hash_val, range_attr, ini, fin):
    return table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(hash_attr).eq(hash_val) & Key(range_attr).between(ini, fin),
        ScanIndexForward=False
    )

def _query_latest(index_name, hash_attr, hash_val):
    return table.query(
        IndexName=index_name,
        KeyConditionExpression=Key(hash_attr).eq(hash_val),
        ScanIndexForward=False,
        Limit=1
    )

def _scan_range_fallback(hash_attr, hash_val, range_attr, ini, fin, limit=1000):
    flt = Attr(hash_attr).eq(hash_val) & Attr(range_attr).between(ini, fin)
    return table.scan(FilterExpression=flt, Limit=limit)

INDEX_TRIES = [
    ("GSI_Tarjeta_Fecha",   "TarjetaID", "FechaHoraISO",   "T"),  # legacy
    ("GSI_IDTarjeta_Fecha", "IDTarjeta", "FechaHoraOrden", "#"),  # nuevo
]

def lambda_handler(event, context):
    try:
        params = (event or {}).get("queryStringParameters") or {}
        idv = _to_int(params.get("IDTarjeta"), "IDTarjeta")
        fecha = params.get("fecha"); desde = params.get("desde"); hasta = params.get("hasta")

        if fecha and not (desde or hasta):
            for idx, h, r, sep in INDEX_TRIES:
                try:
                    ini, fin = _day_bounds(fecha, sep)
                    q = _query_range(idx, h, idv, r, ini, fin)
                    return _resp(200, {"ok": True, "data": q.get("Items", [])})
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code")
                    if code in ("ResourceNotFoundException","ValidationException"):
                        scan = _scan_range_fallback(h, idv, r, ini, fin)
                        return _resp(200, {"ok": True, "data": scan.get("Items", [])})
                    return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

        if desde and hasta:
            for idx, h, r, sep in INDEX_TRIES:
                try:
                    ini, _ = _day_bounds(desde, sep); _, fin = _day_bounds(hasta, sep)
                    q = _query_range(idx, h, idv, r, ini, fin)
                    return _resp(200, {"ok": True, "data": q.get("Items", [])})
                except ClientError as e:
                    code = e.response.get("Error", {}).get("Code")
                    if code in ("ResourceNotFoundException","ValidationException"):
                        scan = _scan_range_fallback(h, idv, r, ini, fin)
                        return _resp(200, {"ok": True, "data": scan.get("Items", [])})
                    return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

        latest_item = None; chosen = None
        for idx, h, r, sep in INDEX_TRIES:
            try:
                latest = _query_latest(idx, h, idv)
                items = latest.get("Items", [])
                if items:
                    latest_item = items[0]; chosen = (idx, h, r, sep)
                    break
            except ClientError:
                continue

        if not latest_item:
            return _resp(200, {"ok": True, "data": []})

        fecha_str = latest_item.get("Fecha")
        if not fecha_str:
            for cand in ["FechaHoraOrden", "FechaHoraISO"]:
                if latest_item.get(cand):
                    fecha_str = str(latest_item[cand]).split("#")[0].split("T")[0]
                    break
        if not fecha_str:
            return _resp(200, {"ok": True, "data": [latest_item]})

        ini, fin = _month_bounds(fecha_str, chosen[3])
        try:
            q = _query_range(chosen[0], chosen[1], idv, chosen[2], ini, fin)
            return _resp(200, {"ok": True, "data": q.get("Items", [])})
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("ResourceNotFoundException","ValidationException"):
                scan = _scan_range_fallback(chosen[1], idv, chosen[2], ini, fin)
                return _resp(200, {"ok": True, "data": scan.get("Items", [])})
            return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

    except ValueError as ve:
        return _resp(400, {"ok": False, "msg": str(ve)})
