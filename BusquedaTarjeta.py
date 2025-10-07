import os, json, boto3
from datetime import datetime, timezone, timedelta
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

TABLE_NAME = os.environ.get("TABLA_TRANSACCION", "TablaTransaccion")
INDEX = "GSI_IDTarjeta_Fecha"
RANGE_ATTR = "FechaHoraOrden"
SEP = "#"
dynamodb = boto3.resource("dynamodb")

def _resp(code, data):
    return {"statusCode": code, "headers": {"Content-Type":"application/json","Access-Control-Allow-Origin":"*"}, "body": json.dumps(data)}

def _day_bounds(fecha): return f"{fecha}{SEP}00:00:00", f"{fecha}{SEP}23:59:59"

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    table = dynamodb.Table(TABLE_NAME)
    try:
        idv = params.get("IDTarjeta")
        if not idv: raise ValueError("Falta IDTarjeta")
        idv = int(idv)
        fecha = params.get("fecha"); desde = params.get("desde"); hasta = params.get("hasta")

        if fecha and not (desde or hasta):
            ini, fin = _day_bounds(fecha)
            cond = Key("IDTarjeta").eq(idv) & Key(RANGE_ATTR).between(ini, fin)
            q = table.query(IndexName=INDEX, KeyConditionExpression=cond, ScanIndexForward=False)
            return _resp(200, {"ok": True, "data": q.get("Items", [])})

        if desde and hasta:
            ini, _ = _day_bounds(desde); _, fin = _day_bounds(hasta)
            cond = Key("IDTarjeta").eq(idv) & Key(RANGE_ATTR).between(ini, fin)
            q = table.query(IndexName=INDEX, KeyConditionExpression=cond, ScanIndexForward=False)
            return _resp(200, {"ok": True, "data": q.get("Items", [])})

        latest = table.query(IndexName=INDEX, KeyConditionExpression=Key("IDTarjeta").eq(idv), ScanIndexForward=False, Limit=1)
        items = latest.get("Items", [])
        if not items: return _resp(200, {"ok": True, "data": []})

        ult = items[0]
        fecha_str = ult.get("Fecha") or (str(ult.get(RANGE_ATTR,"")).split("#")[0] if ult.get(RANGE_ATTR) else None)
        if not fecha_str: return _resp(200, {"ok": True, "data": items})

        dt = datetime.strptime(fecha_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start = dt.replace(day=1)
        nextm = (start.replace(year=start.year+1, month=1, day=1) if start.month==12 else start.replace(month=start.month+1, day=1))
        end = nextm - timedelta(seconds=1)
        ini = start.strftime("%Y-%m-%d#00:00:00"); fin = end.strftime("%Y-%m-%d#%H:%M:%S")

        cond = Key("IDTarjeta").eq(idv) & Key(RANGE_ATTR).between(ini, fin)
        q = table.query(IndexName=INDEX, KeyConditionExpression=cond, ScanIndexForward=False)
        return _resp(200, {"ok": True, "data": q.get("Items", [])})
    except ValueError as ve:
        return _resp(400, {"ok": False, "msg": str(ve)})
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("ResourceNotFoundException","ValidationException"):
            return _resp(200, {"ok": True, "data": []})
        return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})
