import os, json, boto3
from decimal import Decimal
from botocore.exceptions import ClientError

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

def _normalize_id(tid):
    s = str(tid).strip().strip('"').strip("'")
    n = None
    try:
        n = int(float(s))
    except Exception:
        pass
    return s, n

def lambda_handler(event, context):
    params = (event or {}).get("queryStringParameters") or {}
    tid = params.get("IDTransaccion")
    if not tid:
        return _resp(400, {"ok": False, "msg": "Falta IDTransaccion"})

    s, n = _normalize_id(tid)

    # Intentar con PK string
    try:
        r = table.get_item(Key={"IDTransaccion": s})
        it = r.get("Item")
        if it:
            return _resp(200, {"ok": True, "data": [it]})
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("ValidationException", "ResourceNotFoundException"):
            return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

    # Intentar con PK number
    if n is not None:
        try:
            r = table.get_item(Key={"IDTransaccion": n})
            it = r.get("Item")
            if it:
                return _resp(200, {"ok": True, "data": [it]})
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code not in ("ValidationException", "ResourceNotFoundException"):
                return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

    return _resp(200, {"ok": True, "data": []})
