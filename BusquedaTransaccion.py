import os, json, boto3
from botocore.exceptions import ClientError

TABLE_NAME = os.environ.get("TABLA_TRANSACCION", "TablaTransacciones")
dynamodb = boto3.resource("dynamodb")

def _resp(code, data):
    return {
        "statusCode": code,
        "headers": {"Content-Type":"application/json","Access-Control-Allow-Origin":"*"},
        "body": json.dumps(data, default=str)
    }

def lambda_handler(event, context):
    params = event.get("queryStringParameters") or {}
    tid = params.get("IDTransaccion")
    if not tid:
        return _resp(400, {"ok": False, "msg": "Falta IDTransaccion"})
    tid = str(tid).strip()

    table = dynamodb.Table(TABLE_NAME)
    try:
        r = table.get_item(Key={"IDTransaccion": tid})
        if "Item" not in r:
            return _resp(404, {"ok": False, "msg": "Transacción no encontrada", "id": tid, "table": TABLE_NAME})
        return _resp(200, {"ok": True, "data": r["Item"]})
    except ClientError as e:
        return _resp(500, {"ok": False, "msg": e.response.get("Error", {}).get("Message", str(e)), "table": TABLE_NAME})
