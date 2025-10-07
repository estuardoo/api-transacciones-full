import json
import os
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

TABLE_DET = os.environ.get("TABLA_COMERCIO", "TablaComercio")
TABLE_AGR = os.environ.get("TABLA_COMERCIOS_AGREG", "TablaComercios")

ddb = boto3.resource('dynamodb')
t_det = ddb.Table(TABLE_DET)
t_agr = ddb.Table(TABLE_AGR)

def _resp(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"}, "body": json.dumps(body, default=str)}

def _to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

def _to_dec(x):
    if x in (None, "", "NULL", "null"):
        return None
    try:
        s = str(x).strip().replace(" ", "").replace(",", "")
        return Decimal(s)
    except Exception:
        try:
            return Decimal(str(float(x)))
        except Exception:
            return None

def lambda_handler(event, context):
    try:
        body = event.get("body")
        if not body: return _resp(400, {"ok": False, "msg": "Body vacío"})
        payload = json.loads(body)
    except Exception as e:
        return _resp(400, {"ok": False, "msg": f"JSON inválido: {e}"})

    # admitir lista directa o {"data": [...]}
    items = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = payload.get("data", [])
        if isinstance(items, dict):
            items = [items]
    else:
        return _resp(400, {"ok": False, "msg": "JSON debe ser lista o {'data': [...]}"})

    ins_det = ins_agr = 0

    try:
        with t_det.batch_writer(overwrite_by_pkeys=["IDComercio"]) as bw_det, \
             t_agr.batch_writer(overwrite_by_pkeys=["Tipo","ID"]) as bw_agr:

            for it in items:
                if not isinstance(it, dict):
                    continue
                it = dict(it)

                # Detección de agregados mensuales (TablaComercios)
                if all(k in it for k in ("Tipo","ID","Agregado","Grupo")):
                    row = {
                        "Tipo": _to_int(it.get("Tipo")) or 0,
                        "ID":   _to_int(it.get("ID")) or 0,
                        "Agregado": str(it.get("Agregado")).strip(),
                        "Grupo":    str(it.get("Grupo")).strip(),
                    }
                    for c in ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic",
                              "Promedio","TotalMonto","TotalFraude","Composicion"]:
                        dv = _to_dec(it.get(c))
                        if dv is not None: row[c] = dv
                    bw_agr.put_item(Item=row); ins_agr += 1
                    continue

                # Detalle de comercios (TablaComercio)
                idc = it.get("IDComercio") or it.get("ComercioID")
                if idc is None:
                    continue
                try:
                    idc_int = int(str(idc).strip())
                except Exception:
                    idc_int = idc  # dejar como string si no es convertible

                row = {k: v for k, v in it.items() if v is not None and v != ""}
                row["IDComercio"] = idc_int
                if "ComercioID" not in row:
                    row["ComercioID"] = idc_int
                bw_det.put_item(Item=row); ins_det += 1

    except ClientError as e:
        return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

    return _resp(200, {"ok": True, "insertados_detalle": ins_det, "insertados_agregados": ins_agr,
                       "tabla_detalle": TABLE_DET, "tabla_agregados": TABLE_AGR})