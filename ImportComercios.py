import json
import os
import boto3
from decimal import Decimal

ddb = boto3.resource('dynamodb')

def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body, default=str)
    }

def _to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None

def _to_float(x):
    try:
        s = str(x).replace(" ", "").replace(",", "")
        return float(s)
    except Exception:
        return None

def _as_decimal_safe(v):
    if v is None:
        return None
    return Decimal(str(v))

def lambda_handler(event, context):
    table_detalle_name = os.environ.get("TABLA_COMERCIO", "TablaComercio")
    table_agreg_name = os.environ.get("TABLA_COMERCIOS_AGREG", "TablaComercios")

    table_det = ddb.Table(table_detalle_name)
    table_ag = ddb.Table(table_agreg_name)

    try:
        body = event.get("body", "")
        payload = json.loads(body) if body else {}
    except Exception as e:
        return _resp(400, {"ok": False, "error": f"JSON inválido: {e}"})

    items = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = payload.get("data", [])
        if isinstance(items, dict):
            items = [items]
    else:
        return _resp(400, {"ok": False, "error": "JSON debe ser lista o {'data': [...]}"})

    ins_det, ins_agr = 0, 0

    # Escritura de agregados (Tipo+ID)
    with table_ag.batch_writer(overwrite_by_pkeys=["Tipo","ID"]) as bw_ag, \
         table_det.batch_writer(overwrite_by_pkeys=["IDComercio"]) as bw_det:

        for it in items:
            if not isinstance(it, dict):
                continue

            # Detección de fila de agregados mensuales
            if all(k in it for k in ["Tipo","ID","Agregado","Grupo"]):
                row = {
                    "Tipo": _to_int(it.get("Tipo")) or 0,
                    "ID":   _to_int(it.get("ID")) or 0,
                    "Agregado": str(it.get("Agregado")).strip(),
                    "Grupo":    str(it.get("Grupo")).strip(),
                }
                for c in ["Ene","Feb","Mar","Abr","May","Jun","Jul","Ago","Sep","Oct","Nov","Dic",
                          "Promedio","TotalMonto","TotalFraude","Composicion"]:
                    v = it.get(c)
                    fv = _to_float(v) if v is not None else None
                    if fv is not None:
                        row[c] = _as_decimal_safe(fv)
                bw_ag.put_item(Item=row)
                ins_agr += 1
                continue

            # Fila de detalle de comercio (mantener compatibilidad)
            idc = it.get("IDComercio") or it.get("ComercioID")
            if idc is not None:
                try:
                    idc = int(str(idc).strip())
                except Exception:
                    pass
                row = dict(it)
                row["IDComercio"] = idc
                # normalizaciones suaves
                if "ComercioID" not in row:
                    row["ComercioID"] = idc
                # guardar tal cual (los tipos se preservan con Decimal en números si ya vienen como float)
                # si quieres normalizar más campos, agrégalo aquí
                bw_det.put_item(Item=row)
                ins_det += 1

    return _resp(200, {"ok": True, "inserted_detalle": ins_det, "inserted_agregados": ins_agr,
                       "tabla_detalle": table_detalle_name, "tabla_agregados": table_agreg_name})