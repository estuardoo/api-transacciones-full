import json
import os
from datetime import datetime, timezone
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError

TABLE_NAME = os.environ.get("TABLA_TRANSACCION", "TablaTransaccion")
ddb = boto3.resource('dynamodb')
table = ddb.Table(TABLE_NAME)

def _resp(status, body):
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(body, default=str)
    }

def _to_int_or_none(x):
    try:
        if x in (None, "", "NULL", "null"):
            return None
        return int(str(x).strip())
    except Exception:
        return None

def _to_dec_or_none(x):
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

def _parse_dt(fecha:str, hora:str):
    if not fecha or not hora:
        raise ValueError("Fecha y Hora son obligatorias")
    try:
        dt = datetime.fromisoformat(f"{fecha}T{hora}")
    except ValueError:
        try:
            dt = datetime.strptime(f"{fecha} {hora}", "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError(f"Fecha/Hora inválidas: {fecha} {hora}")
    return dt.replace(tzinfo=timezone.utc)

def _fmt_hash(fecha, hora): return _parse_dt(fecha, hora).strftime("%Y-%m-%d#%H:%M:%S")
def _fmt_iso (fecha, hora): return _parse_dt(fecha, hora).strftime("%Y-%m-%dT%H:%M:%S")

STRING_FIELDS = [
    "CodigoAutorizacion","Estado","Canal","CodigoMoneda",
    "NombreComercio","Sector","Producto",
    "NombreCompleto","DNI","telefono","email","Tarjeta"
]
DEC_FIELDS = ["MontoBruto","TasaCambio","Monto"]
INT_FIELDS = ["IndicadorAprobada","LatenciaAutorizacionMs","Fraude"]

def lambda_handler(event, context):
    try:
        body = event.get("body")
        if not body:
            return _resp(400, {"ok": False, "msg": "Body vacío"})
        items = json.loads(body)
        if isinstance(items, dict):
            items = items.get("data", [items])
        if not isinstance(items, list):
            return _resp(400, {"ok": False, "msg": "Se espera una lista o {'data': [...]}"})
    except Exception as e:
        return _resp(400, {"ok": False, "msg": f"JSON inválido: {e}"})

    inserted = 0
    try:
        with table.batch_writer(overwrite_by_pkeys=["IDTransaccion"]) as bw:
            for it in items:
                if not isinstance(it, dict):
                    continue
                it = dict(it)

                # Normalización nombres nuevos y legacy
                it.setdefault("IDTransaccion", it.get("TransaccionID"))
                it.setdefault("IDCliente", it.get("ClienteID"))
                it.setdefault("IDComercio", it.get("ComercioID"))
                it.setdefault("IDTarjeta", it.get("TarjetaID"))

                # Requeridos mínimos
                required = ("IDTransaccion", "IDCliente", "IDComercio", "Fecha", "Hora")
                if not all(k in it and it[k] not in (None, "") for k in required):
                    # omitimos fila incompleta
                    continue

                clean = {}

                # PK
                clean["IDTransaccion"] = str(it["IDTransaccion"])

                # IDs (enteros) + espejos legacy
                clean["IDCliente"]  = _to_int_or_none(it.get("IDCliente"))
                clean["IDComercio"] = _to_int_or_none(it.get("IDComercio"))
                if clean["IDCliente"] is not None:
                    clean["ClienteID"] = clean["IDCliente"]
                if clean["IDComercio"] is not None:
                    clean["ComercioID"] = clean["IDComercio"]

                idt = _to_int_or_none(it.get("IDTarjeta"))
                if idt is not None:
                    clean["IDTarjeta"] = idt
                    clean["TarjetaID"] = idt

                # IDs opcionales
                for k in ("IDMoneda","IDCanal","IDEstado"):
                    v = _to_int_or_none(it.get(k))
                    if v is not None:
                        clean[k] = v

                # Strings descriptivos
                for k in STRING_FIELDS:
                    v = it.get(k)
                    if v not in (None, ""):
                        clean[k] = str(v).strip()

                # Montos y números
                for k in DEC_FIELDS:
                    dv = _to_dec_or_none(it.get(k))
                    if dv is not None:
                        clean[k] = dv
                for k in INT_FIELDS:
                    iv = _to_int_or_none(it.get(k))
                    if iv is not None:
                        clean[k] = iv

                # Fecha/Hora y derivados
                clean["Fecha"] = str(it["Fecha"])
                clean["Hora"]  = str(it["Hora"])
                clean["FechaHoraOrden"] = _fmt_hash(clean["Fecha"], clean["Hora"])
                clean["FechaHoraISO"]   = _fmt_iso (clean["Fecha"], clean["Hora"])

                # FechaCarga opcional
                fc = it.get("FechaCarga")
                if fc:
                    try:
                        s = str(fc)
                        if " " in s and "T" not in s:
                            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                            clean["FechaCarga"] = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                        else:
                            clean["FechaCarga"] = s
                    except Exception:
                        clean["FechaCarga"] = str(fc)

                # Escribir
                bw.put_item(Item={k: v for k, v in clean.items() if v is not None})
                inserted += 1
    except ClientError as e:
        return _resp(500, {"ok": False, "msg": e.response["Error"]["Message"]})

    return _resp(200, {"ok": True, "insertados": inserted, "tabla": TABLE_NAME})