import json
import os
from datetime import datetime, timezone
import boto3
from decimal import Decimal

ddb = boto3.resource('dynamodb')

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

def _to_float_or_none(x):
    try:
        if x in (None, "", "NULL", "null"):
            return None
        s = str(x).replace(" ", "").replace(",", "")
        return float(s)
    except Exception:
        return None

def _as_decimal_safe(v):
    if v is None:
        return None
    return Decimal(str(v))

def _build_fecha_fields(fecha, hora):
    if not fecha:
        return None, None
    hora = hora or "00:00:00"
    try:
        dt = datetime.strptime(f"{fecha} {hora}", "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return f"{fecha}#{hora}", dt.strftime("%Y-%m-%dT%H:%M:%S%z") or dt.isoformat()
    except Exception:
        return f"{fecha}#{hora}", f"{fecha}T{hora}"

def lambda_handler(event, context):
    table_name = os.environ.get("TABLA_TRANSACCION", "TablaTransacciones")
    table = ddb.Table(table_name)

    try:
        body = event.get("body", "")
        items = json.loads(body) if body else []
        if isinstance(items, dict):
            items = items.get("data", [items])
        if not isinstance(items, list):
            return _resp(400, {"ok": False, "error": "JSON debe ser lista o {'data': [...]}"})
    except Exception as e:
        return _resp(400, {"ok": False, "error": f"JSON inválido: {e}"})

    inserted = 0
    with table.batch_writer(overwrite_by_pkeys=["IDTransaccion"]) as bw:
        for it in items:
            if not isinstance(it, dict):
                continue

            clean = {}

            tid = it.get("IDTransaccion") or it.get("TransaccionID") or it.get("id") or it.get("Id")
            if tid is None:
                tid = f"{it.get('IDCliente','')}-{it.get('IDComercio','')}-{it.get('Fecha','')}-{it.get('Hora','')}"
            clean["IDTransaccion"] = str(tid)

            for src, dst in [
                ("IDCliente","IDCliente"), ("ClienteID","ClienteID"),
                ("IDComercio","IDComercio"), ("ComercioID","ComercioID"),
                ("IDTarjeta","IDTarjeta"), ("TarjetaID","TarjetaID"),
                ("IDMoneda","IDMoneda"), ("IDCanal","IDCanal"), ("IDEstado","IDEstado")
            ]:
                v = _to_int_or_none(it.get(src))
                if v is not None:
                    clean[dst] = v

            if "IDCliente" in clean and "ClienteID" not in clean:
                clean["ClienteID"] = clean["IDCliente"]
            if "IDComercio" in clean and "ComercioID" not in clean:
                clean["ComercioID"] = clean["IDComercio"]
            if "IDTarjeta" in clean and "TarjetaID" not in clean:
                clean["TarjetaID"] = clean["IDTarjeta"]

            fecha = it.get("Fecha")
            hora = it.get("Hora")
            fh_orden, fh_iso = _build_fecha_fields(fecha, hora)
            if fh_orden: clean["FechaHoraOrden"] = fh_orden
            if fh_iso:   clean["FechaHoraISO"] = fh_iso
            if fecha:    clean["Fecha"] = str(fecha)
            if hora:     clean["Hora"] = str(hora)

            for k in ["CodigoAutorizacion","Estado","Canal","CodigoMoneda",
                      "NombreComercio","Sector","Producto",
                      "NombreCompleto","DNI","telefono","email","Tarjeta"]:
                v = it.get(k)
                if v not in (None, ""):
                    clean[k] = str(v).strip()

            for k in ["MontoBruto","TasaCambio","Monto"]:
                fv = _to_float_or_none(it.get(k))
                if fv is not None:
                    clean[k] = _as_decimal_safe(fv)

            for k in ["IndicadorAprobada","LatenciaAutorizacionMs","Fraude"]:
                iv = _to_int_or_none(it.get(k))
                if iv is not None:
                    clean[k] = iv

            fc = it.get("FechaCarga")
            if fc:
                try:
                    if " " in str(fc) and "T" not in str(fc):
                        dt = datetime.strptime(str(fc), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        clean["FechaCarga"] = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                    else:
                        clean["FechaCarga"] = str(fc)
                except Exception:
                    clean["FechaCarga"] = str(fc)

            bw.put_item(Item=clean)
            inserted += 1

    return _resp(200, {"ok": True, "inserted": inserted, "table": table_name})