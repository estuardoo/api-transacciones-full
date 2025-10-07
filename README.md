# api-transacciones-full

Mantiene tu `serverless.yml` y handlers como ya te funcionaban. Actualiza importadores para admitir **todas las columnas nuevas** y agregados de comercios.

## Endpoints
- POST `/import/transacciones`
- POST `/import/comercios`
- GET `/transacciones/buscar-por-id?IDTransaccion=...`
- GET `/transacciones/buscar-cliente?IDCliente=...`
- GET `/transacciones/buscar-comercio?IDComercio=...`
- GET `/transacciones/buscar-tarjeta?IDTarjeta=...`

## TablaTransacciones (campos admitidos)
Obligatorios: `IDTransaccion (PK)`, `IDCliente`, `IDComercio`, `Fecha`, `Hora`  
Derivados: `FechaHoraOrden = "YYYY-MM-DD#HH:MM:SS"`, `FechaHoraISO = "YYYY-MM-DDTHH:MM:SS"`  
IDs opcionales: `IDTarjeta` (y espejo `TarjetaID`), `IDMoneda`, `IDCanal`, `IDEstado`  
Strings: `CodigoAutorizacion`, `Estado`, `Canal`, `CodigoMoneda`, `NombreComercio`, `Sector`, `Producto`, `NombreCompleto`, `DNI`, `telefono`, `email`, `Tarjeta`  
Números: `MontoBruto`, `TasaCambio`, `Monto` (Decimal), `IndicadorAprobada`, `LatenciaAutorizacionMs`, `Fraude` (int)  
Extras: `FechaCarga` (ISO o `YYYY-MM-DD HH:MM:SS`)

## TablaComercios (agregados mensuales)
PK compuesta: `Tipo (N)` + `ID (N)`  
Atributos: `Agregado`, `Grupo`, `Ene..Dic`, `Promedio`, `TotalMonto`, `TotalFraude`, `Composicion`

## Variables de entorno
- `TABLA_TRANSACCION` (default `TablaTransaccion`)
- `TABLA_COMERCIO` (detalle)
- `TABLA_COMERCIOS_AGREG` (agregados, default `TablaComercios`)

## Deploy (igual que tu flujo actual)
```bash
export AWS_REGION=us-east-1
export STAGE=dev
export TABLA_TRANSACCION=TablaTransaccion
export TABLA_COMERCIO=TablaComercio
export TABLA_COMERCIOS_AGREG=TablaComercios

# usa tu misma CLI y serverless.yml
sls deploy --region $AWS_REGION --stage $STAGE
```