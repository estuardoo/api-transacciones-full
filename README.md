# api-transacciones-full

Servicio Serverless para importar y consultar transacciones/comercios en DynamoDB.

## Endpoints

- **POST `/import/transacciones`**
- **POST `/import/comercios`**
- **GET `/busqueda/cliente`**
- **GET `/busqueda/comercio`**
- **GET `/busqueda/tarjeta`**
- **GET `/busqueda/transaccion`**

## TablaTransacciones (nuevo esquema)
Incluye: IDTransaccion, IDCliente/ClienteID, IDComercio/ComercioID, Fecha, Hora, FechaHoraOrden, FechaHoraISO,
IDTarjeta/TarjetaID, IDMoneda, IDCanal, IDEstado, CodigoAutorizacion, Estado, Canal, CodigoMoneda,
MontoBruto, TasaCambio, Monto, IndicadorAprobada, LatenciaAutorizacionMs, Fraude, FechaCarga,
NombreComercio, Sector, Producto, NombreCompleto, DNI, telefono, email, Tarjeta.

## TablaComercios (agregados)
PK compuesta Tipo (N) + ID (N). Atributos: Agregado, Grupo, Ene..Dic, Promedio, TotalMonto, TotalFraude, Composicion.

## Despliegue rápido
```bash
export AWS_REGION=us-east-1
export STAGE=dev
export TABLA_TRANSACCION=TablaTransacciones
export TABLA_COMERCIO=TablaComercio
export TABLA_COMERCIOS_AGREG=TablaComercios

sls deploy --region $AWS_REGION --stage $STAGE
```