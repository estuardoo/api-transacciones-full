# api-transacciones-full

Librería/servicio Serverless para importar y consultar transacciones y comercios en DynamoDB.

## Endpoints

- **POST `/import/transacciones`**  
  Importa una lista de transacciones con el nuevo esquema.

- **POST `/import/comercios`**  
  Importa comercios de detalle (tabla `TablaComercio`) y/o filas de agregados mensuales (tabla `TablaComercios`) en el mismo payload.

- **GET `/busqueda/cliente`**  
- **GET `/busqueda/comercio`**  
- **GET `/busqueda/tarjeta`**  
- **GET `/busqueda/transaccion`**  
  Búsquedas con soporte de GSIs y fallback integrado.

## Esquema: TablaTransacciones

Campos soportados (además de los ya existentes):

```
IDTransaccion (PK string)
IDCliente, ClienteID (numbers, mirrors)
IDComercio, ComercioID (numbers, mirrors)
Fecha (YYYY-MM-DD), Hora (HH:MM:SS)
FechaHoraOrden (YYYY-MM-DD#HH:MM:SS)  # para GSIs nuevos
FechaHoraISO (YYYY-MM-DDTHH:MM:SS)    # compatibilidad
IDTarjeta, IDMoneda, IDCanal, IDEstado (number)
CodigoAutorizacion, Estado, Canal, CodigoMoneda (string)
MontoBruto, TasaCambio, Monto (number)
IndicadorAprobada, LatenciaAutorizacionMs, Fraude (number)
FechaCarga (ISO string)
NombreComercio, Sector, Producto (string)
NombreCompleto, DNI, telefono, email, Tarjeta (string)
```

## Esquema: TablaComercios (agregados)

PK compuesta `Tipo` (N) + `ID` (N). Atributos:
`Agregado`, `Grupo`, `Ene..Dic`, `Promedio`, `TotalMonto`, `TotalFraude`, `Composicion`.

## Despliegue

1. Renombra el repo a **api-transacciones-full**.
2. Exporta variables si deseas nombres custom:
   ```bash
   export AWS_REGION=us-east-1
   export STAGE=dev
   export TABLA_TRANSACCION=TablaTransacciones
   export TABLA_COMERCIO=TablaComercio
   export TABLA_COMERCIOS_AGREG=TablaComercios
   ```
3. Deploy:
   ```bash
   npx serverless deploy
   ```

## Payloads de ejemplo

### /import/transacciones
```json
[{
  "IDTransaccion":"1","IDCliente":52,"IDComercio":234,"Fecha":"2025-01-01","Hora":"00:00:00",
  "IDTarjeta":84,"IDMoneda":0,"IDCanal":0,"IDEstado":0,"CodigoAutorizacion":"31369578",
  "Estado":"Rechazada","Canal":"ATM","CodigoMoneda":"PEN","MontoBruto":1.90,"TasaCambio":null,"Monto":1.90,
  "IndicadorAprobada":0,"LatenciaAutorizacionMs":772,"Fraude":0,"FechaCarga":"2025-10-07 00:05:08",
  "NombreComercio":"Hospedaje Integral San Borja Group","Sector":"Turismo","Producto":"Tour de ciudad",
  "NombreCompleto":"Natalia Gómez","DNI":"78976774","telefono":"51984091249","email":"natalia.gomez@utec.edu.pe",
  "Tarjeta":"**** 0762"
}]
```

### /import/comercios
```json
{
  "data": [
    {
      "Tipo":1,"ID":1,"Agregado":"Sector","Grupo":"Agroindustria",
      "Ene":5.02,"Feb":5.95,"Mar":4.97,"Abr":5.66,"May":5.18,"Jun":5.14,"Jul":5.74,"Ago":4.05,"Sep":3.77,"Oct":0,"Nov":0,"Dic":0,
      "Promedio":5.05,"TotalMonto":2462502.70,"TotalFraude":94352.44,"Composicion":9.54
    }
  ]
}
```