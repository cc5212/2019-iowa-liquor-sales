

# 2019-iowa-liquor-sales

Analysis of Iowa liquor sales database with big data tools, like Pig Latin and Apache Spark. [Jean Cherubini, Aníbal Fuentes, Juan Sáez, Claudio Urbina. Group 6]

## Objetivos

El objetivo del presente proyecto es el análisis de datos de ventas de alcohol en el estado de Iowa, EEUU; a través del uso de herramientas de análisis de big data, como Pig Latin y Apache Spark, además de herramientas de visualización de datos como Tableau. Se plantea además como hipótesis que el análisis realizado en el proyecto es una muestra del comportamiento general del consumo de alcohol en estados unidos.

Dentro de las preguntas que se quieren responder se encuentran las siguientes:

* ¿Cuál es el consumo mensual de alcohol en Iowa?, esto tanto en litros, como en dinero gastado en alcohol, número de botellas compradas y cantidad de transacciones.
* ¿Cuáles son los tipos de alcohol más vendidos? ¿Cuáles son los productores de alcohol con mayor cantidad de ventas?
* ¿Cuál es el condado y la ciudad más alcoholica cada año? Esto en litros consumidos per capita de forma anual.
* ¿Es posible visualizar como se comporta el consumo de alcohol por persona en los diferentes estados de Iowa de manera mensual?

Para responder las dos últimas preguntas se utiliza además un dataset que nos indica la población de Iowa por condado y otro dataset que nos indica la población de Iowa por ciudad, esto de manera anual.

## Dataset

### Attributes of the dataset

| Column Name           | Type    | Description               |
| --------------------- | ------- | ------------------------- |
| Invoice/Item Number   | object  | A unique ID of the liquor |
| Date                  | object  |                           |
| Store Number          | int64   |                           |
| Store Name            | object  |                           |
| Address               | object  |                           |
| City                  | object  |                           |
| Zip Code              | object  |                           |
| Store Location        | object  |                           |
| County Number         | float64 |                           |
| County                | object  |                           |
| Category              | float64 |                           |
| Category Name         | object  |                           |
| Vendor Number         | int64   |                           |
| Vendor Name           | object  |                           |
| Item Number           | int64   |                           |
| Item Description      | object  |                           |
| Pack                  | int64   |                           |
| Bottle Volume (ml)    | int64   |                           |
| State Bottle Cost     | object  |                           |
| State Bottle Retail   | object  |                           |
| Bottles Sold          | int64   |                           |
| Sale (Dollars)        | object  |                           |
| Volume Sold (Liters)  | float64 |                           |
| Volume Sold (Gallons) | float64 |                           |
