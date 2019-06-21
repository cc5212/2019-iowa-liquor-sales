

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
### Iowa Liquor Sells 
Este set de datos contiene informacion del nombre, tipo, precio, cantidad y ubicacion de ventas de contenedores individuales o paquetes de bebidas alcoholicas. En el estado de Iowa, Estados Unidos.
Esto se debe a que todas las ventas de bebidas alcoholizas deben ser registradas apropiadamente en el sistema del Departamento de Comercio de Iowa. Esta data está catalogada como open data por el mismo Departamento, como se establece en https://data.iowa.gov/Sales-Distribution/Iowa-Liquor-Sales/m3tr-qhgy.
#### Attributes of the dataset

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

## Métodos

// Detail the methods used during the project. Provide an overview of the techniques/technologies used, why you used them and how you used them. Refer to the source-code delivered with the project. Describe any problems you encountered. //

Se utilizaron principalmente Pig y Spark para realizar consultas sobre la base de datos. En principio, se utilizó Pig debido a su simplicidad al momento de hacer las consultas y almacenarlas. Se utilizó Spark debido a que una de las consultas era mas compleja, pues  y se necesitó procesar y realizar JOINS complejos y pesados sobre la base de datos. Como Spark divide el trabajo de manera mas apropiada, se obtuvieron los resultados mas rápidamente que en Pig.

Algunos problemas encontrados fueron por ejemplo que debido a saltos de linea presentes en el set de datos que eran interpretados de forma incorrecta tanto por Pig como por Spark, fue necesario realizar un preprocesamiento de los datos, en este caso, en python.

## Resultados

//Detail the results of the project. Different projects will have different types of results; e.g., run-times or result sizes, evaluation of the methods you're comparing, the interface of the system you've built, and/or some of the results of the data analysis you conducted.//

Debido a la cantidad de información obtenida, es complejo demostrarlos en el presente texto, por lo que se generaron visualizaciones utilizando Tableau para describirlas. Los resultados que si se pueden presentar aquí son:

###Ciudad mas alcoholica:

|Year|County|City|Total Volume Sold (Liters)|Population|Liters per capita|County coordinates|
|----|------|----|--------------------------|----------|-----------------|------------------|
|2012|Dickinson|SPIRIT LAKE|74841.08001|4936|15.16229336|(43.3779848, -95.1508301)|
|2013|Polk|WINDSOR HEIGHTS|84216.11005|4992|16.87021435|(41.6855048, -93.5735335)|
|2014|Polk|WINDSOR HEIGHTS|87592.70005|4992|17.54661459|(41.6855048, -93.5735335)|
|2015|Polk|WINDSOR HEIGHTS|93153.36005|5021|18.55275046|(41.6855048, -93.5735335)|
|2016|Polk|WINDSOR HEIGHTS|31796.97002|5010|6.346700602|(41.6855048, -93.5735335)|

###Condado mas alcoholico:

|Year|County|Total Volume Sold (Liters)|Population|Liters per capita|County coordinates|
|----|------|--------------------------|----------|-----------------|------------------|
|2012|Dickinson|134454.78|16924|7.944621841|(43.3779848, -95.1508301)|
|2013|Dickinson|127827.05|16876|7.57448744|(43.3779848, -95.1508301)|
|2014|Dickinson|131560.43|16830|7.817019016|(43.3779848, -95.1508301)|
|2015|Dickinson|142531.24|16990|8.389125371|(43.3779848, -95.1508301)|
|2016|Dickinson|101514.13|17104|5.935110503|(43.3779848, -95.1508301)|
|2017|Adair|4410.250002|7054|0.625212646|(41.3307464, -94.4709413)|

###Categorias de alcohol mas consumidas:

|Category Name|Number of transactions|
|-|-|
|VODKA 80 PROOF|1265974|
|CANADIAN WHISKIES|936212|
|STRAIGHT BOURBON WHISKIES|543684|
|SPICED RUM|530323|
|VODKA FLAVORED|502813|
|BLENDED WHISKIES|441610|
|TEQUILA|435298|
|IMPORTED VODKA|404048|
|PUERTO RICO & VIRGIN ISLANDS RUM|395376|
|WHISKEY LIQUEUR|334572|

###Distribuidores mas grandes de alcohol:
|Vendor Name|Number of transactions|
|-|-|
|Diageo Americas|1684443|
|Jim Beam Brands|1228550|
|Luxco-St Louis|1044534|
|Pernod Ricard USA/Austin Nichols|627973|
|Constellation Wine Company, Inc.|538552|
|Bacardi U.S.A., Inc.|489599|
|Heaven Hill Brands|488173|
|Sazerac North America|480349|
|DIAGEO AMERICAS|474104|
|Sazerac Co., Inc.|458168|


## Conclusión

//Summarise main lessons learnt. What was easy? What was difficult? What could have been done better or more efficiently?//


## Apéndice

// You can use this for key code snippets that you don't want to clutter the main text. //
