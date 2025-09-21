 Prueba Técnica – Senior BI Analyst
 # Objetivo:

Desarrollar un caso de negocio utilizando el conjunto de datos de SpaceParts.

Proceso ETL con Python

Modelado y visualización en Power BI.

Control de versiones con Git Flow.

Publicación en Power BI Service.

# Arquitectura de la solución


1. ETL en Python:

Archivo: etl.py

Procesos a realizar:

Lectura de archivos CSV y Parquet desde /Data/Raw.

Normalización de nombres de columnas (snake_case).

Eliminación de duplicados.

Manejo de nulos (fillna).

Conversión de campos de fechas.

Corrección de valores negativos en tablas con valores de dínero.

- Invoices: NetInvoiceQuantity, NetInvoicePrice, NetInvoiceValue.

- Budget: TotalBudget.

- Forecast: Forecast_(EUR).

Librerías utilizadas:

pandas: Para el procesamiento de datos.

os: Para el manejo de rutas y archivos.

Comentario:

Los archivos limpios quedaron direccionados en /Data/Processed/*.csv

# Modelo de datos en Power BI

1. Fact Tables

Descripción de las Fact Tables:

Invoices: Ventas, costos, envíos, penalidades.

Orders: Órdenes de clientes (fechas de creación, entrega, cumplimiento).

Budget: Objetivos de ventas anuales por cliente/producto.

Forecast: Revisión mensual del budget, más agregada.


2. Dimension Tables

Descripción de las Dim Tables:

Customers: Clientes

Regions: Regiones ficticias por cliente.

Products: Productos, tipo de productos.

Brands: Marcas asociadas a productos.

Employees: Empleados y roles.

Invoice DocType / Order DocType: Tipos de facturas y órdenes.

Order Status: Estados de órdenes.

Otras tablas

Exchange Rate: Tasas de cambio diarias.

Budget Rate: Tasas de cambio anuales.

Date Table: Tabla calendario generada en Power BI.



Relaciones en el Diagrama ER

Invoices ↔ Customers, Products, DocTypes.

Orders ↔ Customers, Products, Order Status.

Invoices[BillingDate] ↔ Date[Date] (relación activa).

Invoices[ShipDate] ↔ Date[Date] (relación inactiva).

Invoices[Currency] ↔ ExchangeRate[Currency].

# Nota:
A las tablas de dimensión se les agregó la sigla "Dim" para facilitar la identificación.

#  Medidas DAX principales

-- Ventas netas en EUR con conversión (usando Budget-rate)
Net Sales    =
SUMX (
    Invoices,
    Invoices[NetInvoiceValue] *
    RELATED('Dim_budget-rate'[rate])
)

-- Ventas por fecha de facturación

Sales x Billing Date = [Net_Sales]

-- Ventas por fecha de envío
Sales x Shipments =
CALCULATE (
    [Net_Sales],
    USERELATIONSHIP (Invoices[ShipDate], 'Date'[Date])
)

-- Budget y Forecast en EUR
Total_Budget = SUM(Budget[TotalBudget])
Total_Forecast = SUM(Forecast[Forecast_(EUR)])

# Nota: En la tabla de Budget se encuentra inconsistencia con el tipo de dato en cuanto al uso del punto decimal. Se debe validar con el cliente la información.

-- Cumplimiento de Budget
Budget Achievement % =
DIVIDE([Net Sales], [Total_Budget], 0)

-- Variación vs Budget
Variance vs Budget = [Net Sales] - [Total Budget]

--Total Ordenes
Total Orders = COUNTROWS(orders_clean)

--Total ordenes facturadas

Total Orders_Invoices = CALCULATE(
    [Total Orders],orders_clean[sales_order_document_line_item_status]="Invoiced")


-- Facturas entregadas a tiempo
OnTime Invoices = CALCULATE(COUNTROWS(invoices),invoices[otd_indicator]=TRUE()) 
OnTime_Invoices % =
DIVIDE([OnTime Invoices],[Total Invoices])

--Facturas entregadas tarde
Late Invoices = CALCULATE(
    COUNTROWS(invoices),invoices[otd_indicator]=FALSE()) 


% Late_Invoices = DIVIDE(
    [Late Invoices],[Total Invoices])

--Total Pago Penalidades

Total Payments Penalties = SUMX(
    invoices,invoices[overdue_payment_penalties]*RELATED('Dim_budget-rate'[rate]))

--Porcentaje de particacion de penalidades por tipo de cuenta
Payments Penalties Part % = DIVIDE(
    [Total Payments Penalties],CALCULATE(
        [Total Payments Penalties],ALL(Dim_customers[account_type],Dim_customers[account_type])))


-- Crecimiento YoY
YoY Sales Growth = 
VAR CurrentYear = [Net_Sales]
VAR LastYear  = 
    CALCULATE([Net_Sales],SAMEPERIODLASTYEAR('Date'[Date]))
RETURN 
DIVIDE(CurrentYear-LastYear,LastYear,0)

       4. Reportes construidos en Power BI
Página 1: Overview (Margen Gross por añor, Ventas netas y crecimiento mes anterior)

Tarjetas: Ventas netas, %Gross Margin, forecast Achievement

Gráfico de lineas: Crecimiento en ventas mes anterior por año vs Gross Margin

Gráfico de lineas con barras: Ventas netas Vs Margen por año

Gráfico de Anillos: % de participación de cada tipo de factura

Página 2: Product Sales

Tarjetas: Margen Gross, % de facturas del total de ordenes

Matriz: Margen Gross por tipo de producto por año

Grafico Anillos: % De participación por tipo de marca  de producto

Grafico de lineas: Margen Gross por tipo de cuenta/ año

Grafico de Barras: Porcentaje de Participacion de cada marca

Página 3: Logistic & Order

Tarjetas: Total facturas, % facturas sobre el total de ordenes, %Facturas a tiempo.

Grafico de anillos: % de ordenes por estado

Grafico de anillos: % de penalidades por tipo de cuenta

Tabla: Porcentaje de facturado a tiempo y tarde por año

Grafico de area: Porcentaje de facturas entregadas a tiempo por año y mes.
 

# . Publicación en Power BI Service

Publicación del .pbix en Power BI Service.


# . Git Flow y control de versiones

Rama principal: main.

Ramas de features: feature/etl, feature/modelo_pbi, etc.

Commits descriptivos.

.gitignore configurado para excluir datasets grandes.

# Analisis de la información: 

Hallazgos:
* A pesa del decrecimiento en Ventas, las utilidades(Margen) se mantienen dentro del promedio anual.
* Se observa una afectación en los productos de tipo "Planetary systems", donde el margen se encuentra por debajo del total en un 5%
* Se observa una baja participación en ordenes de productos premiums, con el 1,63% de las ventas.
* Para la temporada de Julio en todos los años se evidencia una caida en las utilidades (Gross Margen), la cual se logra recuperar a inicio de año.

Recomendaciones: 

* Revisar con el area comercial la categoria de productos "Planetary systems" para encontrar la afectación y  encontrar estrategias que permitan impulsar la venta.
* Impulsar con estrategias comerciales los productos premiums.
* Revisar con el area comercial y gerentes de territorios la posibilidad de realizar ofertas llamativas en las temporadas de mitad de año debido a la caida en ventas.

