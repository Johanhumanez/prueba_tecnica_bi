# Proceso ETL

1. Extracción

-Lectura de archivos .csv y .parquet (fact y dimensiones del    dataset SpaceParts).

-Uso de librerías: pandas, pyarrow.

2. Transformación

-Estandarización de nombres de columnas.

-Eliminación de duplicados.

-Manejo de valores nulos.

-Conversión de tipos de datos (fechas, numéricos).

3. Carga (Load)

-Los datasets limpios se exportan a la carpeta Data/      Processed/   listos para su uso en Power BI.

# Requisitos

1. Instalar dependencias necesarias:

-pip install pandas pyarrow


# Ejecución

1. Para correr el proceso ETL:

-python etl.py


# Modelado en Power BI

1. Los datasets procesados se importan a Power BI.
2. Se construye un modelo estrella con tablas fact (Invoices, Orders, Budget, Forecast) y dimensiones (Customers, Products, Regions,Products, Brands).
3. Se implementan medidas DAX para análisis de ventas, presupuesto y desempeño.

# Entregables 

1. Repositorio GitHub con scripts y documentación.

2. Script ETL (etl.py) documentado.

3. Archivo Power BI (.pbix) con modelo y visualizaciones.

4. Publicación en Power BI Service para compartir reportes.

    