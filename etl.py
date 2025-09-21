import pandas as pd
import os

print("Script ETL iniciado")

# Ruta donde esta la data cruda
data_path = r"C:\Users\johan\OneDrive\Documentos\Documentos Johan\Pruebas de seleccion\Prueba_Tecnica_BI\Data\Raw"

# Carpeta de salida
OUTPUT_DIR = r"C:\Users\johan\OneDrive\Documentos\Documentos Johan\Pruebas de seleccion\Prueba_Tecnica_BI\Data\Processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Diccionario para guardar las tablas cargadas
tables = {}

# Cargar archivos CSV y Parquet
for file in os.listdir(data_path):
    file_path = os.path.join(data_path, file)

    if file.endswith(".csv"):
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="latin1")
        tables[file.replace(".csv", "")] = df
        print(f"Cargado CSV: {file} -> {df.shape}")

    elif file.endswith(".parquet"):
        df = pd.read_parquet(file_path)
        tables[file.replace(".parquet", "")] = df
        print(f"Cargado Parquet: {file} -> {df.shape}")

print("\nTablas disponibles:")
for name in tables.keys():
    print("-", name)

#Función de limpieza
def clean_table(name, df):
    print(f"\nLimpiando tabla: {name}")

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Quitar duplicados
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    if before != after:
        print(f"{before - after} duplicados eliminados")

    # Manejo de nulos
    nulls = df.isnull().sum().sum()
    if nulls > 0:
        print(f"{nulls} valores nulos detectados")
        df = df.fillna("NA")

    # Conversión de fechas
    for col in df.columns:
        if "date" in col or "fecha" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except:
                pass

    #Corrección específica por tabla
    if name == "invoices":
        for col in ["net_invoice_quantity", "net_invoice_price", "net_invoice_value"]:
            if col in df.columns:
                df[col] = df[col].astype(float).abs()
        print("Corrección aplicada a valores negativos en Invoices")

    if name == "budget":
        if "total_budget" in df.columns:
            df["total_budget"] = (
                df["total_budget"]
                .astype(str)
                .str.replace(",", ".", regex=False)
            )
            df["total_budget"] = pd.to_numeric(df["total_budget"], errors="coerce").abs()
        print("Corrección aplicada a valores negativos y texto en Budget")

    if name == "forecast":
        if "forecast_(eur)" in df.columns:
            df["forecast_(eur)"] = pd.to_numeric(df["forecast_(eur)"], errors="coerce").abs()
        print("Corrección aplicada a valores negativos en Forecast")

    # Guardar archivo limpio
    output_path = os.path.join(OUTPUT_DIR, f"{name}_clean.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Guardado en {output_path}")

    return df

# Procesar todas las tablas
processed_tables = {}
for name, df in tables.items():
    processed_tables[name] = clean_table(name, df)

# Verificación final de negativos en tablas clave
print("\nVerificación de mínimos y máximos (invoices, budget, forecast):")
for key in ["invoices", "budget", "forecast"]:
    fname = os.path.join(OUTPUT_DIR, f"{key}_clean.csv")
    if os.path.exists(fname):
        df = pd.read_csv(fname)
        print(f"\n{key}_clean.csv")
        print("Mínimos:\n", df.min(numeric_only=True))
        print("Máximos:\n", df.max(numeric_only=True))

print("\n Script ETL finalizado correctamente")
