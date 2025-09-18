import pandas as pd
print("✅ Script ETL ejecutado correctamente.")

import pandas as pd
import os

# 📂 Ruta donde tienes los datos

data_path = r"C:\Users\johan\OneDrive\Documentos\Documentos Johan\Pruebas de seleccion\Prueba_Tecnica_BI\Data"

# Diccionario para guardar las tablas

tables = {}

# Recorremos todos los archivos dentro de Data

for file in os.listdir(data_path):
    file_path = os.path.join(data_path, file)

    if file.endswith(".csv"):
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="latin1")
        tables[file.replace(".csv", "")] = df
        print(f"✅ Cargado CSV: {file} -> {df.shape}")

    elif file.endswith(".parquet"):
        df = pd.read_parquet(file_path)
        tables[file.replace(".parquet", "")] = df
        print(f"✅ Cargado Parquet: {file} -> {df.shape}")


    # Mostrar nombres de tablas cargadas
print("\n📊 Tablas disponibles:")
for name in tables.keys():
    print("-", name)

    import os
import pandas as pd

# Carpeta de salida
OUTPUT_DIR = "Processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Función de limpieza
def clean_table(name, df):
    print(f"\n🔧 Limpiando tabla: {name}")

    # Normalizar nombres de columnas
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Quitar duplicados
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    if before != after:
        print(f"   ➖ {before - after} duplicados eliminados")

    # Manejo de nulos (ejemplo: reemplazar por NA)
    nulls = df.isnull().sum().sum()
    if nulls > 0:
        print(f"   ⚠️ {nulls} valores nulos detectados")
        df = df.fillna("NA")  # aquí puedes personalizar por columna

    # Intento de conversión de fechas (si existen columnas con 'date')
    for col in df.columns:
        if "date" in col or "fecha" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except:
                pass

    return df

# Procesar todas las tablas y guardarlas limpias
for name, df in tables.items():
    cleaned_df = clean_table(name, df)

    # Guardar en CSV limpio
    output_path = os.path.join(OUTPUT_DIR, f"{name}_clean.csv")
    cleaned_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"   ✅ Guardado en {output_path}")

    