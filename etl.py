import pandas as pd
import os

print("✅ Script ETL iniciado")

# 📂 Rutas
RAW_DIR = r"C:\Users\johan\OneDrive\Documentos\Documentos Johan\Pruebas de seleccion\Prueba_Tecnica_BI\Data\Raw"
OUTPUT_DIR = r"C:\Users\johan\OneDrive\Documentos\Documentos Johan\Pruebas de seleccion\Prueba_Tecnica_BI\Data\Processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Diccionario de tablas
tables = {}

# === 1. EXTRACCIÓN ===
for file in os.listdir(RAW_DIR):
    file_path = os.path.join(RAW_DIR, file)

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

print("\n📊 Tablas disponibles:")
for name in tables.keys():
    print("-", name)


# === 2. TRANSFORMACIÓN ===
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

    # Manejo de nulos
    nulls = df.isnull().sum().sum()
    if nulls > 0:
        print(f"   ⚠️ {nulls} valores nulos detectados")
        df = df.fillna("NA")

    # Conversión de fechas
    for col in df.columns:
        if "date" in col or "fecha" in col:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except:
                pass

    # === Corrección de negativos ===
    if name == "invoices":
        for col in [
            "net_invoice_quantity",
            "net_invoice_price",
            "net_invoice_value",
            "delivery_cost",
            "late_delivery_penalties",
            "overdue_payment_penalties",
            "taxes_&_commercial_fees",
            "freight",
            "net_invoice_cost",
            "net_invoice_cogs"
        ]:
            if col in df.columns:
                df[col] = df[col].abs()
        print("   ✅ Corrección aplicada a valores negativos en Invoices")

    if name == "budget":
        if "total_budget" in df.columns:
            df["total_budget"] = df["total_budget"].abs()
        print("   ✅ Corrección aplicada a valores negativos en Budget")

    if name == "forecast":
        if "forecast_(eur)" in df.columns:
            df["forecast_(eur)"] = df["forecast_(eur)"].abs()
        print("   ✅ Corrección aplicada a valores negativos en Forecast")

    return df


# Procesar todas las tablas
for name, df in tables.items():
    cleaned_df = clean_table(name, df)

    # Guardar en carpeta Processed
    output_path = os.path.join(OUTPUT_DIR, f"{name}_clean.csv")
    cleaned_df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"   ✅ Guardado en {output_path}")


# === 3. VALIDACIÓN FINAL ===
print("\n📌 Verificación de mínimos (para confirmar que no hay negativos):\n")
for check in ["invoices_clean.csv", "budget_clean.csv", "forecast_clean.csv"]:
    path = os.path.join(OUTPUT_DIR, check)
    if os.path.exists(path):
        df = pd.read_csv(path, encoding="utf-8")
        print(f"👉 {check}")
        print(df.min(numeric_only=True))
        print()
    else:
        print(f"⚠️ No encontré {check} en Processed")

print("✅ Script ETL finalizado correctamente")
