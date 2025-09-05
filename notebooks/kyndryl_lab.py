# Databricks notebook source
import pandas as pd
import json
import yaml
import os

# 📂 Paths en DBFS
clientes_path = "/dbfs/FileStore/jobs_data/raw/clientes.csv"
ventas_path = "/dbfs/FileStore/jobs_data/raw/ventas.json"
parametros_path = "/dbfs/FileStore/jobs_data/config/parametros.yaml"

output_dir = "/dbfs/FileStore/jobs_output/reports/"
log_dir = "/dbfs/FileStore/jobs_output/logs/"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)

# 🧾 Leer parámetros
with open(parametros_path, "r") as f:
    params = yaml.safe_load(f)

umbral = params.get("umbral_monto", 50)
reporte_nombre = params.get("reporte_nombre", "reporte.csv")

# 📥 Leer datasets
clientes = pd.read_csv(clientes_path)
with open(ventas_path, "r") as f:
    ventas = pd.DataFrame(json.load(f))

# 🔗 Unir data
df = ventas.merge(clientes, left_on="cliente_id", right_on="id")

# 💡 Filtrar según umbral
df_filtrado = df[df["monto"] >= umbral]

# 📤 Guardar reporte
output_file = os.path.join(output_dir, reporte_nombre)
df_filtrado.to_csv(output_file, index=False)

# 📝 Guardar log
with open(os.path.join(log_dir, "run.log"), "w") as f:
    f.write(f"Job ejecutado correctamente.\n")
    f.write(f"Filas procesadas: {len(df)}\n")
    f.write(f"Filas filtradas: {len(df_filtrado)}\n")

print(f"✅ Reporte generado en {output_file}")
