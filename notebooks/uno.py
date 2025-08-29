# Databricks notebook source
# COMMAND ----------
dbutils.widgets.text("input_path", "dbfs:/mnt/input/data.csv", "Input Path")
dbutils.widgets.text("output_path", "dbfs:/mnt/output/results/", "Output Path")

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")

print(f"📥 Leyendo datos desde: {input_path}")
print(f"💾 Guardando resultados en: {output_path}")

# Ejemplo: leer CSV, hacer transformación y guardar
df = spark.read.option("header", "true").csv(input_path)

# Pequeña transformación: contar registros por columna "category"
result = df.groupBy("category").count()

result.write.mode("overwrite").parquet(output_path)

print("✅ Proceso ETL finalizado correctamente")
