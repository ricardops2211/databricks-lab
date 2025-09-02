# Databricks notebook source
from pyspark.sql import SparkSession
from helper_notebook import cargar_parametros, calcular_descuento, obtener_nombre_reporte
from pyspark.sql.functions import col, expr

# Iniciar sesión Spark (Databricks ya lo trae como spark por defecto)
spark = SparkSession.builder.getOrCreate()

# 🔹 Cargar parámetros del bloque py_job en YAML
params = cargar_parametros("py_job")

# 📥 Leer clientes (CSV)
df_clientes = spark.read.csv(
    "dbfs:/FileStore/jobs_data/raw/clientes.csv",
    header=True,
    inferSchema=True
)

# 📥 Leer ventas (JSON)
df_ventas = spark.read.json("dbfs:/FileStore/jobs_data/raw/ventas.json")

# 🔗 JOIN clientes con ventas
df_join = df_clientes.join(
    df_ventas,
    df_clientes.id == df_ventas.id_cliente,
    "inner"
)

# 🧮 Calcular total con posible descuento
df_result = df_join.withColumn(
    "total",
    expr("cantidad * precio")
)

# Aplicar descuento (UDF con los parámetros)
apply_descuento = lambda monto: calcular_descuento(monto, job="py_job", params=params)
udf_descuento = spark.udf.register("apply_descuento", apply_descuento)

df_result = df_result.withColumn(
    "total_descuento",
    expr("apply_descuento(total)")
)

# 💾 Guardar reporte en DBFS con el nombre dinámico
output_path = f"dbfs:/FileStore/jobs_output/{obtener_nombre_reporte('py_job', params)}"
df_result.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

print(f"✅ Reporte generado en {output_path}")

# 👀 Mostrar datos combinados
df_result.show(10)
