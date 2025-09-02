# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Crear sesión Spark (Databricks ya la maneja como 'spark')
spark = SparkSession.builder.appName("ETL_Python_Demo").getOrCreate()

# 1️⃣ Leer CSV de clientes desde DBFS
clientes_df = spark.read.option("header", True).option("inferSchema", True).csv(
    "dbfs:/FileStore/jobs_data/raw/clientes.csv"
)

# 2️⃣ Leer JSON de ventas desde DBFS
ventas_df = spark.read.option("inferSchema", True).json(
    "dbfs:/FileStore/jobs_data/raw/ventas.json"
)

# 3️⃣ Unir las tablas (id del cliente vs cliente_id de ventas)
ventas_clientes_df = ventas_df.join(
    clientes_df, ventas_df.cliente_id == clientes_df.id, "inner"
).select(
    ventas_df.venta_id,
    clientes_df.nombre.alias("cliente"),
    ventas_df.producto_id,
    ventas_df.cantidad,
    ventas_df.precio,
    expr("cantidad * precio").alias("total")
)

# 4️⃣ Guardar resultados en una tabla de Delta Lake
ventas_clientes_df.write.format("delta").mode("overwrite").saveAsTable("analytics.ventas_clientes_py")

# 5️⃣ Mostrar resultados
display(ventas_clientes_df.limit(20))
