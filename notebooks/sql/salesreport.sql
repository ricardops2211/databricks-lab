-- 📊 Notebook SQL: ETL de Ventas
-- Objetivo: Leer datos crudos de ventas, transformarlos y generar un reporte agregado.

-- 1️⃣ Crear base de datos si no existe
CREATE DATABASE IF NOT EXISTS analytics;

-- 2️⃣ Usar esa base
USE analytics;

-- 3️⃣ Crear tabla externa con datos crudos (ejemplo: montados desde DBFS)
CREATE TABLE IF NOT EXISTS raw_sales (
    sale_id STRING,
    product_id STRING,
    customer_id STRING,
    quantity INT,
    price DECIMAL(10,2),
    sale_date DATE
)
USING PARQUET
LOCATION 'dbfs:/FileStore/data/raw_sales/';

-- 4️⃣ Crear tabla transformada con ingresos calculados
CREATE OR REPLACE TABLE sales_enriched AS
SELECT
    sale_id,
    product_id,
    customer_id,
    quantity,
    price,
    (quantity * price) AS revenue,
    sale_date
FROM raw_sales;

-- 5️⃣ Generar reporte de ventas por producto
CREATE OR REPLACE TABLE sales_report AS
SELECT
    product_id,
    SUM(revenue) AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers,
    MAX(sale_date) AS last_purchase_date
FROM sales_enriched
GROUP BY product_id
ORDER BY total_revenue DESC;

-- 6️⃣ Mostrar resultados
SELECT * FROM sales_report LIMIT 20;
