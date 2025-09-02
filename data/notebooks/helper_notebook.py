import yaml
import os

# 📌 Cargar parámetros de un bloque específico en el YAML
def cargar_parametros(job: str, path: str = "/dbfs/FileStore/jobs_data/config/parametros.yaml") -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"⚠️ No se encontró el archivo de parámetros en: {path}")
    
    with open(path, "r") as f:
        all_params = yaml.safe_load(f) or {}

    return all_params.get(job, {})  # Devuelve el bloque de ese job


# 📌 Función que aplica descuento en base al umbral definido en YAML
def calcular_descuento(monto: float, job: str = "py_job", params: dict = None) -> float:
    if params is None:
        params = cargar_parametros(job)
    
    umbral = params.get("umbral_monto", 100)
    return monto * 0.9 if monto > umbral else monto


# 📌 Función que devuelve nombre del reporte desde YAML
def obtener_nombre_reporte(job: str = "py_job", params: dict = None) -> str:
    if params is None:
        params = cargar_parametros(job)
    
    return params.get("reporte_nombre", "reporte_default.csv")
