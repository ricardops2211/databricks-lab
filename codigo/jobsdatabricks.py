#!/usr/bin/env python3
import os
import json
import subprocess
import sys

# Variables de entorno necesarias
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
print(f"Usando token DATABRICKS_TOKEN: {'*'*8}...")


if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    print("❌ Faltan variables de entorno DATABRICKS_HOST o DATABRICKS_TOKEN")
    sys.exit(1)

# Función para ejecutar comandos shell y obtener output
def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Error ejecutando: {cmd}")
        print(result.stderr)
        sys.exit(1)
    return result.stdout

# Listar todos los jobs existentes
jobs_list_json = run_cmd("databricks jobs list --output JSON")
jobs_list = json.loads(jobs_list_json).get("jobs", [])

# Guardar resumen
summary = []

# Iterar sobre archivos de jobs
jobs_dir = "jobs"
for job_file in os.listdir(jobs_dir):
    if not job_file.endswith(".json"):
        continue

    job_path = os.path.join(jobs_dir, job_file)
    with open(job_path) as f:
        job_config = json.load(f)
    job_name = job_config.get("name")

    if not job_name:
        print(f"❌ El job {job_file} no tiene un campo 'name'")
        continue

    # Verificar si el job existe
    existing_job = next((job for job in jobs_list if job["settings"]["name"] == job_name), None)

    if existing_job:
        job_id = existing_job["job_id"]
        print(f"⚠️ Se detectó job workflow existente '{job_name}' (job_id={job_id}). Se procederá a resetear...")
        run_cmd(f'databricks jobs reset --job-id {job_id} --json @"{job_path}"')
        summary.append({"name": job_name, "job_id": job_id, "action": "editado"})
    else:
        print(f"✅ No existe job workflow '{job_name}'. Se procederá a crear...")
        output = run_cmd(f'databricks jobs create --version=2.1 --json @"{job_path}"')
        created_job_id = json.loads(output)["job_id"]
        summary.append({"name": job_name, "job_id": created_job_id, "action": "creado"})

# Imprimir resumen final
print("\n📄 Resumen de jobs procesados:")
for entry in summary:
    print(f" - {entry['name']} (job_id={entry['job_id']}): {entry['action']}")
