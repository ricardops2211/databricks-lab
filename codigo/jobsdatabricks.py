#!/usr/bin/env python3
import os
import json
import subprocess
import sys

# Variables de entorno necesarias
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Mostrar token de manera segura
print(f"Usando token DATABRICKS_TOKEN: {'*'*8}...")

if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
    print("Faltan variables de entorno DATABRICKS_HOST o DATABRICKS_TOKEN")
    sys.exit(1)

# Función para ejecutar comandos shell y obtener output
def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Error ejecutando: {cmd}\n{result.stderr}")
    return result.stdout

# Guardar resumen
summary = []

# Directorio donde están los JSON de jobs
jobs_dir = "jobs"

# Directorio para guardar el resumen como artifact
outputs_dir = "outputs"
os.makedirs(outputs_dir, exist_ok=True)
summary_file = os.path.join(outputs_dir, "jobs_summary.json")

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

    print(f"✅ Creando job workflow '{job_name}'...")
    try:
        output = run_cmd(f'databricks jobs create --version 2.1 --json @"{job_path}"')
        created_job_id = json.loads(output)["job_id"]
        summary.append({"name": job_name, "job_id": created_job_id, "action": "creado"})
    except RuntimeError as e:
        print(f"❌ Error creando el job '{job_name}'. Error:\n{e}")
        summary.append({"name": job_name, "job_id": None, "action": "fallido"})
        continue

# Guardar resumen en JSON
with open(summary_file, "w") as f:
    json.dump(summary, f, indent=2)

# Imprimir resumen final
print("\n📄 Resumen de jobs procesados:")
for entry in summary:
    print(f" - {entry['name']} (job_id={entry['job_id']}): {entry['action']}")
print(f"\n📂 Resumen guardado en {summary_file}")
