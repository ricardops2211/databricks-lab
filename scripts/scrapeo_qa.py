#!/usr/bin/env python3
import json
import sys
import os
import requests

# Archivos
input_file = sys.argv[1]  # tickets/story_001.json
output_file = input_file.replace(".json", "_processed.json")

# Variables de entorno (desde GitHub Actions)
JIRA_URL = os.getenv("JIRA_URL")
JIRA_AUTH = os.getenv("JIRA_AUTH")
PROJECT_KEY = os.getenv("JIRA_PROJECT", "ONE")

if not JIRA_URL or not JIRA_AUTH:
    print("❌ Error: JIRA_URL o JIRA_AUTH no definidos")
    sys.exit(1)

# Leer ticket
with open(input_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

# Valor que queremos buscar (ej: "Cristiano Ronaldo")
value_to_find = ticket["fields"]["customfield_10078"]

# Obtener metadata de creación de issues
createmeta_url = f"{JIRA_URL}/rest/api/3/issue/createmeta?projectKeys={PROJECT_KEY}&issuetypeNames=Story&expand=projects.issuetypes.fields"
headers = {"Authorization": f"Basic {JIRA_AUTH}", "Accept": "application/json"}

r = requests.get(createmeta_url, headers=headers)
if r.status_code != 200:
    print(f"❌ Error al consultar Jira: {r.status_code}")
    print(r.text)
    sys.exit(1)

meta = r.json()

# Extraer allowedValues del customfield_10078
fields_meta = meta["projects"][0]["issuetypes"][0]["fields"]
cf_meta = fields_meta.get("customfield_10078")
if not cf_meta or "allowedValues" not in cf_meta:
    print("❌ No se encontró metadata de customfield_10078")
    sys.exit(1)

allowed_values = cf_meta["allowedValues"]

selected_id = None
for opt in allowed_values:
    # hijos (cascading select)
    for child in opt.get("children", []):
        if child["value"] == value_to_find:
            selected_id = child["id"]
            break
    if selected_id:
        break

if not selected_id:
    print(f"❌ No se encontró opción válida para '{value_to_find}'")
    sys.exit(1)

# Reemplazar valor en ticket
ticket["fields"]["customfield_10078"] = {"id": selected_id}

# Guardar ticket procesado
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
