#!/usr/bin/env python3
import json
import sys
import os
import requests
from base64 import b64encode

JIRA_URL = os.getenv("JIRA_URL")
JIRA_AUTH = os.getenv("JIRA_AUTH")
PROJECT_KEY = os.getenv("JIRA_PROJECT")

input_file = sys.argv[1]
output_file = input_file.replace(".json", "_processed.json")

# Leer ticket original
with open(input_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

value_to_find = ticket["fields"]["customfield_10078"]

# Obtener metadata de Jira para el campo cascada
url = f"{JIRA_URL}/rest/api/3/issue/createmeta?projectKeys={PROJECT_KEY}&issuetypeNames=Story&expand=projects.issuetypes.fields"
headers = {
    "Authorization": f"Basic {JIRA_AUTH}",
    "Accept": "application/json"
}
resp = requests.get(url, headers=headers)
if resp.status_code != 200:
    print(f"❌ Error al consultar Jira: {resp.status_code}")
    sys.exit(1)

meta = resp.json()
fields_meta = meta["projects"][0]["issuetypes"][0]["fields"]
cf_field = fields_meta["customfield_10078"]
allowed = cf_field["allowedValues"]

selected_id = None

for parent in allowed:
    children = parent.get("children", [])
    for child in children:
        if child["value"] == value_to_find:
            selected_id = child["id"]
            break
    if selected_id:
        break

if not selected_id:
    print(f"❌ No se encontró opción válida para '{value_to_find}'")
    sys.exit(1)

# Reemplazar en ticket
ticket["fields"]["customfield_10078"] = {"id": selected_id}

# Guardar JSON procesado
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
