#!/usr/bin/env python3
import json
import sys
import os
import requests
from base64 import b64decode

# --- Variables de entorno ---
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_AUTH = os.environ.get("JIRA_AUTH")
PROJECT_KEY = os.environ.get("JIRA_PROJECT", "KAN")
ISSUE_TYPE = "Story"  # se puede parametrizar si quieres

if not JIRA_URL or not JIRA_AUTH:
    print("❌ Error: debes definir JIRA_URL y JIRA_AUTH en el entorno")
    sys.exit(1)

# --- Leer JSON del ticket ---
input_file = sys.argv[1]  # ejemplo: tickets/story_001.json
with open(input_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

# --- Obtener metadata de Jira para el proyecto y tipo de issue ---
url = f"{JIRA_URL}/rest/api/3/issue/createmeta?projectKeys={PROJECT_KEY}&issuetypeNames={ISSUE_TYPE}&expand=projects.issuetypes.fields"
headers = {
    "Authorization": f"Basic {JIRA_AUTH}",
    "Accept": "application/json"
}

resp = requests.get(url, headers=headers)
if resp.status_code != 200:
    print(f"❌ Error al consultar Jira: {resp.status_code} {resp.text}")
    sys.exit(1)

meta = resp.json()
projects = meta.get("projects", [])
if not projects:
    print("❌ No se encontraron proyectos en la metadata de Jira")
    sys.exit(1)

issuetypes = projects[0].get("issuetypes", [])
if not issuetypes:
    print("❌ No se encontraron tipos de issue en la metadata de Jira")
    sys.exit(1)

fields_meta = issuetypes[0].get("fields", {})
field_10078_meta = fields_meta.get("customfield_10078")
if not field_10078_meta:
    print("❌ No se encontró customfield_10078 en la metadata")
    sys.exit(1)

allowed_values = field_10078_meta.get("allowedValues", [])

# --- Buscar el ID correspondiente al valor del ticket ---
value_to_find = ticket["fields"].get("customfield_10078")
if not value_to_find:
    print("❌ No se encontró valor para customfield_10078 en el ticket")
    sys.exit(1)

selected_option = None
for opt in allowed_values:
    if "children" in opt:
        for child in opt["children"]:
            if child["value"] == value_to_find:
                selected_option = {"id": opt["id"], "child": {"id": child["id"]}}
                break
    else:
        if opt["value"] == value_to_find:
            selected_option = {"id": opt["id"]}
            break
    if selected_option:
        break

if not selected_option:
    print(f"❌ Error: no se encontró opción válida para '{value_to_find}' en Jira")
    sys.exit(1)

# --- Reemplazar el valor en el ticket ---
ticket["fields"]["customfield_10078"] = selected_option

# --- Guardar JSON procesado ---
output_file = input_file.replace(".json", "_processed.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
