#!/usr/bin/env python3
import json
import sys
import os
import requests
from requests.auth import HTTPBasicAuth

if len(sys.argv) < 2:
    print("Uso: python3 scrapeo_qa.py <ticket_json_file>")
    sys.exit(1)

ticket_file = sys.argv[1]

# Leer ticket original
with open(ticket_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

# Config
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_AUTH = os.environ.get("JIRA_AUTH")  # debe ser Base64 user:token
PROJECT_KEY = ticket["fields"]["project"]["key"]
ISSUE_TYPE = ticket["fields"]["issuetype"]["name"]
CF_KEY = "customfield_10078"

# Obtener metadata de creación de issues
createmeta_url = f"{JIRA_URL}/rest/api/3/issue/createmeta?projectKeys={PROJECT_KEY}&issuetypeNames={ISSUE_TYPE}&expand=projects.issuetypes.fields"

headers = {
    "Accept": "application/json",
    "Authorization": f"Basic {JIRA_AUTH}"
}

r = requests.get(createmeta_url, headers=headers)
if r.status_code != 200:
    print(f"❌ Error al consultar Jira: {r.status_code}")
    print(r.text)
    sys.exit(1)

meta = r.json()

if not meta.get("projects"):
    print(f"❌ No se encontraron proyectos en la metadata para {PROJECT_KEY}/{ISSUE_TYPE}")
    sys.exit(1)

fields_meta = meta["projects"][0]["issuetypes"][0]["fields"]

if CF_KEY not in fields_meta:
    print(f"❌ No se encontró el campo {CF_KEY} en la metadata del issue")
    sys.exit(1)

allowed_values = fields_meta[CF_KEY].get("allowedValues", [])
value_to_find = ticket["fields"].get(CF_KEY)

selected_id = None
for opt in allowed_values:
    # Revisar children
    for child in opt.get("children", []):
        if child.get("value") == value_to_find:
            selected_id = child.get("id")
            break
    if selected_id:
        break

if not selected_id:
    print(f"❌ No se encontró opción válida para '{value_to_find}'")
    sys.exit(1)

# Reemplazar valor en el ticket
ticket["fields"][CF_KEY] = {"id": selected_id}

# Guardar JSON procesado
output_file = ticket_file.replace(".json", "_processed.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
