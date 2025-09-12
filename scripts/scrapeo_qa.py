#!/usr/bin/env python3
import json
import sys
import requests
import os
from requests.auth import HTTPBasicAuth

# ----------- Parámetros -----------
if len(sys.argv) < 2:
    print("Uso: python3 scrapeo_qa.py <ticket_file.json>")
    sys.exit(1)

ticket_file = sys.argv[1]  # ejemplo: tickets/story_001.json
jira_url = os.environ.get("JIRA_URL")  # https://tu_jira.atlassian.net
jira_auth = os.environ.get("JIRA_AUTH")  # Base64 de usuario:API token
customfield_id = "customfield_10078"  # campo cascading select QA

if not jira_url or not jira_auth:
    print("❌ Error: debes definir JIRA_URL y JIRA_AUTH en variables de entorno")
    sys.exit(1)

# ----------- Leer ticket -----------

with open(ticket_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

value_to_find = ticket["fields"].get(customfield_id)
if not value_to_find:
    print(f"❌ Error: No se encontró el campo {customfield_id} en el JSON")
    sys.exit(1)

# ----------- Consultar Jira -----------

url_field = f"{jira_url}/rest/api/3/field/{customfield_id}"
resp = requests.get(url_field, headers={"Authorization": f"Basic {jira_auth}", "Accept": "application/json"})

if resp.status_code != 200:
    print(f"❌ Error al consultar Jira: {resp.status_code} {resp.text}")
    sys.exit(1)

field_data = resp.json()
allowed_values = field_data.get("allowedValues", [])

# ----------- Buscar valor en children -----------

selected_id = None

for opt in allowed_values:
    # Revisar hijos
    for child in opt.get("children", []):
        if child["value"].lower() == str(value_to_find).lower():
            selected_id = child["id"]
            break
    if selected_id:
        break

if not selected_id:
    print(f"❌ Error: No se encontró opción válida para '{value_to_find}' en allowedValues de Jira")
    sys.exit(1)

# ----------- Reemplazar valor en ticket -----------

ticket["fields"][customfield_id] = {"value": value_to_find}

# ----------- Guardar JSON procesado -----------

output_file = ticket_file.replace(".json", "_processed.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
