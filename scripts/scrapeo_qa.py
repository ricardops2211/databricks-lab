#!/usr/bin/env python3
import json
import sys
import requests
import base64
import os

# --- Configuración ---
ticket_file = sys.argv[1]  # ejemplo: tickets/story_001.json
jira_url = os.getenv("JIRA_URL")
jira_auth = os.getenv("JIRA_AUTH")  # base64: username:token

if not jira_url or not jira_auth:
    print("❌ Error: No se encontraron las variables de entorno JIRA_URL o JIRA_AUTH")
    sys.exit(1)

# Leer el ticket original
with open(ticket_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

# Valor del customfield que queremos mapear
custom_value = ticket["fields"].get("customfield_10078")
if not custom_value:
    print("❌ No se encontró customfield_10078 en el ticket")
    sys.exit(1)

# Obtener metadata de Jira
createmeta_url = f"{jira_url}/rest/api/3/issue/createmeta?projectKeys={ticket['fields']['project']['key']}&issuetypeNames={ticket['fields']['issuetype']['name']}&expand=projects.issuetypes.fields"
resp = requests.get(createmeta_url, headers={
    "Authorization": f"Basic {jira_auth}",
    "Accept": "application/json"
})

if resp.status_code != 200:
    print(f"❌ Error al consultar Jira: {resp.status_code}")
    print(resp.text)
    sys.exit(1)

meta = resp.json()

# Buscar el campo customfield_10078
fields_meta = meta.get("projects", [])[0].get("issuetypes", [])[0].get("fields", {})
field_cf = fields_meta.get("customfield_10078")
if not field_cf:
    print("❌ No se encontró customfield_10078 en la metadata")
    sys.exit(1)

allowed_values = field_cf.get("allowedValues", [])

# Buscar el ID padre e hijo correspondiente al valor
parent_id = None
child_id = None

for option in allowed_values:
    # Primer nivel
    if "children" in option:
        for child in option["children"]:
            if child["value"] == custom_value:
                parent_id = option["id"]
                child_id = child["id"]
                break
    # Si ya encontramos
    if parent_id and child_id:
        break

if not parent_id or not child_id:
    print(f"❌ No se encontró opción válida para '{custom_value}'")
    sys.exit(1)

# Reemplazar en el ticket
ticket["fields"]["customfield_10078"] = {"id": parent_id, "child": {"id": child_id}}

# Guardar JSON procesado
output_file = ticket_file.replace(".json", "_processed.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
