#!/usr/bin/env python3
import json
import sys

# Leer archivo JSON
input_file = sys.argv[1]  # ejemplo: tickets/story_001.json
with open(input_file, "r", encoding="utf-8") as f:
    ticket = json.load(f)

# Leer archivo JSON de allowedValues de Jira (puede descargarse con GET /rest/api/3/field/customfield_10078)
# Suponiendo que lo tienes en scripts/customfield_10078.json
with open("scripts/customfield_10078.json", "r", encoding="utf-8") as f:
    options = json.load(f)["allowedValues"]

value_to_find = ticket["fields"]["customfield_10078"]
selected_id = None

# Buscar ID correspondiente al valor
for opt in options:
    if "children" in opt:
        for child in opt["children"]:
            if child["value"] == value_to_find:
                selected_id = child["id"]
                break
    if selected_id:
        break

if not selected_id:
    print(f"❌ Error: No se encontró opción válida para '{value_to_find}'")
    sys.exit(1)

# Reemplazar valor en el ticket
ticket["fields"]["customfield_10078"] = {"id": selected_id}

# Guardar JSON modificado
output_file = input_file.replace(".json", "_processed.json")
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(ticket, f, ensure_ascii=False, indent=2)

print(f"✅ JSON procesado y guardado en {output_file}")
