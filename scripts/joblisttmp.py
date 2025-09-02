#!/usr/bin/env python3
# scripts/joblist_build_and_show.py
#
# Hace TODO desde Python:
# 1) Llama a la API /api/2.1/jobs/list (paginado), construye el JSON completo
# 2) Guarda el "raw" en joblist.tmp (en carpeta temporal del runner)
# 3) Muestra joblist.tmp "bonito" en consola (con límite configurable)
# 4) Genera .gha/job_ids.json con { nombre_job: job_id } y lo muestra
# 5) Limpia el tmp (a menos que KEEP_JOBLIST=true)
#
# Requisitos de entorno:
#   - DATABRICKS_HOST (p.ej. https://adb-xxx.azuredatabricks.net)
#   - DATABRICKS_TOKEN (PAT obtenido de Vault)
#
# Opcionales:
#   - PREVIEW_LINES (default 400)
#   - KEEP_JOBLIST (true/false, default false)

import os, sys, json, tempfile, urllib.request, urllib.parse, urllib.error

HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN") or ""
PREVIEW_LINES = int(os.getenv("PREVIEW_LINES", "400"))
KEEP_JOBLIST = os.getenv("KEEP_JOBLIST", "false").lower() in ("1","true","yes")
OUTPUT_PATH = ".gha/job_ids.json"

if not HOST or not TOKEN:
    print("❌ Falta DATABRICKS_HOST o DATABRICKS_TOKEN en el entorno.", file=sys.stderr)
    sys.exit(2)

# Mask del token en logs de GitHub
print(f"::add-mask::{TOKEN}")

HDRS = {"Authorization": f"Bearer {TOKEN}"}

def api_get(path: str, qs: dict | None = None) -> dict:
    url = HOST + path
    if qs:
        url += "?" + urllib.parse.urlencode(qs)
    req = urllib.request.Request(url, headers=HDRS, method="GET")
    try:
        with urllib.request.urlopen(req) as r:
            data = r.read()
            return json.loads(data.decode()) if data else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"HTTP {e.code} {e.reason} @ {url}\n{body}", file=sys.stderr)
        raise

def list_all_jobs() -> dict:
    """Devuelve un dict con clave 'jobs' agregando todas las páginas."""
    jobs = []
    page_token = None
    while True:
        qs = {"limit": 100}
        if page_token:
            qs["page_token"] = page_token
        resp = api_get("/api/2.1/jobs/list", qs)
        jobs.extend(resp.get("jobs", []))
        # soporta ambas convenciones
        page_token = resp.get("next_page_token")
        has_more = resp.get("has_more")
        if not page_token and not has_more:
            break
    return {"jobs": jobs}

def pretty_print_file(path: str, max_lines: int = 400):
    """Imprime máximo 'max_lines' líneas de un archivo."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                print(line.rstrip("\n"))
                if i >= max_lines:
                    print("... (truncado)")
                    break
    except Exception as e:
        print(f"(No se pudo leer {path}: {e})", file=sys.stderr)

def ensure_dir(p: str):
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)

def main():
    # 1) Obtener jobs (todas las páginas)
    data = list_all_jobs()

    # 2) Escribir joblist.tmp en el directorio temporal del runner
    tmp_dir = os.getenv("RUNNER_TEMP") or tempfile.gettempdir()
    joblist_path = os.path.join(tmp_dir, "joblist.tmp")
    with open(joblist_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print("")
    print("================== joblist.tmp (raw) ==================")
    # 3) Mostrar "bonito" (primeras PREVIEW_LINES líneas)
    #    Usamos json.tool para pretty-print antes de limitar líneas
    try:
        pretty_json = json.dumps(data, ensure_ascii=False, indent=2)
        lines = pretty_json.splitlines()
        for i, line in enumerate(lines, start=1):
            print(line)
            if i >= PREVIEW_LINES:
                print("... (truncado)")
                break
    except Exception:
        pretty_print_file(joblist_path, PREVIEW_LINES)
    print("=======================================================")
    print("")

    # 4) Construir mapping { nombre: job_id } y guardarlo en .gha/job_ids.json
    jobs = data.get("jobs") or []
    mapping = {}
    for j in jobs:
        job_id = j.get("job_id")
        name = (j.get("settings") or {}).get("name") or j.get("name")
        if name and job_id:
            mapping[str(name)] = job_id

    ensure_dir(OUTPUT_PATH)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    # 5) Mostrar mapping
    print("")
    print("================== .gha/job_ids.json ==================")
    try:
        print(json.dumps(mapping, ensure_ascii=False, indent=2))
    except Exception:
        print(mapping)
    print("=======================================================")
    print(f"✅ Generado {OUTPUT_PATH} con {len(mapping)} jobs")
    print("")

    # 6) Step Summary (opcional)
    step_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as sf:
            sf.write("## Job mapping (desde API jobs/list)\n")
            for k, v in mapping.items():
                sf.write(f"- {k}: {v}\n")

    # 7) Limpieza del tmp (a menos que KEEP_JOBLIST=true)
    if KEEP_JOBLIST:
        print(f"(Conservado tmp en {joblist_path} - KEEP_JOBLIST=true)")
    else:
        try:
            os.remove(joblist_path)
        except OSError:
            pass

if __name__ == "__main__":
    main()
