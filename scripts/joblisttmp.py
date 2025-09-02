#!/usr/bin/env python3
# scripts/joblist_build_and_show.py
#
# 1) Llama /api/2.1/jobs/list con paginación
# 2) Guarda "raw" en joblist.tmp (RUNNER_TEMP)
# 3) Muestra un preview bonito (limitado)
# 4) Genera .gha/job_ids.json con filtro (repo/por nombre/regex/etc.)
# 5) (Opcional) .gha/job_ids.full.json con TODOS (solo para depurar)
# 6) Limpia el tmp (a menos que KEEP_JOBLIST=true)

import os, sys, json, glob, re, tempfile, urllib.request, urllib.parse, urllib.error

HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN") or ""
PREVIEW_LINES = int(os.getenv("PREVIEW_LINES", "400"))
KEEP_JOBLIST = os.getenv("KEEP_JOBLIST", "false").lower() in ("1","true","yes")
OUTPUT_PATH = ".gha/job_ids.json"
OUTPUT_FULL_PATH = ".gha/job_ids.full.json"
WRITE_FULL_MAPPING = os.getenv("WRITE_FULL_MAPPING","false").lower() in ("1","true","yes")

# Filtros
FILTER_REPO_ONLY = os.getenv("FILTER_REPO_ONLY","false").lower() in ("1","true","yes")
JOB_NAME = os.getenv("JOB_NAME") or ""
JOB_NAME_CONTAINS = os.getenv("JOB_NAME_CONTAINS") or ""
JOB_NAME_REGEX = os.getenv("JOB_NAME_REGEX") or ""
JOB_NAME_LIST = os.getenv("JOB_NAME_LIST") or ""  # coma-separado
IGNORE_CASE = os.getenv("IGNORE_CASE","true").lower() in ("1","true","yes")
ENFORCE_SINGLE = os.getenv("ENFORCE_SINGLE","false").lower() in ("1","true","yes")

if not HOST or not TOKEN:
    print("❌ Falta DATABRICKS_HOST o DATABRICKS_TOKEN en el entorno.", file=sys.stderr)
    sys.exit(2)

# Oculta el token en logs
print(f"::add-mask::{TOKEN}")

HDRS = {"Authorization": f"Bearer {TOKEN}"}

def api_get(path: str, qs: dict | None = None) -> dict:
    url = HOST + path
    if qs:
        url += "?" + urllib.parse.urlencode(qs)
    req = urllib.request.Request(url, headers=HDRS, method="GET")
    with urllib.request.urlopen(req) as r:
        data = r.read()
        return json.loads(data.decode()) if data else {}

def list_all_jobs() -> dict:
    jobs, page_token = [], None
    while True:
        qs = {"limit": 100}
        if page_token:
            qs["page_token"] = page_token
        resp = api_get("/api/2.1/jobs/list", qs)
        jobs.extend(resp.get("jobs", []))
        page_token = resp.get("next_page_token")
        has_more = resp.get("has_more")
        if not page_token and not has_more:
            break
    return {"jobs": jobs}

def pretty_print_preview(obj, max_lines: int):
    pretty_json = json.dumps(obj, ensure_ascii=False, indent=2)
    lines = pretty_json.splitlines()
    for i, line in enumerate(lines, start=1):
        print(line)
        if i >= max_lines:
            print("... (truncado)")
            break

def ensure_dir(p: str):
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)

def load_repo_job_names(repo_dir="jobs") -> set[str]:
    """Lee jobs/*.json y devuelve los 'name' definidos en el repo."""
    names = set()
    for fp in glob.glob(os.path.join(repo_dir, "*.json")):
        try:
            spec = json.load(open(fp, "r", encoding="utf-8"))
            name = spec.get("name")
            if name:
                names.add(str(name))
        except Exception:
            pass
    return names

def normalize(s: str) -> str:
    return s.lower() if IGNORE_CASE else s

def apply_filters(mapping_all: dict[str,int]) -> dict[str,int]:
    # Si no hay filtros, y FILTER_REPO_ONLY es false, devolver todo
    if not any([FILTER_REPO_ONLY, JOB_NAME, JOB_NAME_CONTAINS, JOB_NAME_REGEX, JOB_NAME_LIST]):
        return mapping_all

    keys = list(mapping_all.keys())

    # Conjunto base: todo
    selected = set(keys)

    # 1) Si piden "solo los del repo": intersección con nombres del repo
    if FILTER_REPO_ONLY:
        repo_names = load_repo_job_names()
        repo_norm = {normalize(n) for n in repo_names}
        selected = {k for k in selected if normalize(k) in repo_norm}

    # 2) Filtros por nombre (union de coincidencias específicas)
    direct_selected = set()

    if JOB_NAME:
        direct_selected |= {k for k in keys if normalize(k) == normalize(JOB_NAME)}

    if JOB_NAME_CONTAINS:
        sub = normalize(JOB_NAME_CONTAINS)
        direct_selected |= {k for k in keys if sub in normalize(k)}

    if JOB_NAME_REGEX:
        flags = re.I if IGNORE_CASE else 0
        rx = re.compile(JOB_NAME_REGEX, flags=flags)
        direct_selected |= {k for k in keys if rx.search(k)}

    if JOB_NAME_LIST:
        wanted = {normalize(x.strip()) for x in JOB_NAME_LIST.split(",") if x.strip()}
        direct_selected |= {k for k in keys if normalize(k) in wanted}

    if direct_selected:
        # Combina: si ya restringiste por repo, intersecta; si no, usa direct_selected
        selected = selected & direct_selected if FILTER_REPO_ONLY else direct_selected

    # Construye mapping filtrado
    return {k: mapping_all[k] for k in keys if k in selected}

def main():
    # 1) Obtener jobs
    data = list_all_jobs()

    # 2) Guardar tmp
    tmp_dir = os.getenv("RUNNER_TEMP") or tempfile.gettempdir()
    joblist_path = os.path.join(tmp_dir, "joblist.tmp")
    with open(joblist_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print("\n================== joblist.tmp (raw) ==================")
    pretty_print_preview(data, PREVIEW_LINES)
    print("=======================================================\n")

    # 3) Mapping completo (todos)
    mapping_all = {}
    for j in data.get("jobs") or []:
        job_id = j.get("job_id")
        name = (j.get("settings") or {}).get("name") or j.get("name")
        if name and job_id:
            mapping_all[str(name)] = job_id

    # (Opcional) escribe el full mapping para auditar
    if WRITE_FULL_MAPPING:
        ensure_dir(OUTPUT_FULL_PATH)
        with open(OUTPUT_FULL_PATH, "w", encoding="utf-8") as f:
            json.dump(mapping_all, f, ensure_ascii=False, indent=2)
        print("🗂  Guardado mapeo completo en", OUTPUT_FULL_PATH)

    # 4) Aplicar filtros → mapping filtrado
    mapping = apply_filters(mapping_all)

    # 5) ENFORCE_SINGLE para evitar ejecuciones masivas por accidente
    if ENFORCE_SINGLE and len(mapping) != 1:
        print(f"⛔ ENFORCE_SINGLE activo: el filtro devolvió {len(mapping)} jobs. "
              f"Refina el filtro (JOB_NAME, *_CONTAINS, *_REGEX, JOB_NAME_LIST).", file=sys.stderr)
        sys.exit(1)

    # 6) Escribir mapping filtrado
    ensure_dir(OUTPUT_PATH)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    # 7) Mostrar mapping filtrado
    print("================== .gha/job_ids.json (FILTRADO) =======")
    print(json.dumps(mapping, ensure_ascii=False, indent=2))
    print("=======================================================\n")
    print(f"✅ Generado {OUTPUT_PATH} con {len(mapping)} job(s)")

    # 8) Step Summary
    step_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as sf:
            sf.write("## Job mapping (filtrado)\n")
            if mapping:
                for k, v in mapping.items():
                    sf.write(f"- {k}: {v}\n")
            else:
                sf.write("- (vacío)\n")

    # 9) Limpieza tmp
    if KEEP_JOBLIST:
        print(f"(Conservado tmp en {joblist_path} - KEEP_JOBLIST=true)")
    else:
        try: os.remove(joblist_path)
        except OSError: pass

if __name__ == "__main__":
    main()
