#!/usr/bin/env python3
# scripts/databricks_upsert_jobs.py
import os, sys, json, glob, urllib.request, urllib.error, urllib.parse

HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN") or ""
JOBS_DIR = os.getenv("JOBS_DIR", "jobs")
JOB_IDS_PATH = os.getenv("JOB_IDS_PATH", ".gha/job_ids.json")

if not HOST or not TOKEN:
    print("❌ Falta DATABRICKS_HOST o DATABRICKS_TOKEN en el entorno.", file=sys.stderr)
    sys.exit(2)

# Enmascara el token en logs de GitHub Actions
print(f"::add-mask::{TOKEN}")

HDRS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(method: str, path: str, qs: dict | None = None, payload: dict | None = None) -> dict:
    url = HOST + path
    if qs:
        url += "?" + urllib.parse.urlencode(qs)
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=HDRS, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            b = r.read().decode()
            return json.loads(b) if b else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        print(f"HTTP {e.code} {e.reason} @ {url}\n{body}", file=sys.stderr)
        raise

def list_jobs_all() -> list[dict]:
    jobs, page_token = [], None
    while True:
        qs = {"limit": 100}
        if page_token: qs["page_token"] = page_token
        resp = api("GET", "/api/2.1/jobs/list", qs)
        jobs.extend(resp.get("jobs", []))
        page_token = resp.get("next_page_token")
        if not page_token:
            break
    return jobs

def main():
    if not os.path.isdir(JOBS_DIR):
        print(f"ℹ️ Carpeta '{JOBS_DIR}' no existe; nada que upsertear.")
        os.makedirs(os.path.dirname(JOB_IDS_PATH) or ".", exist_ok=True)
        with open(JOB_IDS_PATH, "w", encoding="utf-8") as f:
            json.dump({}, f)
        return

    # Índice de jobs existentes por nombre
    existing = {j["settings"]["name"]: j["job_id"] for j in list_jobs_all()}
    print(f"🔎 Jobs actuales en el workspace: {len(existing)}")

    job_ids = {}
    changed = []

    for fp in sorted(glob.glob(os.path.join(JOBS_DIR, "*.json"))):
        with open(fp, "r", encoding="utf-8-sig") as f:
            spec = json.load(f)
        name = spec.get("name")
        if not name:
            print(f"⚠️ '{fp}' no tiene campo 'name'; se omite.", file=sys.stderr)
            continue

        if name in existing:
            # reset (update in place)
            payload = {"job_id": existing[name], "new_settings": spec}
            api("POST", "/api/2.1/jobs/reset", payload=payload)
            job_ids[name] = existing[name]
            changed.append(f"reset  {name} -> {existing[name]}")
        else:
            res = api("POST", "/api/2.1/jobs/create", payload=spec)
            job_ids[name] = res["job_id"]
            changed.append(f"create {name} -> {res['job_id']}")

    os.makedirs(os.path.dirname(JOB_IDS_PATH) or ".", exist_ok=True)
    with open(JOB_IDS_PATH, "w", encoding="utf-8") as f:
        json.dump(job_ids, f, ensure_ascii=False, indent=2)

    print("✅ UPSERT completado. Jobs procesados:")
    for line in changed:
        print("  •", line)

    # Resumen bonito en Step Summary (si aplica)
    step_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write("## Databricks Jobs creados/actualizados\n")
            if changed:
                for line in changed:
                    f.write(f"- {line}\n")
            else:
                f.write("- (sin cambios)\n")

if __name__ == "__main__":
    main()
