#!/usr/bin/env python3
# scripts/databricks_run_jobs.py
import os, sys, json, time, urllib.request, urllib.error, urllib.parse

HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_IDS_PATH = os.getenv("JOB_IDS_PATH", ".gha/job_ids.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "3600"))  # 60 min
CONTINUE_ON_FAILURE = os.getenv("CONTINUE_ON_FAILURE", "false").lower() in ("1","true","yes")

if not HOST or not TOKEN:
    print("❌ Falta DATABRICKS_HOST o DATABRICKS_TOKEN en el entorno.", file=sys.stderr)
    sys.exit(2)

# Enmascara el token en logs
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

def load_job_ids() -> dict:
    if not os.path.isfile(JOB_IDS_PATH):
        print(f"❌ No existe '{JOB_IDS_PATH}'. Ejecuta primero el UPSERT.", file=sys.stderr)
        sys.exit(2)
    with open(JOB_IDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def run_and_wait(name: str, job_id: int) -> tuple[bool, str]:
    start = time.time()
    res = api("POST", "/api/2.1/jobs/run-now", payload={"job_id": job_id})
    run_id = res["run_id"]
    print(f"▶ Ejecutando '{name}' (job_id={job_id}, run_id={run_id})")

    while True:
        if time.time() - start > TIMEOUT_SECONDS:
            print(f"⏰ TIMEOUT ({TIMEOUT_SECONDS}s) para '{name}'", file=sys.stderr)
            return False, "(timeout)"
        time.sleep(POLL_SECONDS)

        info = api("GET", "/api/2.1/jobs/runs/get", qs={"run_id": run_id})
        life = info["state"]["life_cycle_state"]
        result = info["state"].get("result_state")
        url = info.get("run_page_url", "")
        print(f"  estado={life}, resultado={result}")

        if life == "TERMINATED":
            if result == "SUCCESS":
                print(f"✅ '{name}' OK → {url}")
                return True, url
            else:
                try:
                    out = api("GET", "/api/2.1/jobs/runs/get-output", qs={"run_id": run_id})
                    preview = json.dumps(out, ensure_ascii=False, indent=2)
                    print(preview[:2000])
                except Exception as e:
                    print(f"(No se pudo obtener get-output: {e})", file=sys.stderr)
                print(f"❌ '{name}' falló ({result}) → {url}", file=sys.stderr)
                return False, url


def main():
    job_ids = load_job_ids()
    if not job_ids:
        print("ℹ️ No hay jobs en el mapping; nada que ejecutar.")
        return

    summary_lines = []
    failures = 0

    for name, jid in job_ids.items():
        ok, url = run_and_wait(name, jid)
        bullet = f"- {'✅' if ok else '❌'} **{name}**"
        if url:
            bullet += f": [Run]({url})"
        summary_lines.append(bullet)
        if not ok:
            failures += 1
            if not CONTINUE_ON_FAILURE:
                break

    # Publica resumen en Step Summary
    step_summary = os.getenv("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write("## Databricks runs\n")
            for line in summary_lines:
                f.write(line + "\n")

    if failures:
        sys.exit(1)

if __name__ == "__main__":
    main()
