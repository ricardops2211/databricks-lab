#!/usr/bin/env python3
import os, sys, time, json, base64, urllib.request, urllib.parse, urllib.error

HOST = (os.getenv("DATABRICKS_HOST") or "").rstrip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN") or ""
JOB_ID = os.getenv("JOB_ID")               # opcional; si no, lee de .gha/job_ids.json
JOB_IDS_PATH = os.getenv("JOB_IDS_PATH", ".gha/job_ids.json")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "10"))
TAIL_INTERVAL = int(os.getenv("TAIL_INTERVAL", "10"))    # cada cuánto leer DBFS
TIMEOUT_SECONDS = int(os.getenv("TIMEOUT_SECONDS", "7200"))
LOGS_DBFS_BASE = os.getenv("LOGS_DBFS_BASE", "dbfs:/cluster-logs")  # fallback si el run no trae el destino

if not HOST or not TOKEN:
    print("❌ Falta DATABRICKS_HOST o DATABRICKS_TOKEN", file=sys.stderr); sys.exit(2)

print(f"::add-mask::{TOKEN}")
HDRS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(method, path, qs=None, payload=None, raw=False):
    url = HOST + path
    if qs: url += "?" + urllib.parse.urlencode(qs)
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=HDRS, method=method)
    with urllib.request.urlopen(req) as r:
        b = r.read()
        return b if raw else (json.loads(b.decode()) if b else {})

# --- DBFS read con offset (máx 1MB por lectura)
def dbfs_read(path, offset, length=1024*1024):
    resp = api("GET", "/api/2.0/dbfs/read", qs={"path": path, "offset": offset, "length": length})
    data_b64 = resp.get("data", "")
    return base64.b64decode(data_b64) if data_b64 else b""

def get_run(run_id):
    return api("GET", "/api/2.1/jobs/runs/get", qs={"run_id": run_id})

def get_output(run_id):
    return api("GET", "/api/2.1/jobs/runs/get-output", qs={"run_id": run_id})

def run_now(job_id):
    res = api("POST", "/api/2.1/jobs/run-now", payload={"job_id": int(job_id)})
    return res["run_id"]

def guess_logs_base_from_run(info):
    # Intenta sacar el destino real desde el run (si expone el cluster_spec)
    new_cluster = (info.get("cluster_spec") or {}).get("new_cluster") or {}
    clc = new_cluster.get("cluster_log_conf") or {}
    # distintos formatos según plataforma
    for k in ("dbfs", "s3", "volumes"):
        if k in clc:
            dest = clc[k].get("destination") or clc[k].get("path")
            if dest: return dest
    return LOGS_DBFS_BASE

def tail_driver_logs(cluster_id, logs_base, stop_when=None):
    """Lee stdout/stderr con offsets sobre DBFS."""
    paths = {
        "stdout": f"{logs_base.rstrip('/')}/{cluster_id}/driver/stdout",
        "stderr": f"{logs_base.rstrip('/')}/{cluster_id}/driver/stderr"
    }
    offsets = {k: 0 for k in paths}

    last_print = 0
    while True:
        now = time.time()
        if stop_when and stop_when():  # condición de parada (lifecycle TERMINATED)
            # una última pasada para no perder cola
            for name, p in paths.items():
                try:
                    chunk = dbfs_read(p, offsets[name])
                    if chunk:
                        sys.stdout.write(chunk.decode(errors="replace"))
                        offsets[name] += len(chunk)
                except urllib.error.HTTPError:
                    pass
            break

        # throttear
        if now - last_print < TAIL_INTERVAL:
            time.sleep(1)
            continue

        for name, p in paths.items():
            try:
                chunk = dbfs_read(p, offsets[name])
                if chunk:
                    sys.stdout.write(chunk.decode(errors="replace"))
                    offsets[name] += len(chunk)
            except urllib.error.HTTPError:
                # el archivo puede no existir aún
                pass
        sys.stdout.flush()
        last_print = now

def main():
    # 1) Determinar lista de jobs a ejecutar
    jobs = []
    if JOB_ID:
        jobs = [("JOB", int(JOB_ID))]
    else:
        if not os.path.isfile(JOB_IDS_PATH):
            print(f"❌ No existe {JOB_IDS_PATH}.", file=sys.stderr); sys.exit(2)
        mapping = json.load(open(JOB_IDS_PATH, "r", encoding="utf-8"))
        jobs = list(mapping.items())  # [(name, job_id), ...]

    for name, jid in jobs:
        print(f"▶ Ejecutando '{name}' (job_id={jid})")
        run_id = run_now(jid)
        start = time.time()
        cluster_id = None
        life = "PENDING"
        result = None

        def terminated():
            nonlocal life
            return life == "TERMINATED"

        logs_base = None
        # 2) Bucle: poll estado + tail de logs
        while True:
            if time.time() - start > TIMEOUT_SECONDS:
                print(f"⏰ TIMEOUT {TIMEOUT_SECONDS}s en '{name}'", file=sys.stderr)
                sys.exit(1)

            info = get_run(run_id)
            state = info.get("state", {})
            life = state.get("life_cycle_state")
            result = state.get("result_state")
            url = info.get("run_page_url", "")
            if not cluster_id:
                ci = info.get("cluster_instance") or {}
                cluster_id = ci.get("cluster_id")
                if cluster_id and not logs_base:
                    logs_base = guess_logs_base_from_run(info)
                    print(f"🪵 Tailing driver logs desde {logs_base}/{cluster_id}/driver/ (cada {TAIL_INTERVAL}s aprox.)")

            print(f"  estado={life}, resultado={result}")
            sys.stdout.flush()

            # mientras RUNNING: intenta tail
            if cluster_id and logs_base and life in ("PENDING","RUNNING","BLOCKED","QUEUED","TERMINATING"):
                # hace tail hasta el próximo poll (TAIL_INTERVAL controla el ritmo)
                t_end = time.time() + POLL_SECONDS
                while time.time() < t_end and not terminated():
                    tail_driver_logs(cluster_id, logs_base, stop_when=lambda: terminated())
                    time.sleep(1)

            if life == "TERMINATED":
                if result == "SUCCESS":
                    print(f"✅ '{name}' OK → {url}")
                    # último tail para vaciar cola
                    if cluster_id and logs_base:
                        tail_driver_logs(cluster_id, logs_base, stop_when=lambda: True)
                    break
                else:
                    # intenta sacar mensaje de error final
                    try:
                        out = get_output(run_id)
                        print(json.dumps(out, ensure_ascii=False, indent=2)[:2000])
                    except Exception as e:
                        print(f"(No se pudo get-output: {e})", file=sys.stderr)
                    print(f"❌ '{name}' falló ({result}) → {url}", file=sys.stderr)
                    sys.exit(1)

            time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    main()
