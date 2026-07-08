from flask import Flask, request, jsonify
import requests
import os
import datetime
import csv

app = Flask(__name__)
WORKERS = [
    "http://10.20.101.44:6000",
    "http://10.20.101.45:6000"
]

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def log_event(message: str):
    today = datetime.date.today().isoformat()
    log_file = os.path.join(LOG_DIR, f"{today}.log")
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = f"[{timestamp}] {message}\n"
    with open(log_file, "a") as f:
        f.write(entry)
    print(entry, end="")

def log_csv_row(row: dict):
    today = datetime.date.today().isoformat()
    log_file = os.path.join(LOG_DIR, f"{today}.csv")
    file_exists = os.path.isfile(log_file)
    with open(log_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp","hostname","process","message", "severity"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

@app.route("/ingest", methods=["POST"])
def ingest():
    payload = request.get_json(force=True)
    logs = payload.get("logs", [])
    if not logs:
        return jsonify({"error": "No logs provided"}), 400

    for i, line in enumerate(logs):
        worker = WORKERS[i % len(WORKERS)]
        try:
            requests.post(f"{worker}/ingest", json={"logs": [line]}, timeout=5)
        except Exception as e:
            log_event(f"[Gateway] Ingest target down {worker}: {e}")
    return jsonify({"status": "ingestion complete"}), 200

@app.route("/search", methods=["GET"])
def search():
    q = request.args.get("q", "")
    date_filter = request.args.get("date", None)
    if not q:
        return jsonify({"error": "Missing query parameter"}), 400

    results = []
    for worker in WORKERS:
        try:
            resp = requests.get(f"{worker}/search", params={"q": q}, timeout=5)
            if resp.status_code == 200:
                hits = resp.json().get("hits", {}).get("hits", [])
                if date_filter:
                    hits = [h for h in hits if date_filter in h.get("_source", {}).get("timestamp", "")]
                
                for h in hits:
                    src = h.get("_source", h)
                    log_csv_row({
                        "timestamp": src.get("timestamp",""),
                        "hostname": src.get("hostname",""),
                        "process": src.get("process",""),
                        "severity": src.get("severity",""),
                        "message": src.get("message","")
                    })
                results.extend(hits)
        except Exception as e:
            log_event(f"Worker failure: {e}")
    return jsonify({"results": results}), 200

# NEW: Purge route to delete elasticsearch indices via workers
@app.route("/purge", methods=["POST"])
def purge():
    log_event("[Gateway] Purge request received. Flushing cluster datastores...")
    for worker in WORKERS:
        try:
            requests.post(f"{worker}/purge", timeout=5)
        except Exception as e:
            log_event(f"Failed to purge {worker}: {e}")
    return jsonify({"status": "Purge signals transmitted successfully"}), 200

# NEW: Query routing architecture matching your advanced parameters
@app.route("/query", methods=["GET"])
def query():
    mode = request.args.get("mode", "")
    value = request.args.get("value", "")
    if not mode or not value:
        return jsonify({"error": "Missing mode or value parameters"}), 400

    aggregated_results = []
    total_count = 0

    for worker in WORKERS:
        try:
            resp = requests.get(f"{worker}/advanced_query", params={"mode": mode, "value": value}, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                if mode == "COUNT_KEYWORD":
                    total_count += data.get("count", 0)
                else:
                    aggregated_results.extend(data.get("results", []))
        except Exception as e:
            log_event(f"Advanced query failed on worker {worker}: {e}")

    if mode == "COUNT_KEYWORD":
        return jsonify({"mode": "COUNT_KEYWORD", "keyword": value, "count": total_count}), 200
    
    return jsonify({"mode": mode, "results": aggregated_results}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)