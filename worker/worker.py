from flask import Flask, request, jsonify
import re
import requests

app = Flask(__name__)
ELASTICSEARCH_URL = "http://elasticsearch:9200/logs"

SYSLOG_REGEX = re.compile(
    r"""
    ^
    (?P<timestamp>\w{3}\s+\d+\s+\d{2}:\d{2}(?::\d{2})?)\s+
    (?P<hostname>\S+)\s+
    (?P<process>[a-zA-Z0-9_\-]+)
    (?:\[(?P<pid>\d+)\])?:\s+
    (?:(?P<severity>[A-Z]+)\s+)?
    (?P<message>.+)
    $
    """, 
    re.VERBOSE
)



@app.route("/ingest", methods=["POST"])
def ingest():
    raw_logs = request.json.get("logs", [])
    indexed = []
    for line in raw_logs:
        match = SYSLOG_REGEX.match(line.strip())
        if match:
            doc = {
                "timestamp": match.group("timestamp"),
                "hostname": match.group("hostname"),
                "process": match.group("process"),
                "severity": match.group("severity") or "INFO",
                "message": match.group("message"),
            }
            resp = requests.post(f"{ELASTICSEARCH_URL}/_doc", json=doc)
            if resp.status_code == 201:
                indexed.append(doc)
    return jsonify({"indexed": indexed})

@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("q", "")
    es_query = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["hostname", "process", "message", "severity"]
            }
        }
    }
    resp = requests.get(f"{ELASTICSEARCH_URL}/_search", json=es_query)
    return jsonify(resp.json())

# NEW: Worker Purge execution path
@app.route("/purge", methods=["POST"])
def purge():
    resp = requests.delete(ELASTICSEARCH_URL)
    return jsonify({"status": "purged", "es_response": resp.status_code})

# NEW: Direct translation from custom modes to standard Elasticsearch Query DSL
@app.route("/advanced_query", methods=["GET"])
def advanced_query():
    mode = request.args.get("mode", "")
    value = request.args.get("value", "")
    
    # Define exact Elasticsearch query mappings depending on the selected mode
    if mode == "SEARCH_HOST":
        es_filter = {"match": {"hostname": value}}
    elif mode == "SEARCH_DAEMON":
        es_filter = {"match": {"process": value}}
    elif mode == "SEARCH_SEVERITY":
        es_filter = {"match": {"severity": value}}
    elif mode in ["SEARCH_KEYWORD", "COUNT_KEYWORD"]:
        es_filter = {"multi_match": {"query": value, "fields": ["message", "process"]}}
    else:
        return jsonify({"error": "Unknown mode"}), 400

    es_query = {"query": es_filter}
    
    if mode == "COUNT_KEYWORD":
        # Request a document tracking count block directly from Elasticsearch
        resp = requests.get(f"{ELASTICSEARCH_URL}/_count", json=es_query)
        return jsonify({"count": resp.json().get("count", 0)})
    
    resp = requests.get(f"{ELASTICSEARCH_URL}/_search", json=es_query)
    hits = resp.json().get("hits", {}).get("hits", [])
    return jsonify({"results": hits})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6000)