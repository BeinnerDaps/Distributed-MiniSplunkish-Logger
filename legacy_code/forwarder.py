import requests, sys

def ingest(file_path, gateway_ip):
    with open(file_path) as f:
        logs = [line.strip() for line in f if line.strip()]

    resp = requests.post(
        f"http://{gateway_ip}:5000/ingest",
        json={"logs": logs}
    )

    try:
        print(resp.json())
    except ValueError:
        print("Non‑JSON response:", resp.text)

if __name__ == "__main__":
    ingest(sys.argv[1], sys.argv[2])
