import json
import requests
import re
import os

ELASTICSEARCH_URL = "http://elasticsearch:9200/logs"

DEFAULT_USERNAME = 'rabbituser'
DEFAULT_PASSWORD = 'rabbit1234'

EC2_Gateway_IP = "3.0.107.3"
EC2_Gateway_IP_WG = "10.13.13.1"
EC2_Port = 5672

EXTERNAL_IP = "103.231.240.136"
INTERNAL_IP0 = "10.20.101.43"   # BROKER IP
INTERNAL_IP1 = "10.20.101.44"
INTERNAL_IP2 = "10.20.101.45"

INTERNAL_PORT = 5672
EXTERNAL_PORT0 = 32143
EXTERNAL_PORT1 = 32144
EXTERNAL_PORT2 = 32145

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

class Forwarder:
    def __init__(self, gateway_ip=EC2_Gateway_IP, username=DEFAULT_USERNAME, password=DEFAULT_PASSWORD):
        self.gateway_ip = gateway_ip
        self.username = username
        self.password = password

    def ingest(self, file_path):
        if not os.path.exists(file_path):
            print(f"[!] Error: File '{file_path}' not found.")
            return

        file_name = os.path.basename(file_path)
        url = f"http://{self.gateway_ip}:8000/ingest/"
        
        print(f"[*] Forwarder: Streaming raw payload '{file_name}' to Gateway ({self.gateway_ip})...")

        try:
            with open(file_path, "rb") as f:
                files = {"file": (file_name, f, "text/plain")}
                response = requests.post(url, files=files)

                if response.status_code == 200:
                    result = response.json()
                    print(f"[+] Success: Transmitted raw payload successfully.")
                    print(f"[+] Server Response: {result.get('message')} ({result.get('total_batch_count')} batches queued)")
                else:
                    print(f"[!] Gateway Error ({response.status_code}): {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

    def query(self, mode, value):
        url = f"http://{self.gateway_ip}:8000/query/"
        
        try:
            # FastAPI endpoint was defined as @app.post("/query/")
            response = requests.post(url, params={"mode": mode, "value": value, "qsize": 100})

            if response.status_code == 200:
                results = response.json()

                if mode == "COUNT_KEYWORD":
                    print(f"[+] Success: Found {results.get('count', 0)} logs with matching keyword '{value}'")
                else:
                    hits = results.get("results", [])
                    print(f"[+] Found {len(hits)} results:")
                    for idx, doc in enumerate(hits, start=1):
                        print(f"[{idx}].[{doc.get('timestamp')}] {doc.get('hostname')} {doc.get('process')}: {doc.get('message')}")
            else:
                print(f"[!] Query failed: {response.text}")
                
        except requests.exceptions.RequestException as e:
             print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

    def purge(self):
        url = f"http://{self.gateway_ip}:8000/purge/"
        try:
            response = requests.post(url)
            if response.status_code == 200:
                print(f"[+] Success: Purged all logs from servers.")
            else:
                print(f"[!] Gateway Error ({response.status_code}): {response.text}")
        except requests.exceptions.RequestException as e:
             print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

    def process_command(self, command: str):
        parts = command.strip().split()
        if not parts:
            return

        cmd = parts[0].lower()

        if cmd == "ingest" and len(parts) == 2:
            self.ingest(parts[1])
        elif cmd == "query" and len(parts) >= 3:
            mode = parts[1].upper()
            value = " ".join(parts[2:])
            self.query(mode, value)
        elif cmd == "purge":
            self.purge()
        elif cmd in ["exit", "quit"]:
            print("Exiting...")
            exit(0)
        else:
            print("[!] Invalid command format.")
            print("    Available commands:")
            print("    - ingest <file_path>")
            print("    - query <MODE> <value>")
            print("    - purge")
            print("    - exit")

def main():
    gateway_ip = input(f"Enter IP of gateway server [{EC2_Gateway_IP}]: ").strip() or EC2_Gateway_IP
    username = input(f"Username [{DEFAULT_USERNAME}]: ").strip() or DEFAULT_USERNAME
    password = input(f"Password [{DEFAULT_PASSWORD}]: ").strip() or DEFAULT_PASSWORD

    forwarder = Forwarder(gateway_ip, username, password)

    while True:
        try:
            command = input("\nEnter a command: ")
            forwarder.process_command(command)
        except KeyboardInterrupt:
            print("\nExiting...")
            break

if __name__ == "__main__":
    main()
