import requests
import re
import os

SYSLOG_REGEX = re.compile(
    r"""
    ^
    (?P<timestamp>\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})\s+
    (?P<hostname>[\w\.\-]+)\s+
    (?P<process>[\w\.\-/]+)
    (?:\[(?P<pid>\d+)\])?:\s+
    (?P<message>.+)
    $
    """,
    re.VERBOSE
)

class Forwarder:
    def __init__(self, gateway_ip: str, gateway_port: int):
        self.gateway_ip  = gateway_ip
        self.api_enpoint = f"http://{gateway_ip}:{gateway_port}"

    
    def process_command(self, command: str):
        command = command.strip().split()

        if not command:
            print("[!] Forwarder: Invalid Input.")
            return

        cmd = command[0].upper()
        args = command[1:]
        match (cmd, args):
            case ("INGEST", [file_path]):
                self.ingest(file_path)
            case ("QUERY", [mode, *value_parts]) if value_parts:
                value = " ".join(value_parts)
                self.query(mode.upper(), value)
            case ("PURGE", []):
                self.purge()
            case ("EXIT" | "QUIT", []):
                print("[~] Exiting...")
                exit(0)
            case _:
                print("[!] Invalid command format.")
                print("    Available commands:")
                print("    - ingest <file_path>")
                print("    - query <MODE> <value>")
                print("    - purge")
                print("    - exit / quit")


    def validate_file(self, file_path: str) -> bool:
        """ Fast-fail check to ensure the file is non-empty and UTF-8 readable. """
        if not os.path.exists(file_path):
            print(f"[!] Error: File '{file_path}' not found.")
            return False
        
        if not file_path.endswith((".txt",".log")):
            print(f"[!] Error: File type invalid. Only '.txt' and '.log' files are accepted.")
            return False
        
        if os.path.getsize(file_path) == 0:
            print(f"[!] Pre-flight failed: File '{file_path}' is completely empty (0 bytes).")
            return False
        
        try:
            with open(file_path, "r", encoding="utf-8", errors="strict") as f:
                f.read(1024)  # Read first 1KB to test encoding
        except UnicodeDecodeError:
            print(f"[!] Pre-flight failed: File '{file_path}' is binary or corrupt (not valid UTF-8 text).")
            return False
        
        return True
   
    def ingest(self, file_path: str, timeout: tuple[float, float] = (10.0, 300.0)):
        if not self.validate_file(file_path):
            return
        
        try:
            with open(file_path, "rb") as file:
                url = f"{self.api_enpoint}/ingest/"
                file_name = os.path.basename(file_path)
                files = {"file": (file_name, file, "text/plain")}

                print(f"[*] Forwarder: Streaming raw payload '{file_name}' to Gateway ({self.gateway_ip})...")
                res = requests.post(url=url, files=files, timeout=timeout)

                if res.status_code not in (200, 202):
                    print(f"[!] Gateway Error ({res.status_code}): {res.json().get("detail", res.text)}")
                    return
                
                print(f"[+] Success: Transmitted raw payload successfully.")
                res = res.json()
                job_id = res.get("job_id", "N/A")
                msg = res.get("message", "Accepted")
                print(f"[+] Server res: {res.get('message')}")
        except requests.exceptions.Timeout:
            print(f"[!] Timeout Error: Gateway at {self.gateway_ip} took too long to respond.")
        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

    def query(self, mode: str, value: str, qsize: int = 100, timeout: float = 10.0):     
        try:
            url = f"{self.api_enpoint}/query/"
            params = {"mode": mode, "value": value, "qsize": qsize}

            print(f"[*] Forwarder: Sending query '{mode}' to Gateway ({self.gateway_ip})...")
            res = requests.post(url=url, params=params, timeout=timeout)

            if res.status_code not in (200, 202):
                print(f"[!] Gateway Error ({res.status_code}): {res.json().get("detail", res.text)}")
                return
            
            res = res.json()
            if mode == "COUNT_KEYWORD":
                count = res.get("count", 0)
                print(f"[+] Success: Found {count:,} log entry(s) matching keyword '{value}'")
                return

            hits = res.get("results", [])
            total_matches = res.get("total_matches", len(hits))
            returned_count = res.get("returned_count", len(hits))

            if total_matches > returned_count:
                print(f"[+] Displaying {returned_count} of {total_matches:,} total matches for '{value}':")
            else:
                print(f"[+] Found {returned_count} result(s) for '{value}':")
            
            for idx, doc in enumerate(hits, start=1):
                worker    = doc.get("worker", "UNKNOWN_WORKER")
                timestamp = doc.get("timestamp", "N/A")
                hostname  = doc.get("hostname", "UNKNOWN")
                process   = doc.get("process", "UNKNOWN")
                message   = doc.get("message") or doc.get("raw_log", "NO_MESSAGE")

                print(f"{idx}. ({worker[10:]}) [{timestamp}] {hostname} {process}: {message} ")

        except requests.exceptions.Timeout:
            print(f"[!] Timeout Error: Gateway at {self.gateway_ip} failed to respond within {timeout} seconds.")
        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

    def purge(self, timeout: float = 20.0):
        try:
            url = f"{self.api_enpoint}/purge/"

            print(f"[*] Requesting global purge on Gateway ({self.gateway_ip})...")
            res = requests.post(url, timeout=timeout)

            if res.status_code == 423:
                print("[!] Purge Rejected (423 Locked): A purge operation is already active on the Gateway.")
                return

            if res.status_code not in (200, 202):
                print(f"[!] Gateway Error ({res.status_code}): {res.text}")
                return

            res = res.json()
            verified_count = res.get("verified_doc_count",0)
            is_purged = res.get("is_purged", True)

            if is_purged and verified_count == 0:
                print("[+] Success: All RabbitMQ queues and Elasticsearch log entries purged (Verified count: 0).")
            else:
                print(f"[!] Warning: Purge completed, but database still reports {verified_count} document(s).")
        except requests.exceptions.Timeout:
            print(f"[!] Timeout Error: Gateway at {self.gateway_ip} failed to respond within {timeout} seconds.")
        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

def main():
    gateway_ip = "10.13.13.1"
    gateway_ip = input(f"Enter IP of gateway server [{gateway_ip}]: ").strip() or gateway_ip

    forwarder = Forwarder(gateway_ip=gateway_ip, gateway_port=8000)

    while True:
        try:
            command = input("\nEnter a command: ")
            forwarder.process_command(command)
        except KeyboardInterrupt:
            print("\nExiting...")
            break

if __name__ == "__main__":
    main()
