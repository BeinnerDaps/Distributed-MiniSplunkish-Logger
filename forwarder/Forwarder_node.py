import calendar
from datetime import datetime
import os
import re
import requests
import time

GATEWAY_IP = "10.13.13.1"

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
        self.valid_modes = ["SEARCH_JOB","SEARCH_DATE","SEARCH_HOST","SEARCH_DAEMON","SEARCH_SEVERITY","SEARCH_KEYWORD","COUNT_KEYWORD","COUNT_ES_NODES"]

    def process_command(self, command: str):
        def _help():
            print(f"")
            print(f" -> commands:")
            print(f"   - ingest <file_path>")
            print(f"   - query <mode*> <value*>")
            print(f"   - purge")
            print(f"   - exit / quit")
            print(f"")
            print(f" -> mode*:")
            for mode in self.valid_modes:
                print(f"   - {mode}")
            print(f"")
            print(f" -> value*:")
            print(f"    - date format")
            print(f"       MMM")
            print(f"       MMM DD")
            print(f"       MMM DD HH")
            print(f"       MMM DD HH:MM")
            print(f"       MMM DD HH:MM:SS")
            print(f"    - date range")
            print(f"       MMM DD HH:MM:SS-MMM DD HH:MM:SS")
            print(f"       MMM DD-MMM DD")
            print(f"       MMM-MMM")
            print(f"    - severities")
            print(f"       INFO WARN ERROR")

        command = command.split()
        if not command:
            print("[!] Forwarder: Invalid Input.")
            return

        cmd = command[0].upper()
        args = command[1:]
        match (cmd, args):
            case ("INGEST", [file_path]):
                self.ingest(file_path)
            case ("QUERY", [mode, *value_parts]):
                value = " ".join(value_parts)
                self.query(mode.upper(), value)
            case ("PURGE", []):
                self.purge()
            case ("EXIT" | "QUIT", []):
                print("[~] Exiting...")
                exit(0)     
            case ("HELP" | "H", []):
                _help()
            case _:
                print("[!] Invalid command format.")
                print(f"-> Type 'help' for list of commands.")
   
    def ingest(self, file_path: str, timeout: tuple[float, float] = (10.0, 300.0)):
        if not self._validate_file(file_path):
            return
        
        url = f"{self.api_enpoint}/ingest/"
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        try:
            with open(file_path, "rb") as file:
                files = {"file": (file_name, file, "text/plain")}

                start_time = time.time()
                print(f"[*] Streaming '{file_name}' logs to Gateway ({self.gateway_ip})...")
                res = requests.post(url=url, files=files, timeout=timeout)

                if res.status_code not in (200, 202):
                    print(f"[!] Gateway Error ({res.status_code}): {res.json().get("detail", res.text)}")
                    return

                print(f"[+] Success: Transmitted {file_size} bytes successfully. (Took {time.time()-start_time:.2f}s)")
                job_id = res.json().get("job_id", "N/A")
                msg = res.json().get("message", "N/A")
                print(f"[+] Server response: ({job_id}) {msg} ")
        except requests.exceptions.Timeout:
            print(f"[!] Timeout Error: Gateway at {self.gateway_ip} took too long to respond.")
        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: Could not connect to Gateway at {self.gateway_ip}. Details: {e}")

    def query(self, mode: str, value: str, qsize: int = 100, timeout: float = 10.0):    
        if mode not in self.valid_modes:
            print(f"[!] Client Error: Invalid query mode '{mode}'")
            return

        url = f"{self.api_enpoint}/query/"
        try:
            if mode == "SEARCH_DATE":
                value = self._parse_dates(value)
                
            print(f"[*] Sending query '{mode}' to Gateway ({self.gateway_ip})...")
            res = requests.post(url=url, params={"mode": mode, "value": value, "qsize": qsize}, timeout=timeout)

            if res.status_code not in (200, 202):
                print(f"[!] Gateway Error ({res.status_code}): {res.json().get("detail", res.text)}")
                return
            
            res = res.json()

            if mode == "COUNT_ES_NODES":
                total_nodes = res.get("total_nodes", -1)
                data_nodes = res.get("data_nodes", -1)
                cluster_status = res.get("cluster_status", "error")
                print(f"[+] Status: {cluster_status.upper()} | Active: {total_nodes} | Holding data: {data_nodes}")
                return
            
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

    def _validate_file(self, file_path: str) -> bool:
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

    def _parse_dates(self, value: str) -> str:
        """Normalizes 'Feb 18', 'Feb', or full timestamps into ES-compliant date strings."""
        def parse_date_boundary(val: str, is_end: bool = False):
            val = " ".join(val.strip().split())
            year = datetime.now().year

            # 1. Full timestamp already provided (e.g., "Feb 18 13:18:41")
            try:
                datetime.strptime(f"{year} {val}", f"%Y %b %d %H:%M:%S")
                return val
            except ValueError:
                pass

            # 2. Date with Day (e.g., "Feb 18" or "Feb 8")
            try:
                dt = datetime.strptime(f"{year} {val}", f"%Y %b %d")
                time_part = "23:59:59" if is_end else "00:00:00"
                return f"{dt.strftime('%b %d')} {time_part}"
            except ValueError:
                pass

            # 3. Month only (e.g., "Feb" or "Mar")
            try:
                dt = datetime.strptime(f"{year} {val}", f"%Y %b")
                month_str = dt.strftime("%b")
                if is_end:
                    last_day = calendar.monthrange(year, dt.month)[1]
                    return f"{month_str} {last_day:02d} 23:59:59"
                else:
                    return f"{month_str} 01 00:00:00"
            except ValueError:
                return val

        # --- Search Handler ---
        parts = value.split("-")

        start_raw = parts[0]
        end_raw = parts[1] if len(parts) > 1 else parts[0]

        start_date = parse_date_boundary(start_raw.strip(), is_end=False)
        end_date = parse_date_boundary(end_raw.strip(), is_end=True)

        return f"{start_date}-{end_date}"

def main():
    gateway_ip = input(f"Enter Gateway IP [{GATEWAY_IP}]: ").strip() or GATEWAY_IP

    forwarder = Forwarder(gateway_ip=gateway_ip, gateway_port=8000)
    print(f"-> Type 'help' for list of commands.")

    while True:
        try:
            command = input("\nEnter a command: ")
            forwarder.process_command(command.strip())
        except KeyboardInterrupt:
            print("\nExiting...")
            break

if __name__ == "__main__":
    main()
