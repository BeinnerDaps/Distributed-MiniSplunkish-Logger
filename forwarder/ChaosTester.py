import os
import time
import random
import threading
import requests

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

# Global event to signal ingestion thread has finished, stops other queries
INGEST_FINISHED = threading.Event()
# Lock to prevent console print overlapping from multiple threads
PRINT_LOCK = threading.Lock()

# Query pool, random selection
QUERY_POOL = [
    ("SEARCH_SEVERITY", "WARN"),
    ("SEARCH_SEVERITY", "ERROR"),
    ("SEARCH_SEVERITY", "INFO"),
    ("SEARCH_DAEMON", "systemd"),
    ("SEARCH_DAEMON", "sshd"),
    ("SEARCH_KEYWORD", "206.123.145.57"),
    ("SEARCH_KEYWORD", "dpkg"),
    ("SEARCH_KEYWORD", "logrotate"),
    ("COUNT_KEYWORD", "root"),
    ("COUNT_KEYWORD", "user"),
    ("COUNT_KEYWORD", "session opened"),
    ("COUNT_KEYWORD", "session closed "),
    
]

def safe_print(message):
    """Thread-safe print function."""
    with PRINT_LOCK:
        print(message)

def ingest_task(file_path, gateway_ip):
    """Thread target for ingesting the log file."""
    url = f"http://{gateway_ip}:8000/ingest/"
    file_name = os.path.basename(file_path)
    
    safe_print(f"[*] [INGEST] Starting file upload for '{file_name}' to {url}...")
    start_time = time.time()
    
    try:
        with open(file_path, "rb") as f:
            files = {"file": (file_name, f, "text/plain")}
            response = requests.post(url, files=files)
            
            elapsed = time.time() - start_time
            if response.status_code in (200, 202):
                result = response.json()
                safe_print(f"[+] [INGEST] SUCCESS: {result.get('message')} (Took {elapsed:.2f}s)")
            else:
                safe_print(f"[!] [INGEST] FAILED with status {response.status_code}: {response.text}")
        time.sleep(30) 
    except Exception as e:
        safe_print(f"[!] [INGEST] CRITICAL EXCEPTION: {e}")
    finally:
        # Signal all query threads to stop, regardless of success or failure
        INGEST_FINISHED.set()

def query_task(thread_id, gateway_ip):

    url = f"http://{gateway_ip}:8000/query/"
    request_count = 0
    success_count = 0
    
    safe_print(f"[*] [QUERY-{thread_id}] Started and awaiting ingestion completion...")
    
    while not INGEST_FINISHED.is_set():
        mode, value = random.choice(QUERY_POOL)
        params = {"mode": mode, "value": value, "qsize": 50}
        
        try:
            # Send the request (Based on new FastAPI gateway)
            resp = requests.post(url, params=params, timeout=5.0)
            request_count += 1
            
            if resp.status_code in (200, 202):
                success_count += 1
                resp = resp.json()
                count = resp.get("total_matches", 0)
                safe_print(f"    [QUERY-{thread_id}] OK | {mode}:{value} | Hits: {count}")
            else:
                safe_print(f"    [QUERY-{thread_id}] HTTP {resp.status_code} | {mode}:{value}")
                
        except requests.exceptions.RequestException as e:
            safe_print(f"    [QUERY-{thread_id}] ERR | {mode}:{value} | {type(e).__name__}")
            
        # Delay to prevent overwhelming, you can play with this
        # remove or decrease to 0 for most chaos
        time.sleep(0.1) 
        
    safe_print(f"[*] [QUERY-{thread_id}] Shutting down. Executed {request_count} queries ({success_count} successful).")

def main():
    print("--- Chaos Tester Script ---")
    gateway_ip = input(f"Enter IP of gateway server [{EC2_Gateway_IP_WG}]: ").strip() or EC2_Gateway_IP_WG

    file_path = ""
    while not os.path.exists(file_path):
        file_path = input("Enter path to log file for ingestion: ").strip()
        if not os.path.exists(file_path):
            print("[!] File does not exist. Please try again.")

    # Below does not include that 1 thread for ingest, thats inherently made by default
    try:
        num_query_threads = int(input("Enter number of concurrent query threads [5]: ").strip() or 5)
    except ValueError:
        num_query_threads = 5

    print(f"\n[*] Initializing Chaos Test with 1 Ingest Thread and {num_query_threads} Query Threads...")
    time.sleep(1)

    # Initialize Threads
    ingest_thread = threading.Thread(target=ingest_task, args=(file_path, gateway_ip))
    query_threads = []
    
    for i in range(num_query_threads):
        t = threading.Thread(target=query_task, args=(i+1, gateway_ip))
        query_threads.append(t)
        t.start()
        
    # Start ingest thread slightly after queries so the system is already under load pre-ingest
    time.sleep(0.5) 
    ingest_thread.start()

    # Wait for the ingest thread to finish
    ingest_thread.join()
    
    # Wait for all query threads to wrap up
    for t in query_threads:
        t.join()

    print("\n[+] Chaos test completed successfully.")

if __name__ == "__main__":
    main()