import pika, json, requests
import re, os

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
        self.create_connection()

    def ingest(self, file_path, gateway_ip):
        if not os.path.basename(file_path):
            return f"Error: File '{file_path}' not found."

        file_name = os.path.basename(file_path)
        url = f"http://{gateway_ip}:8000/ingest/"
        
        print(f"[*] Forwarder: Streaming raw payload '{file_name}' to Gateway ({gateway_ip})...")

        try:
            with open(file_path, "rb") as f:
                files = {"file":(file_name, f, "text/plain")}
                response = requests.post(url, files=files)

                if response.status_code == 200:
                    result = response.json()
                    print(f"[+] Success: Transmitted raw payload successfully.")
                    print(f"[+] Server Response: {result['message']} ({result['batches_queued']} batches queued)")
                else:
                    print(f"[!] Gateway Error ({response.status_code}): {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"[!] Network Error: Could not connect to Gateway at {gateway_ip}. Details: {e}")

    def query(self, mode, value, gateway_ip):
        url = f"http://{gateway_ip}:8000/query"
        response = requests(url, params={"mode":mode,"value":value, "qsize": 100})

        if response.status_code == 200:
            results = response.json()

            if mode == "COUNT_KEYWORD":
                print(f"[+] Success: Found {results["count"]} logs with matchin keyword '{value}'")
            
            if mode != "COUNT_KEYWORD":
                for idx, doc in enumerate(results["results"], start=1):
                    print(f"[{idx}].[{doc.get('timestamp')}] {doc.get('hostname')} {doc.get('process')}: {doc.get('message')}")

        else:
            print(f"[!] Query failed: {response.text}")

    def purge(self, gateway_ip):
        url = f"http://{gateway_ip}:8000/purge/"
        response = requests.post(url)

        if response.status_code == 200:
            print(f"[+] Success: Purged all logs from servers.")
        else:
            print(f"[!] Gateway Error ({response.status_code}): {response.text}")

    def process_command(self, command: str):
        pass

def main():
    
    gateway_ip = input("Enter IP of gateway server: ")
    username = input("Username: ")
    password = input("Password: ")

    forwarder = Forwarder(gateway_ip, username, password)

    while True:
        command = input("Enter a command:" )

        command_dict = forwarder.process_command(command)

if __name__ == "__main__":
    main()
        