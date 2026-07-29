import hashlib
import json
import pika 
import re
import requests
import threading
import time
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

username    = 'rabbituser'
password    = 'rabbit1234'
num_threads = 4

GTWY_PUB_IP = "3.0.107.3"
SVRS_PUB_IP = "103.231.240.136"

GTWY_VPN_IP = "10.13.13.1"
SVR0_VPN_IP = "10.13.13.7"
SVR1_VPN_IP = "10.13.13.8"
SVR2_VPN_IP = "10.13.13.9"


RABBITMQ_IP = GTWY_VPN_IP

ES_CLUSTER_NODES = [
    f'http://{SVR0_VPN_IP}:9200',
    f'http://{SVR1_VPN_IP}:9200',
    f'http://{SVR2_VPN_IP}:9200'
]

INDEX_NAME = 'distributed-logs'
QUEUE_NAME = 'log_ingest_queue'

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

class Worker:
    def __init__(self, srvr_name: str, username: str , password: str, num_threads: int):
        self.srvr_name      = srvr_name
        self.username       = username
        self.password       = password
        self.num_threads    = num_threads
        self.es             = Elasticsearch(hosts=ES_CLUSTER_NODES)
    
        print("-- Distributed Event Logger initialized. --") 

    def start(self):
        self.initialize_distributed_index()
        threads = []
        
        print(f" [*] Launching Worker Node with {self.num_threads} concurrent consumer threads...")
        for i in range(1, self.num_threads+1):
            t = threading.Thread(
                target=self._ingest_batch_thread_worker,
                args=(i,),
                daemon=True
            )
            t.start()
            threads.append(t)

        # Keep main thread active
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n [!] Stopping worker threads...")
            for t in threads:
                t.join()

    def initialize_distributed_index(self):
        try:
            if self.es.indices.exists(index=INDEX_NAME):
                print(f" [*] Worker: Index '{INDEX_NAME}' already online in cluster.")
                return

            index_body = {
                "settings": {
                    "number_of_shards": len(ES_CLUSTER_NODES),
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "timestamp": {"type": "date", "fields": {"keyword": {"type": "keyword"}}},
                        "hostname":  {"type": "keyword"},
                        "process":   {"type": "keyword"},
                        "severity":  {"type": "keyword"},
                        "message":   {"type": "text"},
                        "raw":       {"type": "text"}
                    }
                }
            }
            self.es.indices.create(index=INDEX_NAME, body=index_body)
        except Exception as e:
            print(f" [!] Error initializing index: {e}")

    def _ingest_batch_thread_worker(self, thread_id: int):
        print(f" [*] {self.srvr_name} thread-{thread_id}: connecting to RabbitMQ broker at {RABBITMQ_IP}...")
        while True:
            try:
                # Thread-isolated connection and channel
                credentials = pika.PlainCredentials(self.username, self.password)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=RABBITMQ_IP,
                        port=5672, 
                        virtual_host='/',
                        credentials=credentials,
                        heartbeat=600,
                        blocked_connection_timeout=300
                    )
                )
                channel = connection.channel()
                channel.queue_declare(queue=QUEUE_NAME)
                print(f" [*] Central Server: {RABBITMQ_IP} Ready. Awaiting log requests...") 

                def message_callback(ch, method, properties, body):
                    batch_id = "unknown"
                    try:
                        payload = json.loads(body.decode('utf-8'))
                        batch_id = payload.get("batch_id", "fallback")
                        raw_logs = payload.get("logs_batch", [])

                        if not raw_logs:
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                            return

                        actions = []
                        for idx, raw_line in enumerate(raw_logs):
                            doc_id = self._generate_doc_id(batch_id, idx)
                            doc = self.parse_logs(raw_line.strip()) 
                            actions.append({
                                "_op_type": "index",
                                "_index": self.index_name,
                                "_id": doc_id,
                                "_source": doc
                            })

                        # 1. Bulk write to Elasticsearch
                        success_count, _ = bulk(self.es, actions, stats_only=False)

                        # 2. ACK message only after successful ES bulk write
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        print(f" [Thread-{thread_id}] Indexed {success_count} logs for Batch: {batch_id[:8]}... (ACKed)")

                    except Exception as e:
                        print(f" [!] Error in {self.srvr_name} thread-{thread_id} batch {batch_id[:8]}: {e}. Requeuing...")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=QUEUE_NAME, on_message_callback=message_callback)
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as conn_err:
                print(f" [!] RabbitMQ connection lost ({conn_err}). Retrying in 5 seconds...")
                time.sleep(5)
            except Exception as unhandled_err:
                print(f" [!] Unexpected worker exception: {unhandled_err}. Restarting loop in 5 seconds...")
                time.sleep(5)

    def parse_logs(self, line: str) -> dict:                
        doc = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hostname": "UNPARSED",
            "process": "UNPARSED",
            "severity": "UNKNOWN",
            "message": "Log failed regex parsing",
            "raw": line
        }

        match = SYSLOG_REGEX.match(line)
        if match:
            doc.update({
                "timestamp":    match.group("timestamp"),
                "hostname":     match.group("hostname"),
                "process":      match.group("process"),
                "severity":     match.group("severity"),
                "message":      match.group("message"),
            })

        return doc

    def _generate_doc_id(self, batch_id: str, line_index: int) -> str:
        """Generates deterministic SHA-256 document ID for idempotency."""
        return hashlib.sha256(f"{batch_id}_{line_index}".encode("utf-8")).hexdigest()


if __name__ == "__main__":

    server_name = input(f"Enter name of server: ")
    username    = input(f"Enter RabbitMQ username ['{username}']: ")
    password    = input(f"Enter RabbitMQ password ['{password}']: ")
    num_threads = input(f"Enter Number of threads ['{num_threads}']: ")

    worker = Worker(
        server_name=server_name, 
        username=username, 
        password=password, 
        num_threads=num_threads
    )
    worker.start()


    