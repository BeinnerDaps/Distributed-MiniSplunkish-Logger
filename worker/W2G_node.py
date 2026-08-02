import hashlib
import json
import os
import pika 
import re
import threading
import time
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

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
    def __init__(self, srvr_name: str, username: str , password: str, num_threads: int,
        gateway_ip: str, es_cluster: list[str], index_name: str, queue_name: str
    ):
        self.srvr_name      = srvr_name
        self.username       = username
        self.password       = password
        self.gateway_ip     = gateway_ip
        self.num_threads    = num_threads
        self.index_name     = index_name
        self.queue_name     = queue_name
        self.es_cluster     = es_cluster
        self.es             = Elasticsearch(hosts=es_cluster)

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
            if self.es.indices.exists(index=self.index_name):
                print(f" [*] Worker: Index '{self.index_name}' already online in cluster.")
                return

            index_body = {
                "settings": {
                    "number_of_shards": len(self.es_cluster),
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
            self.es.indices.create(index=self.index_name, body=index_body)
        except Exception as e:
            print(f" [!] Error initializing index: {e}")

    def _ingest_batch_thread_worker(self, thread_id: int):
        print(f" [*] {self.srvr_name} thread-{thread_id}: connecting to RabbitMQ broker at {self.gateway_ip}...")
        while True:
            try:
                # Thread-isolated connection and channel
                credentials = pika.PlainCredentials(self.username, self.password)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=self.gateway_ip,
                        port=5672, 
                        virtual_host='/',
                        credentials=credentials,
                        heartbeat=600,
                        blocked_connection_timeout=300
                    )
                )
                
                channel = connection.channel()
                channel.queue_declare(queue=self.queue_name)
                print(f" [*] Central Server: {self.gateway_ip} Ready. Awaiting log requests...") 

                def message_callback(ch, method, properties, body):
                    try:
                        payload  = json.loads(body.decode('utf-8'))
                        batch_id = payload.get("batch_id", "fallback")
                        raw_logs = payload.get("logs_batch", [])

                        if not raw_logs:
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                            return

                        # Assign batch IDs to logs to prevent duplicates (overwrite logs with same batch_id)
                        actions = []
                        for idx, raw_line in enumerate(raw_logs):
                            actions.append({
                                "_op_type": "index",
                                "_index":   self.index_name,
                                "_id":      self._generate_doc_id(batch_id, idx),
                                "_source":  self.parse_logs(raw_line.strip())
                            })

                        # Bulk write to Elasticsearch
                        success_count, _ = bulk(self.es, actions, stats_only=False)

                        # ACK message only after successful ES bulk write
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                        print(f" [Thread-{thread_id}] Indexed {success_count} logs for Batch: {batch_id[:8]}... (ACKed)")
                    except Exception as e:
                        print(f" [!] Error in {self.srvr_name} thread-{thread_id} batch {batch_id[:8]}: {e}. Requeuing...")
                        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=self.queue_name, on_message_callback=message_callback)
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
    worker = Worker(
        server_name = os.getenv("SERVER_NAME", "default_name"), 
        username    = os.getenv("USERNAME",    "default_usr"), 
        password    = os.getenv("PASSWORD",    "default_pwd"), 
        gateway_ip  = os.getenv("GATEWAY_IP",  "127.0.0.1"),
        es_cluster  = os.getenv("ES_CLUSTER",  "http://localhost:9200").split(","),
        index_name  = os.getenv("INDEX_NAME",  "default_idx"),
        queue_name  = os.getenv("QUEUE_NAME",  "default_que"),
        num_threads = os.getenv("NUM_THREADS", 0),
    )
    worker.start()
    