import hashlib
import json
import os
import pika 
import re
import signal
import threading
import time
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import BadRequestError

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
    def __init__(self, server_name: str, username: str , password: str, num_threads: int,
        gateway_ip: str, es_cluster: list[str], index_name: str, queue_name: str 
    ):
        # Metadata
        self.server_name    = server_name
        self.username       = username
        self.password       = password

        # Threads
        self.num_threads    = num_threads
        self.shutdown_event = threading.Event()
        self.channel_lock   = threading.Lock()
        self.open_channels  = []
        
        # Worker-Gateway variables
        self.gateway_ip     = gateway_ip
        self.es_cluster     = es_cluster
        self.index_name     = index_name.lower()
        self.queue_name     = queue_name

        self.es             = Elasticsearch(hosts=es_cluster)

        print(f"-- Distributed Event Logger initialized: [{self.server_name}] --") 

    def start(self):
        # Fail-fast: Do not spawn consumers, halts startup if ES index initialization fails
        if not self.initialize_distributed_index():
            print(" [!] Aborting startup: Distributed index setup failed.")
            return

        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

        print(f" [*] Launching Worker Node with {self.num_threads} concurrent consumer threads...")

        threads = []
        for i in range(1, self.num_threads+1):
            t = threading.Thread(
                target=self._ingest_batch_thread_worker,
                args=(i,),
                name=f"WorkerThread-{i}",
                daemon=False
            )
            t.start()
            threads.append(t)

        # Keep main thread active
        while not self.shutdown_event.is_set():
            self.shutdown_event.wait(timeout=1.0)

        print(" [*] Waiting for consumer threads to finish active batches...")
        for t in threads:
            t.join(timeout=10.0)

        print(" [*] Worker node shut down cleanly.")

    def initialize_distributed_index(self) -> bool:
        try:                
            if self.es.indices.exists(index=self.index_name):
                print(f" [*] Worker: Index '{self.index_name}' already online in cluster.")
                return True
            
            settings = {
                "number_of_shards": max(len(self.es_cluster), 1),
                "number_of_replicas": 1
            }

            mappings = {
                "properties": {
                    "timestamp": {"type": "keyword", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
                    "hostname":  {"type": "keyword"},
                    "process":   {"type": "keyword"},
                    "severity":  {"type": "keyword"},
                    "message":   {"type": "text"},
                    "raw":       {"type": "text"}
                }
            }

            self.es.indices.create(index=self.index_name, settings=settings, mappings=mappings)
            print(f" [*] Worker: Successfully created index '{self.index_name}'.")
            return True
        except Exception as e:
            print(f" [!] Error initializing '{self.index_name}': {e}")
            return False

    def _ingest_batch_thread_worker(self, thread_id: int):
        print(f" [*] {self.server_name} thread-{thread_id}: connecting to RabbitMQ broker at {self.gateway_ip}...")
        while not self.shutdown_event.is_set():
            connection, channel = None, None
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
                channel.queue_declare(queue=self.queue_name, durable=True)
                self.register_channel_thread(channel=channel)
                print(f" [+] Thread-{thread_id}: Connected to gateway ({self.gateway_ip}). Awaiting log requests...") 

                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(queue=self.queue_name, on_message_callback=self.message_callback)

                # Thread is blocked here until channel.stop_consuming() is called by request_shutdown
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as conn_err:
                if self.shutdown_event.is_set(): break
                print(f" [!] Thread-{thread_id}: RabbitMQ connection lost ({conn_err}). Retrying in 5 seconds...")
                time.sleep(5)
            except Exception as unhandled_err:
                if self.shutdown_event.is_set(): break
                print(f" [!] Thread-{thread_id}: Unexpected worker exception: {unhandled_err}. Restarting loop in 5 seconds...")
                time.sleep(5)
            finally:
                if channel: 
                    self.unregister_channel_thread(channel=channel)
                if connection and connection.is_open:
                    try:
                        connection.close()
                    except Exception:
                        pass

    def message_callback(self, ch, method, properties, body):
        batch_id = "N/A"
        try:
            raw_text = body.decode("utf-8")

            if "TEST-TEST-TEST" in raw_text:
                print(f"TEST RECEIVED: {raw_text}")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return
            
            payload  = json.loads(body.decode('utf-8'))
            batch_id = payload.get("batch_id", "fallback")
            raw_logs = payload.get("logs_batch", [])
            print(type(batch_id))

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
            print(f" [+] Indexed {success_count} logs for Batch: {batch_id[:8]}... (ACKed)")
        except Exception as e:
            print(f" [!] Error in {self.server_name}. Batch {batch_id[:8]}: {e}. Requeuing...")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

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

    def _handle_shutdown(self, signum, frame):
        """Signal handler for SIGTERM (Docker) and SIGINT (Ctrl+C)."""
        print(f"\n [!] Received signal {signum}. Initiating worker shutdown...")
        self.shutdown_event.set()

        with self.channel_lock:
            for channel in self.open_channels:
                try:
                    if channel.is_open and channel.connection.is_open:
                        channel.connection.add_callback_threadsafe(channel.stop_consuming)
                except Exception as e:
                    print(f" [!] Error signalling channel shutdown: {e}")

    def register_channel_thread(self, channel):
        with self.channel_lock:
            self.open_channels.append(channel)

    def unregister_channel_thread(self, channel):
        with self.channel_lock:
            if channel in self.open_channels:
                self.open_channels.remove(channel)

if __name__ == "__main__":
    worker = Worker(
        server_name = os.getenv("SERVER_NAME", "default_name"), 
        username    = os.getenv("USERNAME",    "default_usr"), 
        password    = os.getenv("PASSWORD",    "default_pwd"), 
        gateway_ip  = os.getenv("GATEWAY_IP",  "127.0.0.1"),
        es_cluster  = os.getenv("ES_CLUSTER",  "http://localhost:9200").split(","),
        index_name  = os.getenv("INDEX_NAME",  "default_idx"),
        queue_name  = os.getenv("QUEUE_NAME",  "default_que"),
        num_threads = int(os.getenv("NUM_THREADS", 0)),
    )
    worker.start()
    