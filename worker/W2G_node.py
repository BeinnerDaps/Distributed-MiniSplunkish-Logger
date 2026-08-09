import asyncio
import aio_pika
import hashlib
import orjson
import os
import random
import re
import signal
from aio_pika.exceptions import AMQPError, AMQPConnectionError, AMQPChannelError
from datetime import datetime, timezone
from elasticsearch import AsyncElasticsearch
from urllib.parse import quote

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

class AsyncWorker:
    def __init__(self, server_name: str, rabbit_user: str , rabbit_pass: str, rabbit_port: str, rabbit_host: str, 
                 prefetch_count: int, gateway_ip: str, es_cluster: list[str], index_name: str, queue_name: str 
    ):  
        # Worker-Gateway variables
        self.server_name    = server_name
        self.gateway_ip     = gateway_ip
        self.shutdown_event = asyncio.Event()
        self.active_tasks   = set()

        # RabbitMQ variables
        self.rabbit_url     = f"amqp://{rabbit_user}:{rabbit_pass}@{gateway_ip}:{rabbit_port}/{quote(rabbit_host, safe='')}"
        self.queue_name     = queue_name
        self.dlq_name       = f"{queue_name}_dlq"
        self.dlx_name       = f"{queue_name}_dlx"
        self.prefetch_count = int(prefetch_count)
        self.connection     : aio_pika.RobustConnection = None
        self.channel        : aio_pika.RobustChannel = None

        # ElasticSearch variables
        self.es_cluster     = es_cluster
        self.index_name     = index_name.lower()
        self.es_client      : AsyncElasticsearch = None

        print(f"=== Distributed Event Logger initialized: [{self.server_name}] ===") 

    async def start(self):
        """Starts the worker node consumer loop."""
        try:
            self._setup_signal_handler()
            if not await self.initialize_es_client():
                print("[!] Worker: Aborting startup. Distributed index setup failed.")
                return
            
            main_queue = await self.connect_to_rabbitmq()
            if not main_queue:
                print("[!] Worker: Aborting startup. Unable to connect to RabbitMQ broker.")
                return

            # Use AbstractQueueIterator as a buffer queue listening for messages
            async with main_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    # Dispatch non-blocking tasks and track them in memory for ACK/NACK callback
                    task = asyncio.create_task(self.process_message(message=message))
                    self.active_tasks.add(task)
                    task.add_done_callback(self.active_tasks.discard)
                    if self.shutdown_event.is_set():
                        break
    
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[!] Unexpected worker error: {e}")
        finally:
            print("[~] Worker: Closing RabbitMQ channels and Elasticsearch client...")
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
            if self.es_client is not None:
                await self.es_client.close()
            print("[-] Worker: Worker node shut down cleanly.")
            
    async def initialize_es_client(self, max_retries: int = 10) -> bool:
        try:
            print(f" [*] ES: Connecting worker to ES cluster: {self.es_cluster}")
            self.es_client = AsyncElasticsearch(
                hosts                   = self.es_cluster,
                # --- Failover & Performance Tuning ---
                request_timeout=3,                  # Fast failover: time out after 3s instead of default 30s
                max_retries=3,                      # Retry up to 3 times before failing
                retry_on_timeout= True,             # Route to next live host on socket/connect timeout
                retry_on_status=(502,503,504,500),  # Failover on gateway/node errors
                # --- Network Topology Safe Settings ---
                sniff_on_start=False,               # Prevents Docker container internal IP hijacking
                sniff_on_node_failure=True,         # Auto-reroute to healthy nodes if an ES node drops
                sniff_timeout=60,                   # Refresh cluster topology every 60s
                maxsize=20                          # Connection pool size matching worker concurrency
            )

            # Health Checks
            for attempt in range(1, max_retries+1):
                print(f"[~] ES: Pinging Elasticsearch cluster (Attempt {attempt}/{max_retries})...")
                if await self.es_client.ping():
                    print(f"[+] ES: Successfully connected to Elasticsearch cluster.")
                    return True
                await asyncio.sleep(0.5*attempt)
            else:
                print(f" [!] ES: ping failed for cluster {self.es_cluster}")
        except Exception as e:
            print(f" [!] ES: Error initializing '{self.index_name}': {e}")

        return False


    async def connect_to_rabbitmq(self, max_retries: int = 10) -> aio_pika.abc.AbstractQueue | None:
        """ Waits for gateway server to initialize RabbitMQ broker. """
        for attempt in range(1, max_retries+1) :
            print(f" [~] Pika: Connecting to RabbitMQ at {self.gateway_ip} (Attempt {attempt})...")
            try:
                self.connection = await aio_pika.connect_robust(self.rabbit_url, timeout=10.0)
                self.channel = await self.connection.channel()

                # Set prefetch count for backpressure control
                await self.channel.set_qos(prefetch_count=self.prefetch_count)

                # Declare Dead Letter Exchange and Dead Letter Queue for rejected messages
                dlx = await self.channel.declare_exchange(self.dlx_name, type="direct", durable=True)
                dlq = await self.channel.declare_queue(self.dlq_name, durable=True)
                await dlq.bind(dlx, routing_key=self.dlq_name)

                main_queue = await self.channel.declare_queue(
                    name      = self.queue_name, 
                    durable   = True,
                    arguments = {
                        "x-queue-type": "quorum",
                        "x-dead-letter-exchange": self.dlx_name,
                        "x-dead-letter-routing-key": self.dlq_name
                    }
                )

                print(f"[*] Worker: '{self.server_name}' active. Listening for '{self.queue_name}'...")
                return main_queue
            except (AMQPConnectionError, AMQPChannelError, AMQPError, ConnectionRefusedError, OSError) as e:
                print(f" [!] Pika: RabbitMQ not ready yet ({e}). Waiting {0.5*attempt}s before retrying...")
                # Cleanup partially created objects before retrying
                if self.channel and not self.channel.is_closed:
                    await self.channel.close()
                if self.connection and not self.connection.is_closed:
                    await self.connection.close()
                await asyncio.sleep(0.5*attempt)
        else:
            return None

    async def process_message(self, message: aio_pika.IncomingMessage, max_retries: int = 3):
        """ Process and upload each message with explicit ACK/NACK/REJECT fault tolerance logic. """
        # Read delivery count header from RabbitMQ quorum queue
        delivery_count = message.headers.get("x-delivery-count", 0) if message.headers else 0

        if delivery_count > 5:
            print(f"[!] Message exceeded max redelivery attempts ({delivery_count}). Sending to DLQ.")
            await message.reject(requeue=False)
            return

        try:
            payload = orjson.loads(message.body)
        except Exception as parse_error:
            print(f" [!] Task: Corrupt payload on {message.message_id}: {parse_error}. Rejecting to DLQ.")
            await message.reject(requeue=False)
            return
        
        batch_id  = payload.get("batch_id")
        log_batch = payload.get("logs_batch", [])

        if not log_batch:
            await message.ack()
            return

        print(f" [*] Task: received log batch '{batch_id}'")
        operations = []
        for line in log_batch:
            line = line.strip()
            if not line: 
                continue

            doc     = self.parse_logs(line, batch_id)
            doc_id  = hashlib.md5(line.encode("utf-8")).hexdigest()

            operations.append({"index": {"_index": self.index_name, "_id": doc_id}})
            operations.append(doc)

        print(f" [~] Task: Bulk uploading batch {batch_id} to ES client...")

        errors = (400, 404, 409)
        retry = (429, 502, 503, 504, 500)
        for attempt in range(1, max_retries+1):
            sleep_time = 0.5*attempt
            try:
                res = await self.es_client.bulk(operations=operations)
                if not res.get("errors"):
                    await message.ack()
                    print(f" [+] Task: Successfully uploaded {len(log_batch)} logs for batch {batch_id}")
                    return

                # Check if errors are due to 429 backpressure
                is_retry = any(item.get("index").get("status") in retry for item in res.get("items", []))
                if is_retry:
                    # Full Jitter Backoff: prevent thundering herd on surviving ES nodes
                    sleep_time = random.uniform(0.5, (2**attempt))
                    print(f"[!] ES indexing pressure (429). Retrying in {sleep_time:.2f}s...")
                    continue
                        
                has_error = any(item.get("index").get("status") in errors for item in res.get("items", []))
                if has_error:
                    print(f" [!] Task: ES Bulk write failed. Rejecting message...")      
                    await message.reject(requeue=False)
                    return 
            except Exception as es_error:
                print(f" [!] Task: ES Bulk write failed (Attempt {attempt}/{max_retries}) for batch {batch_id}: {es_error}")
            finally:
                await asyncio.sleep(sleep_time)
        else:
            print(f" [!] Task: Max ES retries reached for batch {batch_id}. Requeueing to RabbitMQ...")
            await message.nack(requeue=True)

    def parse_logs(self, line: str, batch_id: str) -> dict:   
        def _infer_severity(message: str) -> str:
            """Infers severity level from common syslog keywords."""
            msg_lower = message.lower()
            if any(k in msg_lower for k in ["failed", "failure", "invalid"]):
                return "WARN"
            elif any(k in msg_lower for k in ["error", "fatal", "critical", "emergency"]):
                return "ERROR"
            return "INFO"
                    
        match = SYSLOG_REGEX.match(line)
        if match:
            match = match.groupdict()
            msg = match.get("message")
            return {
                # Metadata
                "tags":         ["parsed"],
                "batch_id":     batch_id,
                "worker":       self.server_name,

                # Core Log Fields
                "timestamp":    match.get("timestamp"),
                "hostname":     match.get("hostname"),
                "process":      match.get("process"),
                "pid":          int(match["pid"]) if match.get("pid") else None,
                "severity":     _infer_severity(msg),
                "message":      msg,
                "raw_log":      line   
            }

        return {
            # Metadata
            "tags":      ["parse_failure"],
            "batch_id":  batch_id,
            "worker":    self.server_name,

            # Core Log Fields
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hostname":  "UNPARSED",
            "process":   "UNPARSED",
            "severity":  "UNKNOWN",
            "pid":       None,
            "message":   "Log failed regex parsing",
            "raw_log":   line,
        }

    def _setup_signal_handler(self):
        """ Registers OS signal handlers for SIGTERM (Docker) and SIGINT (Ctrl+C) for graceful shutdown. """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_shutdown, sig)

    def _handle_shutdown(self, signum):
        print(f"\n[~] Worker: Received signal {signum}. Initiating worker shutdown...")
        self.shutdown_event.set()

if __name__ == "__main__":
    worker = AsyncWorker(
        server_name     = os.getenv("SERVER_NAME", "default_name"), 
        rabbit_user     = os.getenv("RABBIT_USER", "default_user"), 
        rabbit_pass     = os.getenv("RABBIT_PASS", "default_pass"), 
        rabbit_port     = os.getenv("RABBIT_PORT", 0), 
        rabbit_host     = os.getenv("RABBIT_HOST", "default_host"), 
        gateway_ip      = os.getenv("GATEWAY_IP",  "127.0.0.1"),
        es_cluster      = os.getenv("ES_CLUSTER",  "http://localhost:9200").split(","),
        index_name      = os.getenv("INDEX_NAME",  "default_idx"),
        queue_name      = os.getenv("QUEUE_NAME",  "default_que"),
        prefetch_count  = int(os.getenv("PREFETCH_COUNT", 0))    
    )

    try:
        asyncio.run(worker.start())
    except (KeyboardInterrupt, SystemExit):
        pass
    