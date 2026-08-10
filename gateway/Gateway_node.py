import aio_pika
import aiofiles
import asyncio
import hashlib
import httpx
import orjson
import os
import redis.asyncio
import uvicorn
from contextlib import asynccontextmanager
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, UploadFile, HTTPException, File, Query, BackgroundTasks, status
from pathlib import Path
from starlette.concurrency import run_in_threadpool
from urllib.parse import quote

# SECRET_KEY = os.getenv("JWT_SECRET_KEY", "supersecretpassword")
# ALGORITHM = "HS256"

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
REDIS_CLIENT = redis.asyncio.from_url(REDIS_URL, decode_responses=True)

UPLOAD_SEMAPHORE = asyncio.Semaphore(2)
TEMP_DIR = Path("/tmp/nsdsyst14_ingest")
TEMP_DIR.mkdir(parents=True, exist_ok=True)

class GatewayError(Exception):
    """Domain exception for gateway operations."""
    pass

class Gateway:
    def __init__(self, rabbit_user: str, rabbit_pass: str, rabbit_port: str, rabbit_host: str, queue_name: str, index_name: str, batch_size: int, es_cluster: list[str]):
        # RabbitMQ
        self.rabbit_url = f"amqp://{rabbit_user}:{rabbit_pass}@localhost:{rabbit_port}/{quote(rabbit_host, safe='')}"
        self.queue_name = queue_name
        self.dlx_name   = f"{queue_name}_dlx"
        self.dlq_name   = f"{queue_name}_dlq"
        self.connection : aio_pika.RobustConnection = None
        self.channel    : aio_pika.RobustChannel = None

        # ElasticSearch
        self.es_cluster = es_cluster
        self.batch_size = batch_size
        self.index_name = index_name.lower()
        self.es_client  : AsyncElasticsearch = None

    async def connect(self):
        """Establish persistent connection with RabbitMQ broker on server startup."""
        print("[*] Connecting to RabbitMQ broker...")
        self.connection = await aio_pika.connect_robust(self.rabbit_url)
        self.channel = await self.connection.channel(publisher_confirms=True)

        # Declare Dead Letter Exchange and Dead Letter Queue for rejected messages
        dlx = await self.channel.declare_exchange(self.dlx_name, type="direct", durable=True)
        dlq = await self.channel.declare_queue(self.dlq_name, durable=True)
        await dlq.bind(dlx, routing_key=self.dlq_name)
        
        # Declare queue as durable (persistent even after RabbitMQ broker restarts)
        await self.channel.declare_queue(
            self.queue_name,
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-dead-letter-exchange": self.dlx_name,
                "x-dead-letter-routing-key": self.dlq_name,
            }
        )
        print("[+] Successfully established connection with RabbitMQ broker.")

        # await self.test_connection()
    
    async def test_connection(self, counter: int = 1):
        """ Test function for testing and debugging worker connectivity. """
        await asyncio.sleep(30)
        while counter < 50:
            payload = f"TEST-TEST-TEST #{counter}".encode("utf-8")
            # Send message directly to the queue
            await self.channel.default_exchange.publish(
                message = aio_pika.Message(
                    body=payload,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=self.queue_name
            )
            print(f" [>] Sent to queue '{self.queue_name}': {payload}")
            counter += 1
            await asyncio.sleep(1)

    async def close(self):
        """Gracefully close connection with RabbitMQ broker on server shutdown."""
        if self.connection and not self.connection.is_closed:
            print("[*] Closing RabbitMQ connection...")
            await self.connection.close()

    async def process_stream(self, file_path: str, job_id: str, redis_key: str):
        """
        - process_stream: Parses file lines off-thread and publishes batches concurrently to RabbitMQ.
        - get_next_window: Uses bounded-memory chunked batching to keep memory usage low.
        - run_in_threadpool: runs sync code safely inside async environment without stalling event loop.
        """
        semaphore = asyncio.Semaphore(5) # Max 5 parallel network writes
        try:
            await REDIS_CLIENT.hset(redis_key, "status", "processing")
            # 1. Offload sync line parsing and chunking to a thread worker
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                def get_next_window(max_batches: int = 20):
                    batches = []
                    batch = []
                    lines = 0
                    for line in f:
                        line = line.strip()
                        if not line: 
                            continue
                        batch.append(line)
                        lines+=1
                        if len(batch) >= self.batch_size:
                            batches.append(batch)
                            batch = []
                            if len(batches) >= max_batches:
                                return batches, lines
                    if batch:
                        batches.append(batch)
                    return batches, lines

                total_lines, total_batches = 0, 0
                while True:
                    # Execute CPU file line parsing off the event loop to async threadpool
                    batches, lines = await run_in_threadpool(get_next_window)
                    if not batches: 
                        break

                    total_lines += lines
                    total_batches += len(batches)

                    # 2. Publish current set of batches concurrently (bounded by Semaphore to prevent socket starvation)
                    tasks = []
                    for batch in batches:
                        batch_content = "".join(batch)
                        batch_id = hashlib.md5(batch_content.encode("utf-8")).hexdigest()[:16]
                        task = asyncio.create_task(self.publish_batch_with_retry(batch_id, batch, semaphore))
                        tasks.append(task)

                    await asyncio.gather(*tasks)
                           
            print(f"[*] [Job {job_id}] Finished: Queued {total_lines} lines across {total_batches} batches.")
        
            await REDIS_CLIENT.hset(redis_key, mapping={
                "status": "completed",
                "total_batches": total_batches,
                "total_logs": total_lines
            })
        except Exception as e:
            print(f"[!] [Job {job_id}] Processing failed: {e}")
            await REDIS_CLIENT.hset(redis_key, mapping={
                "status": "failed",
                "error": str(e)
            })
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[*] [Job {job_id}] Cleaned up temp file {file_path}")

    async def publish_batch_with_retry(self, batch_id: str, batch: list[str], semaphore: asyncio.Semaphore, max_retries: int = 3):
        """
        Publish a batch of logs to message queue.
        Idempotency batch_id key to prevent dupes.
        Awaits confirmation from RabbitMQ disk storage; 
        retries automatically if transient failures occur.
        """
        async with semaphore:
            # Dump log batch using fast Rust-based JSON serialization directly to bytes.
            payload = orjson.dumps({"batch_id": batch_id, "logs_batch": batch })

            message = aio_pika.Message(
                body=payload,
                message_id=batch_id,
                content_type="application/json",
                delivery_mode = aio_pika.DeliveryMode.PERSISTENT,
            )
            for attempt in range(1, max_retries+1):
                try:
                    # Await broker ACK to confirm message is in queue
                    await self.channel.default_exchange.publish(
                        message=message,
                        routing_key=self.queue_name,
                        timeout=5.0
                    )
                    return
                except Exception as e:
                    if attempt == max_retries:
                        raise GatewayError(f"Failed to publish batch {batch_id} after {max_retries} attempts: {e}") from e
                    await asyncio.sleep(0.2 * attempt)
    
    async def initialize_es_cluster_index(self, max_retries: int = 20) -> bool:
        """
        Polls and waits for the Elasticsearch cluster to respond to ping requests
        before verifying or creating the index schema.
        """
        print(f" [~] ES: Connecting to cluster {self.es_cluster}...")
        if not self.es_client:
            self.es_client = AsyncElasticsearch(
                hosts                   = self.es_cluster,
                sniff_on_start          = False, # Prevents Docker container internal IP redirection
                sniff_on_node_failure   = True,  # Auto-reroute if an ES node drops
                sniff_timeout           = 60,    # Refresh cluster topology every 60s
                request_timeout         = 5,     # Fast failover: time out after 3s instead of default 30s
                retry_on_timeout        = True,
                retry_on_status         = (502,503,504,500)
            )

        # Wait for ES ping response
        for attempt in range(1, max_retries + 1):
            try:
                if await self.es_client.ping():
                    print(f" [+] ES: Ping successful on attempt {attempt}/{max_retries}!")
                    break
                print(f" [!] ES: Ping returned False ({attempt}/{max_retries}). Retrying in {0.5*attempt}s...")
            except Exception as e:
                print(f" [!] ES: Cluster offline ({e}). Retrying in {0.5*attempt}s ({attempt}/{max_retries})...")
            await asyncio.sleep(0.5*attempt)
        else:
            print(f"[CRITICAL] ES: Failed to reach Elasticsearch after {max_retries} retries.")
            return False
            
        try:            
            if await self.es_client.indices.exists(index=self.index_name):
                print(f" [*] ES: Index '{self.index_name}' already online in cluster.")
                return True
            
            date_formats = "strict_date_optional_time||MMM  d HH:mm:ss||MMM dd HH:mm:ss||epoch_millis"
            await self.es_client.indices.create(
                index    = self.index_name, 
                settings = {
                    "number_of_shards": max(len(self.es_cluster), 1),
                    "number_of_replicas": 1,
                    "refresh_interval": "5s"
                }, 
                mappings = {
                    "properties": {
                        # Metadata
                        "tags":      {"type": "keyword"},
                        "batch_id":  {"type": "keyword"},
                        "worker":    {"type": "keyword"},

                        # Core Log Fields
                        "timestamp": {"type": "date", "format": date_formats},
                        "hostname":  {"type": "keyword"},
                        "process":   {"type": "keyword"},
                        "pid":       {"type": "integer"},
                        "severity":  {"type": "keyword"},
                        "message":   {"type": "text", "fields": {"keyword":{"type":"keyword", "ignore_above": 256}}},
                        "raw_log":   {"type": "text", "fields": {"keyword":{"type":"keyword", "ignore_above": 256}}}
                    }
                }
            )
            print(f" [+] ES: Successfully created index '{self.index_name}'.")
            return True
        except Exception as e:
            print(f" [!] ES: Error initializing '{self.index_name}': {e}")
            return False
    
    async def es_cluster_request(self, method: str, es_endpoint: str, payload: dict = None):
        """Attempt to send an HTTP request to the ES cluster with automatic failover across all worker nodes."""
        async with httpx.AsyncClient(timeout=5.0) as client:
            last_exc = None
            for es_node in self.es_cluster:
                url = f"{es_node}/{es_endpoint}"
                try:
                    match method.upper():
                        case "POST": resp = await client.post(url, json=payload)
                        case "PUT":  resp = await client.put(url, json=payload)
                        case _:
                            raise ValueError(f"Unsupported HTTP method: {method}")

                    # Because ElasticSearch is a unified 3-node cluster, querying any healthy node is sufficient.
                    resp.raise_for_status()
                    return resp 
                except(httpx.RequestError, httpx.HTTPStatusError) as e:
                    last_exc = e
            else:
                raise HTTPException(
                    status_code=503, 
                    detail=f"All {len(self.es_cluster)} nodes are unreachable. Error: {str(last_exc)}"
                )

    async def es_cluster_purge(self) -> dict:
        print("[*] Initiating system-wide purge sequence...")
        purge_status = {"rabbitmq": False, "elasticsearch": False, "redis": False}
        
        try:
            # Purge RabbitMQ Queue
            main_queue = await gateway.channel.get_queue(gateway.queue_name, ensure=False)
            await main_queue.purge()
    
            dlq_queue = await gateway.channel.get_queue(gateway.dlq_name, ensure=False)
            await dlq_queue.purge()
    
            purge_status["rabbitmq"] = True
            print("[+] Successfully purged RabbitMQ main queue and DLQ.")
        except Exception as e:
            print(f"[!] Error purging RabbitMQ queues: {e}")
    
        try:
            if await self.es_client.indices.exists(index=self.index_name):
                await self.es_client.indices.delete(index=self.index_name)
                print(f"[+] ES index '{self.index_name}' dropped.")

            es_ready = await self.initialize_es_cluster_index()
            purge_status["elasticsearch"] = es_ready
        except Exception as e:
            print(f"[!] Error purging ElasticSearch cluster: {e}")
           
        try: 
            async for key in REDIS_CLIENT.scan_iter("job:*"):
                await REDIS_CLIENT.delete(key)

            purge_status["redis"] = True
            print("[+] Redis job tracking states flushed.")
        except Exception as e:
            print(f"[!] Redis purge error: {e}") 

        return purge_status
     
"""Global Gateway instance."""
gateway = Gateway(
    rabbit_user = os.getenv("RABBIT_USER", "N/A"), 
    rabbit_pass = os.getenv("RABBIT_PASS", "N/A"), 
    rabbit_port = os.getenv("RABBIT_PORT", "N/A"), 
    rabbit_host = os.getenv("RABBIT_HOST", "N/A"), 
    queue_name  = os.getenv("QUEUE_NAME",  "N/A"),
    index_name  = os.getenv("INDEX_NAME",  "N/A"),
    es_cluster  = os.getenv("ES_CLUSTER",  "N/A").split(","),
    batch_size  = int(os.getenv("BATCH_SIZE",  0))
)

""" Creates FastAPI lifespan. """
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[*] Starting Gateway Control Plane...")
    await gateway.connect() # Start up: Connect to RabbitMQ

    es_ready = await gateway.initialize_es_cluster_index()
    if not es_ready:
        print("[CRITICAL] Could not initialize Elasticsearch index. Exiting...")
        raise RuntimeError("Elasticsearch initialization failed.")

    yield

    print("[-] Shutting down Gateway services...")
    await gateway.close() # Shut down: Graceful Clean Ip

""" Initialize FastAPI app. """
app = FastAPI(title="Mini Splunk EC2 Gateway", lifespan=lifespan)

# http://10.13.13.1:8000/test-get
@app.get("/test-get")
async def test_connection_get():
    return {"status": "success", "message": "The FastAPI server is reachable with HTTP GET!"}

# http://10.13.13.1:8000/test-post
@app.post("/test-post")
async def test_connection_post():
    return {"status": "success", "message": "The FastAPI server is reachable with HTTP POST!"}

""" INGEST logs to the RabbitMQ message queue. """
@app.post("/ingest/", status_code=status.HTTP_202_ACCEPTED)
async def ingest_logs(bg_task: BackgroundTasks, file: UploadFile = File(...)) -> dict:
    if not file.filename.endswith((".txt",".log")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="Only .txt and .log files are accepted."
        )

    # Semaphore limits ingestion to 2 simultaneously for memory
    if UPLOAD_SEMAPHORE.locked():
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Server busy processing concurrent uploads. Please try again shortly."
        )

    job_id = hashlib.md5(file.filename.encode("utf-8")).hexdigest()
    file_type = "txt" if file.filename.endswith(".txt") else "log"
    temp_file_path = TEMP_DIR / f"job_{job_id}.{file_type}"

    redis_key = f"job:{job_id}"
    await REDIS_CLIENT.hset(redis_key, mapping={
        "status": "queued",
        "filename": file.filename,
        "total_batches": 0,
        "total_logs": 0,
        "error": ""
    })
    await REDIS_CLIENT.expire(redis_key, 3600)

    # Stream directly to disk in 1MB chunks to prevent memory bloat and keep RAM footprint negligible
    async with UPLOAD_SEMAPHORE:
        try:   
            async with aiofiles.open(temp_file_path, 'wb') as out_file:
                while chunk := await file.read(1024 * 1024):
                    await out_file.write(chunk)
        except Exception as e:
            if temp_file_path.exists():
                temp_file_path.unlink()
            await REDIS_CLIENT.hset(redis_key, mapping={"status": "failed", "error": str(e)})
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail=f"Ingest failed during file transfer: {str(e)}"
            )
        finally:
            await file.close()

    # Offload line parsing and RabbitMQ publishing to a background task using BackgroundTasks
    bg_task.add_task(gateway.process_stream, temp_file_path, job_id, redis_key)

    return {
        "status": "accepted",
        "job_id": job_id,
        "message": f"File '{file.filename}' uploaded and queued for background processing."
    } 

"""Status polling endpoint read by client applications."""
@app.get("/job/{job_id}/")
async def get_job_status(job_id: str) -> dict:
    job_info = await REDIS_CLIENT.hgetall(f"job:{job_id}")
    if not job_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, 
            detail=f"Job ID: {job_id} not found or expired."
        )
    return job_info

""" QUERY standard Elasticsearch Query DSL for specific logs. """
@app.post("/query/", status_code=status.HTTP_202_ACCEPTED)
async def advanced_query(mode: str, value: str, qsize: int = Query(default=100, ge=1, le=10000)) -> dict:
    # Create query json structure
    is_count = bool(mode == "COUNT_KEYWORD")
    es_query = { "size": qsize, "sort": [{"timestamp": {"order":"desc"}}] } if not is_count else {}

    if mode == "SEARCH_DATE":
        start, end = [dates.strip() for dates in (value.split("-")*2)[:2]]

    match mode:
        case "SEARCH_DATE":     es_query["query"] = {"range": {"timestamp": {"gte": start, "lte": end}}}
        case "SEARCH_HOST":     es_query["query"] = {"bool": {"filter": [{"term": {"hostname": value}}]}}
        case "SEARCH_DAEMON":   es_query["query"] = {"bool": {"filter": [{"term": {"process": value}}]}}
        case "SEARCH_SEVERITY": es_query["query"] = {"bool": {"filter": [{"term": {"severity": value}}]}}
        case "SEARCH_KEYWORD":  es_query["query"] = {"multi_match": {"query": value, "fields": ["raw_log"]}}
        case "COUNT_KEYWORD":   es_query["query"] = {"multi_match": {"query": value, "fields": ["raw_log"]}}
        case _: 
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Unsupported command: {mode}")

    get_type = "_count" if is_count else "_search"
    resp = await gateway.es_cluster_request("POST", f"{gateway.index_name}/{get_type}", es_query)

    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=f"Elasticsearch search failed ({resp.status_code}): {resp.text}"
        ) 

    resp = resp.json()
    if is_count:
        return {
            "command": mode, 
            "keyword": value, 
            "count": resp.get("count", 0)
        }

    hits = resp.get("hits", {})
    total_matches = hits.get("total", {}).get("value",0)
    total_hits = [hit["_source"] for hit in hits.get("hits", [])]

    return {
        "command": mode, 
        "keyword": value, 
        "total_matches": total_matches, 
        "hit_count": len(total_hits), 
        "results": total_hits
    }

""" PURGE all indexed log entries in Elastisearch. """
@app.post("/purge/", status_code=status.HTTP_202_ACCEPTED)
async def purge_logs():
    lock_name = "global_purge_lock"
    lock_acquired = await REDIS_CLIENT.set(lock_name, "LOCKED", nx=True, ex=30)

    if not lock_acquired:
        raise HTTPException(
            status_code=status.HTTP_423_LOCKED, 
            detail="System is currently busy. Distributed lock active."
        )
    
    try:
        results = await gateway.es_cluster_purge()

        if not all(results.values()):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": "System purge encountered partial failures.", "components": results}
            )
    
        # VERIFICATION: Query document count
        count_endpoint = f"{gateway.index_name}/_count"
        count_resp = await gateway.es_cluster_request("POST", count_endpoint, payload={"query": {"match_all": {}}})

        doc_count = count_resp.json().get("count", -1)

        if doc_count != 0:
            raise HTTPException(
                status_code=500,
                detail=f"Purge incomplete. Database still contains {doc_count} document(s)."
            )

        return {
            "command": "PURGE", 
            "status": "SUCCESS", 
            "doc_count": doc_count,
            "message": "All queues, Elasticsearch indices, and job states purged successfully.",
            "results": results
        }
    
    except Exception as e:
        print(f"[!] Error purging: {e}")
    finally:
        await REDIS_CLIENT.delete(lock_name)

# Used only when executing "python Gateway_node.py" locally
if __name__ == "__main__":
    print("[~] Gateway: Starting Gateway server on port 8000...")
    uvicorn.run("Gateway_node:app", host="0.0.0.0", port=8000, reload=False, workers=1, log_level="info")