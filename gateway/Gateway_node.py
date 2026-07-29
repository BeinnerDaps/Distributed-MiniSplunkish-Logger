import aio_pika
import asyncio
import httpx
import json
import redis
import uuid
from contextlib import asynccontextmanager
from elasticsearch import Elasticsearch
from fastapi import FastAPI, UploadFile, HTTPException, File, Depends


INDEX_NAME = "distributed-logs"

SVR0_VPN_IP = "10.13.13.7"
SVR1_VPN_IP = "10.13.13.8"
SVR2_VPN_IP = "10.13.13.9"

ES_HOSTS = [
    f'http://{SVR0_VPN_IP}:9200',
    f'http://{SVR1_VPN_IP}:9200',
    f'http://{SVR2_VPN_IP}:9200'
]

es_client = Elasticsearch(hosts=ES_HOSTS)


REDIS_CLIENT = redis.Redis(host="localhost", port=6379, db=0)

INDEX_BODY = {
    "settings": {
        "number_of_shards": len(ES_HOSTS),
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

# RabbitMQ Publisher 
class GatewayPublisher:
    def __init__(self, amqp_url: str, queue_name: str, batch_size: int):
        self.amqp_url   = amqp_url
        self.queue_name = queue_name
        self.batch_size = batch_size
        self.connection = None
        self.channel    = None
    
    async def connect(self):
        """Establish persistent connection with RabbitMQ broker on server startup."""
        print("[*] Connecting to RabbitMQ broker...")
        # Connect robust auto-reconnects if connection is dropped
        self.connection = await aio_pika.connect_robust(self.amqp_url)
        # CRITICAL FOR FAULT TOLERANCE: Enable publisher confirmations
        self.channel = await self.connection.channel(publisher_confirms=True)
        # Declare queue as durable (persistent even after RabbitMQ broker restarts)
        await self.channel.declare_queue(self.queue_name, durable=True)
        print("[+] Successfully established connection with RabbitMQ broker.")

    async def close(self):
        """Gracefully close connection with RabbitMQ broker on server shutdown."""
        if self.connection and not self.connection.is_closed:
            print("[*] Closing RabbitMQ connection...")
            await self.connection.close()
      
    async def process_stream(self, file_object) -> tuple[int, int]:
        """Reads file stream per line and publishes each batch to RabbitMQ."""
        current_batch = []
        batch_count = 0
        line_count = 0

        for line in file_object:
            decoded_line = line.decode('utf-8', errors='ignore').strip()
            if not decoded_line: 
                continue

            current_batch.append(decoded_line)

            if len(current_batch) >= self.batch_size:
                batch_id = str(uuid.uuid4())
                await self.publish_batch_with_retry(batch_id, current_batch)
                line_count += len(current_batch)
                batch_count += 1
                current_batch = []
        
        if current_batch:
            batch_id = str(uuid.uuid4())
            await self.publish_batch_with_retry(current_batch)
            line_count += len(current_batch)
            batch_count += 1

        return batch_count, line_count

    async def publish_batch_with_retry(self, batch_id: str, log_batch: list, max_retries: int = 3):
            """
            Publish a batch of logs to message queue. 
            Awaits confirmation from RabbitMQ disk storage; 
            retries automatically if transient failures occur.
            """
            # Dump log batch into json with Idempotency batch_id key for workers
            payload = json.dumps({
                "batch_id": batch_id,
                "logs_batch": log_batch
            }).encode("utf-8")
    
            message = aio_pika.Message(
                body = payload,
                delivery_mode = aio_pika.DeliveryMode.PERSISTENT,
                message_id=batch_id,
                content_type="application/json"
            ), 
    
            for attempt in range(1, max_retries+1):
                try:
                    # Await broker ACK to confirm message is in queue
                    await self.channel.default_exchange.publish(
                        message,
                        routing_key=self.queue_name,
                        timeout=5.0
                    )
                    return
                except Exception as e:
                    print(f" [!] [Attempt {attempt}/{max_retries}] Publish failed for batch {batch_id}: {e}")
                    await asyncio.sleep(0.5 * attempt)
            else:
                raise HTTPException(
                    status_code=500, 
                    detail=f"Broker failed to confirm batch persistence after {max_retries} retries."
                )

    """ Attempt to send an HTTP request to the ES cluster with automatic failover across all worker nodes. """
    async def es_cluster_request(self, method: str, es_endpoint: str, payload: dict = None):
        async with httpx.AsyncClient(timeout=5.0) as client:
            last_exc = None
            for es_node in ES_HOSTS:
                url = f"{es_node}/{es_endpoint}"
                try:
                    match method.upper():
                        case "POST":    resp = await client.post(url, json=payload)
                        case "PUT":     resp = await client.put(url, json=payload)
                        case "DELETE":  resp = await client.delete(url)
                        case _:
                            raise ValueError(f"Unsupported HTTP method: {method}")

                    # Because Elasticsearch is a unified 3-node cluster, 
                    # querying any single healthy node is sufficient.
                    return resp 
                except(httpx.RequestError, httpx.HTTPStatusError) as e:
                    last_exc = e
                    continue
            else:
                raise HTTPException(
                    status_code=503, 
                    detail=f"All {len(ES_HOSTS)} nodes are unreachable. Error: {str(last_exc)}"
                )
                    
"""Global GatewayPublisher instance"""

username = 'rabbituser'
password = 'rabbit1234'

username = input(f"Enter RabbitMQ username ['{username}']: ")
password = input(f"Enter RabbitMQ password ['{password}']: ")

gateway_server = GatewayPublisher(
    amqp_url = f"amqp://{username}:{password}@localhost:5672",
    queue_name="log_ingest_queue",
    batch_size=5000
)

""" Creates FastAPI lifespan """
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start up: Connect to RabbitMQ
    await gateway_server.connect()
    yield

    # Shut down: Graceful Clean Ip
    await gateway_server.close()

app = FastAPI(title="Mini Splunk EC2 Gateway", lifespan=lifespan)


""" INGEST logs to the log_ingest_queue. """
@app.post("/ingest/")
async def ingest_logs(file: UploadFile = File(...)):
    if not file.filename.endswith((".txt",".log")):
        raise HTTPException(status_code=400, detail="Only .txt and .log files are accepted.")
    
    try:
        batch_count, line_count = await gateway_server.process_stream(file.file)

        return {
            "status": "success",
            "total_batch_count": batch_count,
            "total_lines_count": line_count,
            "message": f"Successfully queued {line_count} logs stream from '{file.filename}'."
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process log payload: {str(e)}")
    finally:
        await file.close()
    

""" QUERY standard Elasticsearch Query DSL for specific logs. """
@app.post("/query/")
async def advanced_query(mode: str, value: str, qsize=100):
    is_count = bool(mode == "COUNT_KEYWORD")
    es_query = { "size": qsize, "sort": [{"timestamp": {"order":"desc"}}] } if not is_count else {}

    match mode:
        case "SEARCH_DATE":     es_query["query"] = {"prefix": {"timestamp.keyword": value}}
        case "SEARCH_HOST":     es_query["query"] = {"term": {"hostname": value}}
        case "SEARCH_DAEMON":   es_query["query"] = {"term": {"process": value}}
        case "SEARCH_SEVERITY": es_query["query"] = {"term": {"severity": value}}
        case "SEARCH_KEYWORD":  es_query["query"] = {"multi_match": {"query": value, "fields": ["message","raw"]}}
        case "COUNT_KEYWORD":   es_query["query"] = {"multi_match": {"query": value, "fields": ["message","raw"]}}
        case _: 
            raise HTTPException(status_code=400, detail=f"Unsupported command: {mode}")

    async with httpx.AsyncClient() as client:
        get_type = "_count" if is_count else "_search"
        resp = await gateway_server.es_cluster_request("POST", f"{INDEX_NAME}/{get_type}", es_query)

        if resp.status_code != 200:
            raise HTTPException(status_code=500, detail="Elasticsearch search failed.") 

        resp_data = resp.json()

        if is_count:
            count = resp_data.get("count", 0)
            return {"command": mode, "keyword": value, "count": count, "results": count}
        else:
            hits = [hit["_source"] for hit in resp_data.get("hits", {}).get("hits", [])]
            return {"command": mode, "keyword": value, "count": len(hits), "results": hits}
    

""" PURGE all indexed log entries in Elastisearch. """
@app.post("/purge/")
async def purge_logs():
    lock_name = "global_purge_lock"
    lock_acquired = REDIS_CLIENT.set(lock_name, "LOCKED", nx=True, ex=30)

    if not lock_acquired:
        raise HTTPException(status_code=423, detail="Purge in progress or system busy. Distributed lock active.")
    
    try:
        # 1. Purge RabbitMQ Queue
        async with gateway_server.channel.declare_queue(gateway_server.queue_name, durable=True) as queue:
            await queue.purge()
            
        # 2. Delete index across ES clusters
        del_resp = await gateway_server.es_cluster_request("DELETE", INDEX_NAME)
        # 3. Re-create index with shared partitioning settings and mapping
        put_resp = await gateway_server.es_cluster_request("PUT", INDEX_NAME, payload=INDEX_BODY)

        return {"command": "PURGE", "keyword": "PURGE", "count": 0, "results": put_resp.status_code}
    finally:
        REDIS_CLIENT.delete(lock_name)
        