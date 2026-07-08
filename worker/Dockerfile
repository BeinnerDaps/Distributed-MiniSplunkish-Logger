FROM python:3.10-slim

RUN apt-get update && apt-get install -y gcc libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY worker.py .

RUN pip install --no-cache-dir flask requests etcd3

ENTRYPOINT ["python", "worker.py"]
