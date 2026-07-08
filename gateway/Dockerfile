FROM python:3.10-slim

WORKDIR /app

COPY gateway.py .

RUN pip install flask requests etcd3

ENTRYPOINT ["python", "gateway.py"]
