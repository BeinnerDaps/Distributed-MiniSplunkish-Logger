# Use lightweight Python base image
FROM python:3.10-slim

# Prevent Python from writing bytecode and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Copy dependencies file first for Docker build caching 
COPY requirements.txt .

# Install curl for potential health checks or debugging
RUN apt-get update && apt-get install -y --no-install-recommends gcc libssl-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the rest of the gateway application code
COPY . .

# Expose FastAPI port
EXPOSE 8000

# Executive/JSON syntax passes signals directly to Uvicorn (PID 1)
# "Gateway_node:app" assumes your file is named Gateway_node.py and your FastAPI instance is named 'app'
CMD ["uvicorn", "Gateway_node:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--log-level", "info"]
