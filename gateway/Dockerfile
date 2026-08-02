# Use lightweight Python base image
FROM python:3.10-slim

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

# Execute Gateway script
CMD ["python", "Gateway_node.py"]
