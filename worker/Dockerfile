# Use lightweight Python base image
FROM python:3.10-slim

# Prevent Python from writing bytecode and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory inside the container
WORKDIR /app

# Copy dependencies file and install them 
COPY requirements.txt .

# Install curl for potential health checks or debugging
RUN apt-get update && apt-get install -y --no-install-recommends gcc libssl-dev \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove gcc libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .
