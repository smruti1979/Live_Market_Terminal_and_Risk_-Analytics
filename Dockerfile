FROM python:3.11-slim

WORKDIR /app

# Install system dependencies needed for compiling confluent-kafka dependencies
RUN apt-get update && apt-get install -y gcc g++ && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all scripts into the container app context
COPY . .

# Expose Streamlit's default web port
EXPOSE 8501