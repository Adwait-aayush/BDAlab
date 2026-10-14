# Multi-service Python container for Movie Review Sentiment Pipeline
FROM python:3.10-slim

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/workspace

WORKDIR /workspace

# Install system utilities for health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install pinned Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code, configurations, and default fixtures
COPY app/ ./app/
COPY web/ ./web/
COPY model/ ./model/
COPY producer/ ./producer/
COPY consumer/ ./consumer/
COPY scripts/ ./scripts/
COPY data/ ./data/
COPY artifacts/ ./artifacts/
COPY .env.example ./.env.example

# Lightweight control-room web server
EXPOSE 8501

CMD ["python", "app/web_server.py"]
