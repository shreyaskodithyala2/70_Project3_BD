# ONE image for every Python container (kafka-init, workers, master, web).
# docker-compose.yml starts several containers from it, each with a different command.

# Small official Debian-based Python image
FROM python:3.12-slim

# No .pyc files; print logs immediately so `docker logs` is live
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies first: this layer is cached and only rebuilt when requirements.txt changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Then the code (changes often -> cheap rebuilds)
COPY common ./common
COPY kafka_setup ./kafka_setup
COPY master ./master
COPY worker ./worker
COPY web ./web
COPY tools ./tools
