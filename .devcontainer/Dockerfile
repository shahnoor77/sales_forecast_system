FROM mcr.microsoft.com/devcontainers/python:3.10

RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    libffi-dev \
    build-essential \
    gcc \
    g++ \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install poetry
