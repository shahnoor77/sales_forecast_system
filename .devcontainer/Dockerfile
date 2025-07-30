FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    make \
    cmake \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    libz-dev \
    curl \
    git \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install poetry (optional)
RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /workspace
