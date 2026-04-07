FROM python:3.10-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install flowcore shared package before requirements.txt
COPY flowcore/ ./flowcore/
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir ./flowcore

# Install app Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final application image
FROM python:3.10-slim AS app

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-installed Python packages from builder (includes flowcore)
COPY --from=builder /usr/local /usr/local/

COPY . .
CMD ["/bin/bash", "run.sh"]