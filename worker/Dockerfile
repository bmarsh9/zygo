FROM python:3.11-slim

WORKDIR /app

# System deps — httpx needs these for SSL, subprocess nodes may call pip at runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install flowcore shared package first (changes less often than worker code)
COPY flowcore/ ./flowcore/
RUN pip install --no-cache-dir -e ./flowcore

# Install worker dependencies
COPY worker/requirements.txt ./worker/requirements.txt
RUN pip install --no-cache-dir -r ./worker/requirements.txt

# Copy worker source
COPY worker/ ./worker/

# Make worker a package if __init__.py doesn't exist yet
RUN touch worker/__init__.py

CMD ["python", "-m", "worker.worker"]