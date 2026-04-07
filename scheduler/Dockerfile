FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install flowcore shared package first (changes less often than scheduler code)
COPY flowcore/ ./flowcore/
RUN pip install --no-cache-dir -e ./flowcore

# Install scheduler dependencies
COPY scheduler/requirements.txt ./scheduler/requirements.txt
RUN pip install --no-cache-dir -r ./scheduler/requirements.txt

# Copy worker source — needed because the scheduler enqueues
# worker.job_handler.execute_flow_job by dotted string path.
# RQ resolves this string on the worker side, NOT the scheduler side,
# so the scheduler does NOT need the worker package at all.
COPY scheduler/ ./scheduler/

# Ensure scheduler is a proper package
RUN touch scheduler/__init__.py

CMD ["python", "-m", "scheduler.scheduler"]