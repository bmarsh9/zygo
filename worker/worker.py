"""
worker.py
=========
RQ worker entry point. Run this in Docker containers.

Usage:
    python -m worker.worker                        # Listen to all queues
    python -m worker.worker --queues high          # Listen to high priority only
    python -m worker.worker --queues high default  # Listen to high and default
"""

import argparse
from redis import Redis
from rq import Queue, Worker
from worker.config import Config

def main():
    parser = argparse.ArgumentParser(description="RQ Worker")
    parser.add_argument(
        "--queues", nargs="*", default=["high", "default", "low"],
        help="Queue names to listen on (default: high default low)",
    )
    parser.add_argument(
        "--name", default=None,
        help="Worker name (auto-generated if not set)",
    )
    parser.add_argument(
        "--burst", action="store_true",
        help="Run in burst mode (quit when all queues are empty)",
    )
    args = parser.parse_args()

    conn = Redis.from_url(Config.REDIS_URL)
    queues = [Queue(name, connection=conn) for name in args.queues]

    print(f"[Worker] Connecting to Redis: {Config.REDIS_URL}")
    print(f"[Worker] Listening on queues: {', '.join(args.queues)}")

    worker = Worker(queues, connection=conn, name=args.name)
    worker.work(burst=args.burst)


if __name__ == "__main__":
    main()