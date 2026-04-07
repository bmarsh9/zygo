"""
scheduler.py
============
Standalone cron scheduler service.

Polls the Flask app's internal API for cron configs and queues flow runs
via Redis. No direct database access.

Architecture:
    [Scheduler] --GET /internal/api/scheduled-flows--> [Flask app]
    [Scheduler] --enqueues jobs to--> [Redis]
    [Workers]   --picks up jobs from--> [Redis]
    [Workers]   --PATCH /internal/api/runs/<id>--> [Flask app]
    [Flask app] --writes results to--> [Postgres]

Required env vars:
    REDIS_URL       e.g. redis://redis:6379/0
    APP_BASE_URL    e.g. http://app:9000

Usage:
    python -m scheduler.scheduler
    python -m scheduler.scheduler --poll-interval 30

Docker:
    docker-compose up scheduler
"""

import argparse
from datetime import datetime, timezone

import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from redis import Redis
from rq import Queue

from scheduler.config import Config


_scheduler = None
_tracked_schedules: dict[int, dict] = {}  # flow_id -> {"cron": "..."}


# ── internal API ──────────────────────────────────────────────────────────────

def _get_scheduled_flows() -> list[dict]:
    """
    Fetch flows with cron enabled from the Flask app.

    Returns a list of:
        {"flow_id": int, "flow_name": str, "cron": str}
    """
    headers = {"X-Internal-Secret": Config.INTERNAL_API_SECRET}
    url = f"{Config.APP_BASE_URL.rstrip('/')}/internal/api/scheduled-flows"
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ── queue helper ──────────────────────────────────────────────────────────────

def _queue_flow_run(flow_id: int) -> str:
    """Enqueue an execute_flow_job to Redis and return the job id."""
    from worker.job_handler import execute_flow_job  # imported here to keep it lazy

    conn = Redis.from_url(Config.REDIS_URL)
    q = Queue("default", connection=conn, default_timeout=300)
    job = q.enqueue(
        execute_flow_job,
        flow_id=flow_id,
        input_data={
            "__trigger__":    "cron",
            "__scheduled_at__": datetime.now(timezone.utc).isoformat(),
        },
        result_ttl=86400,
        failure_ttl=86400,
        ttl=600,
    )
    return job.id


# ── cron trigger ──────────────────────────────────────────────────────────────

def run_flow(flow_id: int):
    """Queue a scheduled flow run."""
    print(f"[Scheduler] Triggering flow {flow_id} at {datetime.now(timezone.utc).isoformat()}")
    try:
        job_id = _queue_flow_run(flow_id)
        print(f"[Scheduler] Flow {flow_id} queued as job {job_id}")
    except Exception as e:
        print(f"[Scheduler] ERROR queuing flow {flow_id}: {e}")


# ── poll loop ─────────────────────────────────────────────────────────────────

def sync_schedules():
    """
    Poll the Flask app for cron config changes.
    Adds/updates/removes APScheduler jobs as needed.
    """
    try:
        scheduled_flows = _get_scheduled_flows()
    except Exception as e:
        print(f"[Scheduler] WARNING: could not fetch scheduled flows: {e}")
        return

    active_flow_ids: set[int] = set()

    for entry in scheduled_flows:
        flow_id: int  = entry["flow_id"]
        flow_name: str = entry["flow_name"]
        cron_expr: str = entry["cron"]
        job_id = f"flow_{flow_id}"
        active_flow_ids.add(flow_id)

        existing = _tracked_schedules.get(flow_id)
        if existing and existing["cron"] == cron_expr:
            continue  # nothing changed

        try:
            trigger = CronTrigger.from_crontab(cron_expr)
            if _scheduler.get_job(job_id):
                _scheduler.reschedule_job(job_id, trigger=trigger)
                print(f"[Scheduler] Updated flow {flow_id} ({flow_name}): {cron_expr}")
            else:
                _scheduler.add_job(
                    run_flow,
                    trigger=trigger,
                    id=job_id,
                    name=f"Flow {flow_id}: {flow_name}",
                    kwargs={"flow_id": flow_id},
                    replace_existing=True,
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=60,
                )
                print(f"[Scheduler] Added flow {flow_id} ({flow_name}): {cron_expr}")

            _tracked_schedules[flow_id] = {"cron": cron_expr}

        except ValueError as e:
            print(f"[Scheduler] Invalid cron for flow {flow_id}: {e}")

    # Remove jobs for flows that no longer have cron enabled
    for flow_id in list(_tracked_schedules.keys()):
        if flow_id not in active_flow_ids:
            job_id = f"flow_{flow_id}"
            if _scheduler.get_job(job_id):
                _scheduler.remove_job(job_id)
                print(f"[Scheduler] Removed flow {flow_id}")
            del _tracked_schedules[flow_id]


# ── entrypoint ────────────────────────────────────────────────────────────────

def main():
    global _scheduler

    parser = argparse.ArgumentParser(description="Cron Scheduler")
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=Config.POLL_INTERVAL,
        help=f"How often (seconds) to poll for config changes (default: {Config.POLL_INTERVAL}s)",
    )
    args = parser.parse_args()

    _scheduler = BlockingScheduler(
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 60,
        },
    )

    _scheduler.add_job(
        sync_schedules,
        "interval",
        seconds=args.poll_interval,
        id="__sync_schedules__",
        name="Sync schedules from Flask app",
        next_run_time=datetime.now(timezone.utc),  # run immediately on startup
    )

    print(f"[Scheduler] Starting — polling every {args.poll_interval}s")
    try:
        _scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[Scheduler] Shutting down")


if __name__ == "__main__":
    main()