from flask import current_app
from rq import Queue, Retry, Worker
from rq.job import Job

from app import db
from app.models import Run
from datetime import datetime
import uuid


# Dotted path to the job function on the worker side.
# Passed as a string so the Flask app never imports the worker package.
_EXECUTE_FLOW_JOB = "worker.job_handler.execute_flow_job"


def _get_queues() -> dict[str, Queue]:
    conn = current_app.redis_conn
    return {
        "default": Queue("default", connection=conn, default_timeout=300),
        "high":    Queue("high",    connection=conn, default_timeout=300),
        "low":     Queue("low",     connection=conn, default_timeout=600),
    }


def queue_flow_run(flow_id: int, input_data: dict = None, start_node_id: str = None,
                   replay_data: dict = None, priority: str = "default",
                   webhook: bool = False) -> str:
    print(f"[Queue] queue_flow_run: flow_id={flow_id}, start_node_id={start_node_id}")

    job_id = str(uuid.uuid4())
    run = Run(
        flow_id=flow_id,
        status="queued",
        started_at=datetime.utcnow(),
        job_id=job_id
    )
    db.session.add(run)
    db.session.flush()

    queues = _get_queues()
    q = queues.get(priority, queues["default"])
    job = q.enqueue(
        _EXECUTE_FLOW_JOB,
        job_id=job_id,
        flow_id=flow_id,
        run_id=run.id,
        input_data=input_data or {},
        start_node_id=start_node_id,
        replay_data=replay_data,
        webhook=webhook,
        retry=Retry(max=2, interval=[10, 30]),
        result_ttl=86400,
        failure_ttl=86400,
        ttl=600,
    )
    return job.id

def queue_form_run(tenant_id: str, flow_id: int, node_id: str,
                   form_output: dict, data_bus: dict,
                   session_token: str = None, existing_run_id: str = None) -> str:
    job_id = str(uuid.uuid4())

    if not existing_run_id:
        run = Run(
            flow_id=flow_id,
            status="queued",
            started_at=datetime.utcnow(),
            job_id=job_id
        )
        db.session.add(run)
        db.session.flush()
        run_id = run.id
    else:
        run_id = existing_run_id
        run = Run.query.get(run_id)
        if run:
            run.job_id = job_id
            run.status = "running"
            db.session.flush()

    queues = _get_queues()
    q = queues.get("high", queues["default"])
    job = q.enqueue(
        "worker.job_handler.execute_form_job",
        kwargs={
            "tenant_id": tenant_id,
            "flow_id": flow_id,
            "node_id": node_id,
            "form_output": form_output,
            "data_bus": data_bus,
            "session_token": session_token,
            "run_id": run_id,
        },
        job_id=job_id,
        retry=Retry(max=1, interval=[5]),
        result_ttl=86400,
        failure_ttl=86400,
        ttl=300,
    )
    return job.id

def get_job_status(job_id: str) -> dict:
    try:
        job = Job.fetch(job_id, connection=current_app.redis_conn)
    except Exception as e:
        current_app.logger.error(e)
        return {"job_id": job_id, "status": "not found - connection issue"}

    status = job.get_status()
    result = None
    error = None
    if status == "finished":
        result = job.result
    elif status == "failed":
        error = str(job.exc_info) if job.exc_info else "Unknown error"

    return {
        "job_id":      job_id,
        "status":      status,
        "result":      result,
        "error":       error,
        "enqueued_at": job.enqueued_at.isoformat() if job.enqueued_at else None,
        "started_at":  job.started_at.isoformat() if job.started_at else None,
        "ended_at":    job.ended_at.isoformat() if job.ended_at else None,
    }


def get_queue_stats() -> dict:
    queues = _get_queues()
    workers = Worker.all(connection=current_app.redis_conn)
    return {
        "queues": {
            name: {"size": q.count, "failed": q.failed_job_registry.count}
            for name, q in queues.items()
        },
        "workers": {
            "total": len(workers),
            "busy":  sum(1 for w in workers if w.get_current_job() is not None),
            "idle":  sum(1 for w in workers if w.get_current_job() is None),
            "details": [
                {
                    "name":        w.name,
                    "state":       w.get_state(),
                    "queues":      [q.name for q in w.queues],
                    "current_job": str(w.get_current_job()) if w.get_current_job() else None,
                }
                for w in workers
            ],
        },
    }