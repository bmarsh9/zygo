"""
job_handler.py
==============
The function RQ workers execute. Lives in the worker package so the
Flask app and scheduler don't need to import it.
"""

import json
import traceback
from datetime import datetime

from flask import current_app
from rq import get_current_job

from app import db
from app.models import Run, RunLog
from app.flows.flow_runner import FlowRunner
from app.flows.flow_helpers import load_flow_data


def execute_flow_job(flow_id: int, input_data: dict = None, start_node_id: str = None,
                     replay_data: dict = None, webhook: bool = False) -> dict:
    with current_app.app_context():
        nodes, edges, tenant_id = load_flow_data(flow_id)
        if nodes is None:
            return {"status": "error", "error": f"Flow {flow_id} not found"}

        run = Run(flow_id=flow_id, status="running", started_at=datetime.utcnow())
        db.session.add(run)
        db.session.flush()

        current_job = get_current_job()
        if current_job:
            run.job_id = current_job.id
        db.session.commit()

        result = None
        try:
            runner = FlowRunner({"tenant_id": tenant_id, "nodes": nodes, "edges": edges}, start_node_id=start_node_id)
            result = runner.run(input_data=input_data or {}, replay_data=replay_data)
        except SystemExit:
            result = {"status": "cancelled", "output": None, "error": "Run cancelled", "logs": []}
        except Exception as e:
            traceback.print_exc()
            result = {"status": "error", "output": None, "error": str(e), "logs": []}
        finally:
            if result is None:
                result = {"status": "cancelled", "output": None, "error": "Job cancelled", "logs": []}

            run.status = result["status"]
            run.finished_at = datetime.utcnow()
            run.output = json.dumps(result.get("output"), default=str)
            run.error = result.get("error")

            for log in result.get("logs", []):
                iter_path = log.get("iteration_path")
                db.session.add(RunLog(
                    run_id=run.id,
                    node_id=log["node_id"],
                    level=log["level"],
                    message=log["message"],
                    detail=log.get("detail", ""),
                    iteration=log.get("iteration"),
                    iteration_path=json.dumps(iter_path) if iter_path is not None else None,
                ))

            db.session.commit()
            result["run_id"] = run.id

        return result