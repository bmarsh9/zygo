"""
job_handler.py
==============
The function RQ workers execute.

All database interaction has been removed. Instead this module calls the
Flask app's internal API to:
  1. Create a Run record before execution starts  (POST /internal/api/runs)
  2. Finalise the Run + write logs after execution (PATCH /internal/api/runs/<id>)
  3. Fetch flow data before execution             (GET  /internal/api/flows/<id>/data)

Required env vars (set via docker-compose):
    APP_BASE_URL   e.g. http://app:9000
"""

import traceback
from datetime import datetime, timezone
import requests
from flowcore.flow_runner import FlowRunner
from worker.config import Config


# ── internal API helpers ──────────────────────────────────────────────────────

def _api(method: str, path: str, **kwargs):
    """Make a request to the Flask internal API."""
    url = f"{Config.APP_BASE_URL.rstrip('/')}{path}"
    headers = kwargs.pop("headers", {})
    headers["X-Internal-Secret"] = Config.INTERNAL_API_SECRET
    resp = requests.request(method, url, timeout=30, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _get_flow_data(flow_id: int):
    """Fetch nodes + edges from the Flask app."""
    return _api("GET", f"/internal/api/flows/{flow_id}/data")


def _mark_running(run_id: int):
    _api("PATCH", f"/internal/api/runs/{run_id}", json={
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
    })


def _finish_run(run_id: int, status: str, output, error: str | None, logs: list):
    """Update the Run record and write all logs."""
    _api("PATCH", f"/internal/api/runs/{run_id}", json={
        "status":      status,
        "output":      output,
        "error":       error,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "logs":        logs,
    })


# ── job entrypoint ────────────────────────────────────────────────────────────

def execute_flow_job(
    flow_id: int,
    input_data: dict = None,
    start_node_id: str = None,
    replay_data: dict = None,
    webhook: bool = False,
    run_id: int = None,       # provided by queue_flow_run
) -> dict:
    print(f"[Worker] execute_flow_job called: flow_id={flow_id}, start_node_id={start_node_id}, run_id={run_id}")

    # 1. fetch flow data
    try:
        flow_data = _get_flow_data(flow_id)
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return {"status": "error", "error": f"Flow {flow_id} not found", "run_id": run_id}
        raise
    except Exception as e:
        return {"status": "error", "error": f"Could not fetch flow data: {e}", "run_id": run_id}

    nodes = flow_data.get("nodes")
    if not nodes:
        return {"status": "error", "error": f"Flow {flow_id} has no nodes", "run_id": run_id}

    # 2. mark run as running
    if run_id is not None:
        try:
            _mark_running(run_id)
        except Exception as e:
            print(f"[Worker] WARNING: could not mark Run {run_id} as running: {e}")

    # 3. execute the flow
    result = None
    try:
        runner = FlowRunner(
            {"tenant_id": flow_data["tenant_id"], "nodes": flow_data["nodes"], "edges": flow_data.get("edges", [])},
            start_node_id=start_node_id,
        )
        result = runner.run(input_data=input_data or {}, replay_data=replay_data)

    except SystemExit:
        result = {"status": "cancelled", "output": None, "error": "Run cancelled", "logs": []}

    except Exception as e:
        traceback.print_exc()
        result = {"status": "error", "output": None, "error": str(e), "logs": []}

    finally:
        if result is None:
            result = {"status": "cancelled", "output": None, "error": "Job cancelled", "logs": []}

        # 4. finalise run record
        if run_id is not None:
            try:
                _finish_run(
                    run_id=run_id,
                    status=result["status"],
                    output=result.get("output"),
                    error=result.get("error"),
                    logs=result.get("logs", []),
                )
            except Exception as e:
                print(f"[Worker] WARNING: could not finalise Run {run_id}: {e}")

        result["run_id"] = run_id

    return result

def execute_form_job(
    tenant_id: str,
    flow_id: int,
    node_id: str,
    form_output: dict,
    data_bus: dict,
    session_token: str = None,
    run_id: str = None,
) -> dict:
    import json
    import uuid

    print(f"[Worker] execute_form_job: flow={flow_id}, node={node_id}, run={run_id}")

    # 1. Fetch flow data
    try:
        flow_data = _get_flow_data(flow_id)
    except Exception as e:
        _finish_run(run_id, "error", None, str(e), [])
        return {"status": "error", "error": str(e)}

    nodes_list = flow_data.get("nodes", [])
    edges = flow_data.get("edges", [])
    node_configs = {n["node_id"]: n.get("config", {}) for n in nodes_list}
    form_node_ids = [n["node_id"] for n in nodes_list if n["node_type"] == "webform"]

    # 2. Mark running
    if run_id:
        try:
            _mark_running(run_id)
        except Exception:
            pass

    # 3. Dispatch webform node
    try:
        from flowcore.nodes import dispatch
        webform_output = dispatch(tenant_id, flow_id, "webform", node_configs.get(node_id, {}), form_output)
    except Exception:
        webform_output = form_output

    data_bus[node_id] = webform_output

    # 4. Run flow from this node
    try:
        runner = FlowRunner(
            {"tenant_id": tenant_id, "nodes": nodes_list, "edges": edges},
            start_node_id=node_id,
        )
        result = runner.run_until_form(
            input_data=form_output, data_bus=data_bus, skip_node=node_id
        )
    except Exception as e:
        _finish_run(run_id, "error", None, str(e), [])
        return {"status": "error", "error": str(e)}

    # 5. Build logs for the webform node itself
    form_data = form_output.get("form_data", {})
    webform_logs = [
        {"node_id": node_id, "level": "info", "message": f"▶  Web Form  [webform]",
         "detail": json.dumps({"input_keys": list(form_data.keys())})},
        {"node_id": node_id, "level": "input", "message": "__node_input__",
         "detail": json.dumps(form_output, default=str)},
        {"node_id": node_id, "level": "success", "message": f"✓  Web Form  [webform]",
         "detail": json.dumps(webform_output, default=str)},
        {"node_id": node_id, "level": "output", "message": "__node_output__",
         "detail": json.dumps(webform_output, default=str)},
    ]
    all_logs = webform_logs + result.get("logs", [])

    # 6. Handle next form (paused_at)
    next_form_id = result.get("paused_at")
    response = {"status": result["status"], "run_id": run_id}

    if next_form_id:
        current_step = form_node_ids.index(node_id) if node_id in form_node_ids else 0
        next_step = form_node_ids.index(next_form_id) if next_form_id in form_node_ids else current_step + 1
        token = session_token or uuid.uuid4().hex[:16]

        # Find the DB primary key for the next form node
        node_id_to_pk = {n["node_id"]: n["id"] for n in nodes_list}

        # Update or create session via internal API
        _api("POST", "/internal/api/form-sessions", json={
            "token": token,
            "tenant_id": tenant_id,
            "flow_id": flow_id,
            "run_id": run_id,
            "current_node_id": next_form_id,
            "step_index": next_step,
            "total_steps": len(form_node_ids),
            "data_bus": result.get("data_bus", data_bus),
            "status": "active",
        })

        status = "waiting"
        response["redirect"] = f"/form/{node_id_to_pk.get(next_form_id, next_form_id)}?session={token}"
        response["session_token"] = token
        response["step"] = next_step + 1
        response["total_steps"] = len(form_node_ids)
        response["status"] = "waiting"

        _finish_run(run_id, status, result.get("output"), result.get("error"), all_logs)
    else:
        # No more forms — complete the session
        if session_token:
            try:
                _api("POST", "/internal/api/form-sessions/complete", json={
                    "token": session_token,
                    "tenant_id": tenant_id,
                })
            except Exception:
                pass

        redirect_url = node_configs.get(node_id, {}).get("redirect", "").strip()
        response["redirect"] = redirect_url or None
        response["status"] = result["status"]

        _finish_run(run_id, result["status"], result.get("output"), result.get("error"), all_logs)

    return response