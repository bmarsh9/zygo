import json
from datetime import datetime

from flask import Blueprint, jsonify, request
from app.utils.decorators import internal_api_required
from app import db
from app.models import (
    Credential, DataRecord, Edge, Flow, Node, NodeConfig,
    Run, RunLog, Ticket, FormSession
)
from app.flows.integrations import get_action_def
from . import internal_api


# ── helpers ───────────────────────────────────────────────────────────────────

def _load_flow_data(flow_id: int):
    flow = Flow.query.get(flow_id)
    if not flow:
        return None, None, None
    node_configs = {
        nc.node_id: json.loads(nc.config_json)
        for nc in NodeConfig.query.filter_by(flow_id=flow_id).all()
    }
    nodes = []
    for n in Node.query.filter_by(flow_id=flow_id).all():
        nd = n.to_dict()
        nd["config"] = node_configs.get(n.node_id, {})
        if nd.get("action_id"):
            nd["config"]["action_id"] = nd["action_id"]
        nodes.append(nd)
    edges = [e.to_dict() for e in Edge.query.filter_by(flow_id=flow_id).all()]
    return nodes, edges, flow.tenant_id


# ══════════════════════════════════════════════════════════════════════════════
# Flow data
# ══════════════════════════════════════════════════════════════════════════════

@internal_api.get("/flows/<string:flow_id>/data")
@internal_api_required
def get_flow_data(flow_id: int):
    nodes, edges, tenant_id = _load_flow_data(flow_id)
    if nodes is None:
        return jsonify({"error": f"Flow {flow_id} not found"}), 404
    return jsonify({"tenant_id": tenant_id, "nodes": nodes, "edges": edges})


# ══════════════════════════════════════════════════════════════════════════════
# Scheduling
# ══════════════════════════════════════════════════════════════════════════════

@internal_api.get("/scheduled-flows")
@internal_api_required
def get_scheduled_flows():
    flows = {f.id: f for f in Flow.query.all()}
    flow_ids = list(flows.keys())

    targets_by_flow = {}
    for e in Edge.query.filter(Edge.flow_id.in_(flow_ids)).all():
        targets_by_flow.setdefault(e.flow_id, set()).add(e.target_node_id)

    configs_by_flow = {}
    for nc in NodeConfig.query.filter(NodeConfig.flow_id.in_(flow_ids)).all():
        configs_by_flow.setdefault(nc.flow_id, {})[nc.node_id] = json.loads(nc.config_json)

    all_nodes = Node.query.filter(Node.flow_id.in_(flow_ids)).all()

    scheduled = []
    seen = set()
    for node in all_nodes:
        if node.flow_id in seen:
            continue
        if node.node_id in targets_by_flow.get(node.flow_id, set()):
            continue
        cfg = configs_by_flow.get(node.flow_id, {}).get(node.node_id, {})
        cron = cfg.get("cron_schedule", "").strip()
        if cron and cfg.get("cron_enabled") in (True, "true", "True"):
            flow = flows[node.flow_id]
            scheduled.append({
                "flow_id": flow.id,
                "flow_name": flow.name,
                "cron": cron,
            })
            seen.add(node.flow_id)

    return jsonify(scheduled)


# ══════════════════════════════════════════════════════════════════════════════
# Runs
# ══════════════════════════════════════════════════════════════════════════════

@internal_api.post("/runs")
@internal_api_required
def create_run():
    body = request.get_json(force=True)
    flow_id = body.get("flow_id")
    if not flow_id:
        return jsonify({"error": "flow_id is required"}), 400
    started_at = body.get("started_at")
    try:
        started_at = datetime.fromisoformat(started_at) if started_at else datetime.utcnow()
    except ValueError:
        started_at = datetime.utcnow()
    run = Run(
        flow_id=flow_id,
        status="running",
        started_at=started_at,
        job_id=body.get("job_id"),
    )
    db.session.add(run)
    db.session.commit()
    return jsonify({"run_id": run.id}), 201


@internal_api.patch("/runs/<run_id>")
@internal_api_required
def finish_run(run_id):
    run = Run.query.get(run_id)
    if not run:
        return jsonify({"error": f"Run {run_id} not found"}), 404

    body = request.get_json(force=True)

    if "status" in body:
        run.status = body["status"]
    if "output" in body:
        run.output = json.dumps(body.get("output"), default=str)
    if "error" in body:
        run.error = body["error"]
    if "started_at" in body:
        try:
            run.started_at = datetime.fromisoformat(body["started_at"])
        except ValueError:
            pass
    if "finished_at" in body:
        try:
            run.finished_at = datetime.fromisoformat(body["finished_at"])
        except ValueError:
            run.finished_at = datetime.utcnow()

    for log in body.get("logs", []):
        iter_path = log.get("iteration_path")
        db.session.add(RunLog(
            run_id=run_id,
            node_id=log["node_id"],
            level=log["level"],
            message=log["message"],
            detail=log.get("detail", ""),
            iteration=log.get("iteration"),
            iteration_path=json.dumps(iter_path) if iter_path is not None else None,
        ))

    db.session.commit()
    return jsonify({"ok": True, "run_id": run_id})

@internal_api.route("/form-sessions", methods=["POST"])
@internal_api_required
def upsert_form_session():
    data = request.json or {}
    token = data["token"]
    fs = FormSession.query.filter_by(token=token).first()
    if not fs:
        fs = FormSession(
            token=token,
            tenant_id=data["tenant_id"],
            flow_id=data["flow_id"],
            current_node_id=data["current_node_id"],
            step_index=data.get("step_index", 0),
            total_steps=data.get("total_steps", 1),
        )
        db.session.add(fs)
    fs.run_id = data.get("run_id")
    fs.current_node_id = data["current_node_id"]
    fs.step_index = data.get("step_index", 0)
    fs.total_steps = data.get("total_steps", 1)
    fs.status = data.get("status", "active")
    if "data_bus" in data:
        fs.set_data_bus(data["data_bus"])
    db.session.commit()
    return jsonify({"ok": True})


@internal_api.route("/form-sessions/complete", methods=["POST"])
@internal_api_required
def complete_form_session():
    data = request.json or {}
    fs = FormSession.query.filter_by(
        token=data["token"], tenant_id=data["tenant_id"]
    ).first()
    if fs:
        fs.status = "completed"
        db.session.commit()
    return jsonify({"ok": True})
# ══════════════════════════════════════════════════════════════════════════════
# Table operations
# ══════════════════════════════════════════════════════════════════════════════

@internal_api.post("/tables/records")
@internal_api_required
def table_insert():
    body = request.get_json(force=True)
    tenant_id = body.get("tenant_id")
    table_name = body.get("table_name")
    record_key = body.get("record_key") or None
    record_data = body.get("data", {})
    if not table_name:
        return jsonify({"error": "table_name is required"}), 400
    record = DataRecord(
        tenant_id=tenant_id,
        table_name=table_name,
        record_key=record_key,
        data=json.dumps(record_data, default=str),
    )
    db.session.add(record)
    db.session.commit()
    return jsonify({
        "success": True, "action": "insert",
        "table": table_name, "id": record.id,
        "key": record_key, "data": record_data,
    })


@internal_api.post("/tables/records/get")
@internal_api_required
def table_get():
    body = request.get_json(force=True)
    table_name = body.get("table_name")
    record_key = body.get("record_key")
    filters = body.get("filters", {})
    if not table_name:
        return jsonify({"error": "table_name is required"}), 400
    if not record_key and not filters:
        return jsonify({"error": "record_key or filters is required"}), 400

    if record_key:
        record = DataRecord.query.filter_by(
            table_name=table_name, record_key=record_key
        ).order_by(DataRecord.id.desc()).first()
    else:
        record = None
        for r in DataRecord.query.filter_by(table_name=table_name).all():
            item = json.loads(r.data)
            if all(str(item.get(k, "")) == v for k, v in filters.items()):
                record = r
                break

    if not record:
        return jsonify({
            "success": False, "action": "get",
            "table": table_name, "key": record_key,
            "data": None, "found": False, "filters": filters,
        })
    return jsonify({
        "success": True, "action": "get",
        "table": table_name, "id": record.id,
        "key": record.record_key, "data": json.loads(record.data), "found": True,
    })


@internal_api.post("/tables/records/list")
@internal_api_required
def table_list():
    body = request.get_json(force=True)
    table_name = body.get("table_name")
    filters = body.get("filters", {})
    limit = int(body.get("limit", 100))
    if not table_name:
        return jsonify({"error": "table_name is required"}), 400

    query = DataRecord.query.filter_by(table_name=table_name)
    for fk in filters:
        query = query.filter(DataRecord.data.contains(f'"{fk}"'))

    items = []
    for r in query.order_by(DataRecord.id.desc()).limit(limit * 2).all():
        item = json.loads(r.data)
        if filters and not all(str(item.get(k, "")) == v for k, v in filters.items()):
            continue
        item["__id__"] = r.id
        item["__key__"] = r.record_key
        items.append(item)
        if len(items) >= limit:
            break

    return jsonify({
        "success": True, "action": "list",
        "table": table_name, "count": len(items), "items": items,
    })


@internal_api.post("/tables/records/update")
@internal_api_required
def table_update():
    body = request.get_json(force=True)
    table_name = body.get("table_name")
    record_key = body.get("record_key")
    new_data = body.get("data", {})
    if not table_name:
        return jsonify({"error": "table_name is required"}), 400
    if not record_key:
        return jsonify({"error": "record_key is required for update"}), 400

    record = DataRecord.query.filter_by(
        table_name=table_name, record_key=record_key
    ).order_by(DataRecord.id.desc()).first()
    if not record:
        return jsonify({
            "success": False,
            "error": f"Record with key '{record_key}' not found in '{table_name}'",
        }), 404

    existing = json.loads(record.data)
    existing.update(new_data)
    record.data = json.dumps(existing, default=str)
    db.session.commit()
    return jsonify({
        "success": True, "action": "update",
        "table": table_name, "id": record.id,
        "key": record_key, "data": existing,
    })


@internal_api.post("/tables/records/delete")
@internal_api_required
def table_delete():
    body = request.get_json(force=True)
    table_name = body.get("table_name")
    record_key = body.get("record_key")
    if not table_name:
        return jsonify({"error": "table_name is required"}), 400

    if record_key:
        deleted = DataRecord.query.filter_by(
            table_name=table_name, record_key=record_key
        ).delete()
    else:
        deleted = DataRecord.query.filter_by(table_name=table_name).delete()

    db.session.commit()
    return jsonify({
        "success": True, "action": "delete",
        "table": table_name, "key": record_key or "__all__",
        "deleted_count": deleted,
    })


# ══════════════════════════════════════════════════════════════════════════════
# Credentials
# ══════════════════════════════════════════════════════════════════════════════
@internal_api.route("/credentials/<tenant_id>/<name>", methods=["GET"])
@internal_api_required
def get_credential_by_name(tenant_id, name):
    cred = Credential.query.filter_by(tenant_id=tenant_id, name=name).first()
    if not cred:
        return jsonify({"error": "Credential not found"}), 404
    return jsonify({"fields": cred.decrypted()})

# ══════════════════════════════════════════════════════════════════════════════
# Integrations
# ══════════════════════════════════════════════════════════════════════════════

@internal_api.get("/integrations/<action_id>")
@internal_api_required
def get_integration_action(action_id: str):
    """
    Returns the action definition for a given action_id.
    The 'handler' callable is not serialisable — we return has_handler=True
    instead so the worker knows to delegate execution back via /run.
    """
    action_def = get_action_def(action_id)
    if not action_def:
        return jsonify({"error": f"Action '{action_id}' not found"}), 404
    safe = {k: v for k, v in action_def.items() if k != "handler"}
    safe["has_handler"] = callable(action_def.get("handler"))
    return jsonify({"action": safe})


@internal_api.post("/integrations/<action_id>/run")
@internal_api_required
def run_integration_action(action_id: str):
    """
    Executes a built-in handler action server-side where db / current_app
    are available. Called when flowcore/nodes.py detects has_handler=True.
    """
    action_def = get_action_def(action_id)
    if not action_def or not callable(action_def.get("handler")):
        return jsonify({"error": f"No handler for action '{action_id}'"}), 404
    body = request.get_json(force=True)
    try:
        result = action_def["handler"](body.get("config", {}), body.get("input_data", {}))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# Tickets
# ══════════════════════════════════════════════════════════════════════════════

@internal_api.post("/tickets")
@internal_api_required
def create_ticket():
    body = request.get_json(force=True)
    ticket = Ticket(
        title=body.get("title", "Untitled"),
        status=body.get("status", "open"),
        priority=body.get("priority", "medium"),
        assignee=body.get("assignee"),
        tags=body.get("tags", []),
        content=body.get("content", {}),
        meta=body.get("meta", {}),
        flow_id=body.get("flow_id"),
        tenant_id=body.get("tenant_id"),
    )
    db.session.add(ticket)
    db.session.commit()
    return jsonify({
        "success": True, "action": "create",
        "ticket": ticket.to_dict(), "ticket_id": ticket.id,
        "ticket_url": f"/tickets/{ticket.id}",
    }), 201


@internal_api.get("/tickets")
@internal_api_required
def list_tickets():
    query = Ticket.query.order_by(Ticket.created_at.desc())
    if request.args.get("status"):
        query = query.filter_by(status=request.args["status"])
    if request.args.get("priority"):
        query = query.filter_by(priority=request.args["priority"])
    if request.args.get("assignee"):
        query = query.filter_by(assignee=request.args["assignee"])
    limit = request.args.get("limit", 100, type=int)
    tickets = query.limit(limit).all()
    return jsonify({
        "success": True, "action": "list",
        "tickets": [t.to_dict() for t in tickets], "count": len(tickets),
    })


@internal_api.get("/tickets/<string:ticket_id>")
@internal_api_required
def get_ticket(ticket_id: int):
    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"success": False, "error": f"Ticket {ticket_id} not found"}), 404
    return jsonify({"success": True, "action": "get", "ticket": ticket.to_dict()})


@internal_api.patch("/tickets/<string:ticket_id>")
@internal_api_required
def update_ticket(ticket_id: int):
    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"success": False, "error": f"Ticket {ticket_id} not found"}), 404
    body = request.get_json(force=True)
    for field in ("title", "status", "priority", "assignee"):
        if field in body:
            setattr(ticket, field, body[field])
    if "tags" in body:
        ticket.tags = body["tags"]
    if "meta" in body:
        existing = ticket.meta or {}
        existing.update(body["meta"])
        ticket.meta = existing
    if "content_blocks" in body:
        existing_content = ticket.content or {"blocks": []}
        existing_content["blocks"].extend(body["content_blocks"])
        ticket.content = existing_content
    db.session.commit()
    return jsonify({"success": True, "action": "update",
                    "ticket": ticket.to_dict(), "ticket_id": ticket.id})


@internal_api.delete("/tickets/<string:ticket_id>")
@internal_api_required
def delete_ticket(ticket_id: int):
    ticket = Ticket.query.get(ticket_id)
    if not ticket:
        return jsonify({"success": False, "error": f"Ticket {ticket_id} not found"}), 404
    db.session.delete(ticket)
    db.session.commit()
    return jsonify({"success": True, "action": "delete", "ticket_id": ticket_id})