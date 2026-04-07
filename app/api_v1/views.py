import copy
import json
import random
from datetime import datetime, timedelta

from flask import jsonify, request
from flask_login import current_user
from rq.command import send_stop_job_command
from rq.job import Job

from . import api
from app.models import *
from app.utils.authorizer import Authorizer
from app.utils.decorators import login_required
from app.flows.integrations import INTEGRATIONS
from flowcore.resolve import _resolve, set_resolve_context
from flowcore.operators import API_SCHEMA as OPERATORS_SCHEMA
from app.flows.flow_helpers import _do_submit_form, _enforce_form_auth
from app.flows.job_queue import get_job_status, get_queue_stats, queue_flow_run
from app import limiter
import re

# ── Integrations & Operators ──────────────────────────────────────────────────

@api.route("/integrations", methods=["GET"])
@login_required
def get_integrations():
    safe = copy.deepcopy(INTEGRATIONS)
    for intg in safe:
        for group in intg.get("actions", []):
            for item in group.get("items", []):
                for key in ("handler", "method", "url", "body", "params"):
                    item.pop(key, None)
    return jsonify(safe)


@api.route("/operators", methods=["GET"])
@login_required
def get_operators():
    return jsonify(OPERATORS_SCHEMA)


# ── Jobs ──────────────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/jobs/<job_id>", methods=["GET"])
@login_required
def get_job(tenant_id, job_id):
    auth = Authorizer(current_user)
    run = Run.query.filter_by(job_id=job_id).first()
    if not run:
        return jsonify({"error": "Not found"}), 404
    auth.flow(run.flow_id, role="viewer", tenant_id=tenant_id)
    return jsonify(get_job_status(job_id))


@api.route("/tenants/<tenant_id>/jobs/<job_id>/cancel", methods=["POST"])
@login_required
def cancel_job(tenant_id, job_id):
    auth = Authorizer(current_user)
    run = Run.query.filter_by(job_id=job_id).first()
    if not run:
        return jsonify({"error": "Not found"}), 404
    auth.flow(run.flow_id, role="editor", tenant_id=tenant_id)
    try:
        job = Job.fetch(job_id, connection=current_app.redis_conn)
        if job.get_status() in ("started", "running"):
            send_stop_job_command(current_app.redis_conn, job_id)
        else:
            job.cancel()
        if run.status == "running":
            run.status = "cancelled"
            run.finished_at = datetime.utcnow()
            run.error = "Cancelled by user"
            db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 404


@api.route("/tenants/<tenant_id>/queue/stats", methods=["GET"])
@login_required
def queue_stats(tenant_id):
    Authorizer(current_user).assert_super()
    return jsonify(get_queue_stats())


# ── Flows ─────────────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/flows", methods=["GET"])
@login_required
def list_flows(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    limit = request.args.get("limit", 50, type=int)
    q = Flow.query.filter_by(tenant_id=tenant_id).order_by(Flow.updated_at.desc())
    if limit:
        q = q.limit(limit)
    return jsonify([f.to_dict() for f in q.all()])

@api.route("/tenants/<tenant_id>/web-forms", methods=["GET"])
@login_required
def list_webform_flows(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="user")
    results = (
        db.session.query(Flow, Node)
        .join(Node, Node.flow_id == Flow.id)
        .filter(Flow.tenant_id == tenant_id, Node.node_type == "webform")
        .order_by(Flow.updated_at.desc())
        .all()
    )
    flows = []
    for flow, node in results:  # ← fixed: was unpacking 3 values
        flows.append({
            **flow.to_dict(),
            "node_id": node.id,
            "form_url": f"/forms/{node.id}",
        })
    return jsonify(flows)

@api.route("/tenants/<tenant_id>/flows", methods=["POST"])
@login_required
def create_flow(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    data = request.json or {}
    flow = Flow(tenant_id=tenant_id, name=data.get("name", "Untitled Flow"), user_id=current_user.id)
    db.session.add(flow)
    db.session.commit()
    return jsonify(flow.to_dict()), 201


@api.route("/tenants/<tenant_id>/flows/<flow_id>", methods=["GET"])
@login_required
def get_flow(tenant_id, flow_id):
    auth = Authorizer(current_user)
    flow = auth.flow(flow_id, role="viewer", tenant_id=tenant_id)
    node_configs = {nc.node_id: json.loads(nc.config_json) for nc in NodeConfig.query.filter_by(flow_id=flow_id).all()}
    nodes = [{**n.to_dict(), "config": node_configs.get(n.node_id, {})} for n in flow.nodes]
    notes = json.loads(flow.notes) if flow.notes else []
    return jsonify({**flow.to_dict(), "nodes": nodes, "edges": [e.to_dict() for e in flow.edges], "notes": notes})


@api.route("/tenants/<tenant_id>/flows/<flow_id>", methods=["PUT"])
@login_required
def update_flow(tenant_id, flow_id):
    auth = Authorizer(current_user)
    flow = auth.flow(flow_id, role="editor", tenant_id=tenant_id)
    data = request.json or {}
    if "name" in data:
        flow.name = data["name"]
    if "folder" in data:
        flow.folder = data["folder"]
    flow.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify(flow.to_dict())


@api.route("/tenants/<tenant_id>/flows/<flow_id>", methods=["DELETE"])
@login_required
def delete_flow(tenant_id, flow_id):
    auth = Authorizer(current_user)
    flow = auth.flow(flow_id, role="editor", tenant_id=tenant_id)
    db.session.delete(flow)
    db.session.commit()
    return jsonify({"ok": True})


@api.route("/tenants/<tenant_id>/flows/<flow_id>", methods=["POST"])
@login_required
def save_flow(tenant_id, flow_id):
    auth = Authorizer(current_user)
    flow = auth.flow(flow_id, role="editor", tenant_id=tenant_id)
    data = request.json or {}

    incoming_node_ids = {str(n["node_id"]) for n in data.get("nodes", [])}

    # Delete nodes no longer on the canvas
    Node.query.filter(
        Node.flow_id == flow_id,
        Node.node_id.notin_(incoming_node_ids)
    ).delete(synchronize_session=False)
    db.session.flush()

    # Upsert nodes
    existing_nodes = {n.node_id: n for n in Node.query.filter_by(flow_id=flow_id).all()}
    for n in data.get("nodes", []):
        node_id = str(n["node_id"])
        if node_id in existing_nodes:
            node = existing_nodes[node_id]
            node.name = n.get("name", "Node")
            node.label = n.get("label", "")
            node.node_type = n.get("node_type", "default")
            node.pos_x = n.get("pos_x", 0)
            node.pos_y = n.get("pos_y", 0)
            node.inputs = n.get("inputs", 1)
            node.outputs = n.get("outputs", 1)
            node.has_failure_path = n.get("has_failure_path", False)
            node.action_id = n.get("action_id")
            node.action_name = n.get("action_name")
            node.description = n.get("description")
        else:
            db.session.add(Node(
                flow_id=flow_id, node_id=node_id, name=n.get("name", "Node"),
                label=n.get("label", ""), node_type=n.get("node_type", "default"),
                pos_x=n.get("pos_x", 0), pos_y=n.get("pos_y", 0),
                inputs=n.get("inputs", 1), outputs=n.get("outputs", 1),
                has_failure_path=n.get("has_failure_path", False),
                action_id=n.get("action_id"), action_name=n.get("action_name"),
                description=n.get("description"),
            ))

    # Upsert edges
    existing_edges = {
        (e.source_node_id, e.source_output, e.target_node_id, e.target_input): e
        for e in Edge.query.filter_by(flow_id=flow_id).all()
    }
    incoming_edges = {
        (str(e["source_node_id"]), e["source_output"], str(e["target_node_id"]), e["target_input"])
        for e in data.get("edges", [])
    }
    for key, edge in existing_edges.items():
        if key not in incoming_edges:
            db.session.delete(edge)
    for e in data.get("edges", []):
        key = (str(e["source_node_id"]), e["source_output"], str(e["target_node_id"]), e["target_input"])
        if key not in existing_edges:
            db.session.add(Edge(
                flow_id=flow_id,
                source_node_id=str(e["source_node_id"]),
                source_output=e["source_output"],
                target_node_id=str(e["target_node_id"]),
                target_input=e["target_input"],
            ))

    # Upsert node configs — only touches config_json, never form_json
    existing_configs = {nc.node_id: nc for nc in NodeConfig.query.filter_by(flow_id=flow_id).all()}
    for nc in data.get("node_configs", []):
        node_id = str(nc["node_id"])
        incoming_config = nc.get("config", {})
        if node_id in existing_configs:
            existing_configs[node_id].config_json = json.dumps(incoming_config)
        else:
            db.session.add(NodeConfig(
                flow_id=flow_id,
                node_id=node_id,
                config_json=json.dumps(incoming_config)
            ))

    # Delete configs for nodes no longer on canvas
    NodeConfig.query.filter(
        NodeConfig.flow_id == flow_id,
        NodeConfig.node_id.notin_(incoming_node_ids)
    ).delete(synchronize_session=False)

    if data.get("name"):
        flow.name = data["name"]
    if data.get("publish"):
        flow.is_published = True
    flow.notes = json.dumps(data.get("notes", []))
    flow.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify({
        "ok": True,
        "nodes": [{"node_id": n.node_id, "id": n.id} for n in Node.query.filter_by(flow_id=flow_id).all()],
        "edges": len(data.get("edges", [])),
        "notes": data.get("notes", [])
    })

@api.route("/tenants/<tenant_id>/flows/<flow_id>/run", methods=["POST"])
@login_required
def run_flow(tenant_id, flow_id):
    auth = Authorizer(current_user)
    auth.flow(flow_id, role="editor", tenant_id=tenant_id)
    body = request.json or {}
    job_id = queue_flow_run(
        flow_id=flow_id, input_data=body.get("input_data", {}),
        start_node_id=body.get("start_node_id"), replay_data=body.get("replay_data"),
    )
    return jsonify({"job_id": job_id, "status": "queued"})

@api.route("/tenants/<tenant_id>/flows/<flow_id>/toggle-publish", methods=["POST"])
@login_required
def toggle_publish(tenant_id, flow_id):
    auth = Authorizer(current_user)
    flow = auth.flow(flow_id, role="editor", tenant_id=tenant_id)
    flow.is_published = not flow.is_published
    db.session.commit()
    return jsonify({"ok": True, "is_published": flow.is_published})

@api.route("/tenants/<tenant_id>/flows/<flow_id>/runs", methods=["GET"])
@login_required
def list_runs(tenant_id, flow_id):
    auth = Authorizer(current_user)
    auth.flow(flow_id, role="viewer", tenant_id=tenant_id)
    limit = request.args.get("limit", 50, type=int)
    return jsonify([r.to_dict() for r in Run.query.filter_by(flow_id=flow_id).order_by(Run.created_at.desc()).limit(limit).all()])

@api.route("/tenants/<tenant_id>/runs/<run_id>", methods=["GET"])
@login_required
def get_run(tenant_id, run_id):
    auth = Authorizer(current_user)
    run = auth.run(run_id, role="viewer", tenant_id=tenant_id)
    return jsonify(run.to_dict(include_logs=True))


# ── Credentials ───────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/credentials", methods=["GET"])
@login_required
def list_credentials(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    credentials = Credential.query.filter_by(tenant_id=tenant_id).order_by(Credential.name).all()
    return jsonify([c.to_dict(masked=True) for c in credentials])

@api.route("/tenants/<tenant_id>/credentials", methods=["POST"])
@login_required
def create_credential(tenant_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="editor")
    data = request.json or {}
    name = data.get("name", "").strip().lower()
    if not name or not re.match(r'^[a-z0-9_]+$', name):
        return jsonify({"error": "Name must be lowercase letters, numbers, and underscores only"}), 400
    if Credential.query.filter_by(tenant_id=tenant_id, name=name).first():
        return jsonify({"error": "A credential with this name already exists"}), 409
    cred = Credential(
        tenant_id=tenant_id, name=name,
        integration=(data.get("integration") or "").strip() or None,
        label=data.get("label", ""),
        created_by=auth.user.id,
    )
    if data.get("data"):
        cred.set_data(data["data"])
    db.session.add(cred)
    db.session.commit()
    return jsonify(cred.to_dict(masked=True)), 201


@api.route("/tenants/<tenant_id>/credentials/<cred_id>", methods=["PUT"])
@login_required
def update_credential(tenant_id, cred_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="editor")
    cred = Credential.query.filter_by(id=cred_id, tenant_id=tenant_id).first()
    if not cred:
        return jsonify({"error": "Not found"}), 404
    auth.assert_credential_owner_or_admin(cred)
    data = request.json or {}
    if "label" in data:
        cred.label = data["label"]
    if "integration" in data:
        cred.integration = data["integration"].strip() or None
    if "data" in data:
        cred.set_data(data["data"])
    cred.updated_at = datetime.utcnow()
    db.session.commit()
    return jsonify(cred.to_dict(masked=True))


@api.route("/tenants/<tenant_id>/credentials/<cred_id>", methods=["DELETE"])
@login_required
def delete_credential(tenant_id, cred_id):
    auth = Authorizer(current_user)
    auth.assert_tenant(tenant_id, role="editor")
    cred = Credential.query.filter_by(id=cred_id, tenant_id=tenant_id).first()
    if not cred:
        return jsonify({"error": "Not found"}), 404
    auth.assert_credential_owner_or_admin(cred)
    db.session.delete(cred)
    db.session.commit()
    return jsonify({"ok": True})


# ── Schedules ─────────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/flows/<flow_id>/schedule", methods=["GET"])
@login_required
def get_flow_schedule(tenant_id, flow_id):
    auth = Authorizer(current_user)
    auth.flow(flow_id, role="viewer", tenant_id=tenant_id)
    target_ids = {e.target_node_id for e in Edge.query.filter_by(flow_id=flow_id).all()}
    configs = {nc.node_id: json.loads(nc.config_json) for nc in NodeConfig.query.filter_by(flow_id=flow_id).all()}
    for node in [n for n in Node.query.filter_by(flow_id=flow_id).all() if n.node_id not in target_ids]:
        cfg = configs.get(node.node_id, {})
        cron = cfg.get("cron_schedule", "").strip()
        if cron and cfg.get("cron_enabled") in (True, "true", "True"):
            return jsonify({"active": True, "cron": cron, "node_id": node.node_id})
    return jsonify({"active": False})

# ── Data Tables ───────────────────────────────────────────────────────────────
@api.route("/tenants/<tenant_id>/tables", methods=["GET"])
@login_required
def list_tables(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")

    # Tables with data records
    data_tables = dict(
        db.session.query(DataRecord.table_name, db.func.count(DataRecord.id))
        .filter_by(tenant_id=tenant_id)
        .filter(~DataRecord.table_name.like("__schema__%"))
        .group_by(DataRecord.table_name)
        .all()
    )

    # Tables with only a schema (no data yet)
    schema_tables = [
        r.table_name.replace("__schema__", "")
        for r in DataRecord.query
        .filter_by(tenant_id=tenant_id)
        .filter(DataRecord.table_name.like("__schema__%"))
        .all()
    ]

    # Merge
    all_names = set(data_tables.keys()) | set(schema_tables)
    return jsonify([
        {"name": name, "count": data_tables.get(name, 0)}
        for name in sorted(all_names)
    ])


@api.route("/tenants/<tenant_id>/tables/<table_name>", methods=["GET"])
@login_required
def get_table(tenant_id, table_name):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    limit = request.args.get("limit", 100, type=int)
    offset = request.args.get("offset", 0, type=int)
    key = request.args.get("key")
    q = DataRecord.query.filter_by(tenant_id=tenant_id, table_name=table_name)
    if key:
        q = q.filter_by(record_key=key)
    total = q.count()
    records = q.order_by(DataRecord.id.desc()).offset(offset).limit(limit).all()
    return jsonify({"table": table_name, "total": total, "records": [r.to_dict() for r in records]})


@api.route("/tenants/<tenant_id>/tables/<table_name>", methods=["DELETE"])
@login_required
def delete_table(tenant_id, table_name):
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    DataRecord.query.filter_by(tenant_id=tenant_id, table_name=table_name).delete()
    db.session.commit()
    return jsonify({"ok": True, "deleted": table_name})


@api.route("/tenants/<tenant_id>/tables/<table_name>/records", methods=["GET"])
@login_required
def api_table_records(tenant_id, table_name):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")

    days = request.args.get("days", 7, type=int)
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 500, type=int)
    per_page = min(per_page, 1000)

    ALLOWED_DAYS = {7, 14, 30, 45}
    if days not in ALLOWED_DAYS:
        days = 7

    query = DataRecord.query.filter_by(tenant_id=tenant_id, table_name=table_name)

    if days:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = query.filter(DataRecord.created_at >= cutoff)

    paginated = query.order_by(DataRecord.id.desc()).paginate(page=page, per_page=per_page, error_out=False)

    result = []
    for r in paginated.items:
        try:
            d = json.loads(r.data) if isinstance(r.data, str) else r.data
        except Exception:
            d = {}
        result.append({"id": r.id, "key": r.record_key, "data": d})

    return jsonify({
        "records": result,
        "page": paginated.page,
        "per_page": paginated.per_page,
        "total": paginated.total,
        "pages": paginated.pages,
        "has_next": paginated.has_next,
    })


@api.route("/tenants/<tenant_id>/tables/<table_name>/records/<record_id>", methods=["DELETE"])
@login_required
def delete_record(tenant_id, table_name, record_id):
    auth = Authorizer(current_user)
    record = auth.record(record_id, role="editor", tenant_id=tenant_id)
    db.session.delete(record)
    db.session.commit()
    return jsonify({"ok": True})

@api.route("/tenants/<tenant_id>/tables/<table_name>/records", methods=["POST"])
@login_required
def create_record(tenant_id, table_name):
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    body = request.get_json(force=True)
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

@api.route("/tenants/<tenant_id>/tables/<table_name>/records/<record_id>", methods=["PUT"])
@login_required
def update_record(tenant_id, table_name, record_id):
    auth = Authorizer(current_user)
    record = auth.record(record_id, role="editor", tenant_id=tenant_id)
    data = request.json or {}
    record.data = json.dumps(data.get("data", {}), default=str)
    db.session.commit()
    return jsonify({"ok": True, "record": record.to_dict()})


@api.route("/tenants/<tenant_id>/tables/<table_name>/schema", methods=["GET"])
@login_required
def get_table_schema(tenant_id, table_name):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    record = DataRecord.query.filter_by(tenant_id=tenant_id, table_name=f"__schema__{table_name}").first()
    return jsonify({"table": table_name, "fields": json.loads(record.data) if record else []})


@api.route("/tenants/<tenant_id>/tables/<table_name>/schema", methods=["POST"])
@login_required
def create_table_schema(tenant_id, table_name):
    """Create a new table schema. Fails if table already exists."""
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    fields = (request.json or {}).get("fields", [])
    if not fields:
        return jsonify({"message": "At least one field is required"}), 400

    # Check for existing schema
    if DataRecord.query.filter_by(tenant_id=tenant_id, table_name=f"__schema__{table_name}").first():
        return jsonify({"message": f"Table '{table_name}' already exists"}), 409

    # Check for existing data records (table created by a flow without schema)
    if DataRecord.query.filter_by(tenant_id=tenant_id, table_name=table_name).first():
        return jsonify({"message": f"Table '{table_name}' already exists"}), 409

    record = DataRecord(
        tenant_id=tenant_id,
        table_name=f"__schema__{table_name}",
        record_key="schema",
        data=json.dumps(fields),
    )
    db.session.add(record)
    db.session.commit()
    return jsonify({"ok": True, "table": table_name, "fields": fields}), 201


@api.route("/tenants/<tenant_id>/tables/<table_name>/schema", methods=["PUT"])
@login_required
def set_table_schema(tenant_id, table_name):
    """Update an existing table schema."""
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    fields = (request.json or {}).get("fields", [])
    if not fields:
        return jsonify({"message": "At least one field is required"}), 400

    schema_record = DataRecord.query.filter_by(
        tenant_id=tenant_id, table_name=f"__schema__{table_name}"
    ).first()
    if not schema_record:
        return jsonify({"message": f"Table '{table_name}' not found"}), 404

    has_records = DataRecord.query.filter_by(
        tenant_id=tenant_id, table_name=table_name
    ).first() is not None

    if has_records:
        old_fields = json.loads(schema_record.data) if schema_record.data else []
        old_names = {f["name"] for f in old_fields}
        new_names = {f["name"] for f in fields}
        removed = old_names - new_names

        # Strip removed fields from all existing records
        if removed:
            records = DataRecord.query.filter_by(
                tenant_id=tenant_id, table_name=table_name
            ).all()
            for r in records:
                try:
                    data = json.loads(r.data) if isinstance(r.data, str) else r.data
                    if isinstance(data, dict):
                        for field_name in removed:
                            data.pop(field_name, None)
                        r.data = json.dumps(data)
                except Exception:
                    pass

    schema_record.data = json.dumps(fields)
    db.session.commit()
    return jsonify({"ok": True, "table": table_name, "fields": fields})

# ── Form Builder ──────────────────────────────────────────────────────────────
@api.route("/tenants/<tenant_id>/flows/<flow_id>/nodes/<node_id>/form", methods=["PUT"])
@login_required
def save_form(tenant_id, flow_id, node_id):
    auth = Authorizer(current_user)
    auth.flow(flow_id, role="editor", tenant_id=tenant_id)
    data = request.json or {}

    node = Node.query.filter_by(flow_id=flow_id, id=node_id).first()
    if not node:
        return jsonify({"error": "Node not found"}), 404

    nc = NodeConfig.query.filter_by(flow_id=flow_id, node_id=node.node_id).first()
    if not nc:
        nc = NodeConfig(flow_id=flow_id, node_id=node.node_id, config_json="{}")
        db.session.add(nc)

    nc.form_json = json.dumps({
        "form_title": data.get("title", ""),
        "form_description": data.get("description", ""),
        "form_elements": data.get("elements", []),
    })
    db.session.commit()
    return jsonify({"ok": True})


@api.route("/tenants/<tenant_id>/flows/<flow_id>/nodes/<node_id>/form", methods=["GET"])
@login_required
def get_form(tenant_id, flow_id, node_id):
    auth = Authorizer(current_user)
    auth.flow(flow_id, role="viewer", tenant_id=tenant_id)

    node = Node.query.filter_by(flow_id=flow_id, id=node_id).first()
    if not node:
        return jsonify({"title": "", "description": "", "elements": []})

    nc = NodeConfig.query.filter_by(flow_id=flow_id, node_id=node.node_id).first()
    form = json.loads(nc.form_json) if nc and nc.form_json else {}
    title = form.get("form_title", "")
    description = form.get("form_description", "")
    elements = form.get("form_elements", [])

    input_data = {}
    url_params = {k: v for k, v in request.args.items() if k not in ("session", "form_password")}

    if url_params and node.label and node.label.strip():
        synthetic = {"session_data": url_params, "form_data": {}}
        input_data[f"{node.node_id}_{node.label.strip()}"] = synthetic

    session_token = request.args.get("session")
    if session_token:
        fs = FormSession.query.filter_by(token=session_token, tenant_id=tenant_id).first()
        if fs and fs.status == "active":
            data_bus = fs.get_data_bus()
            for nid, output in data_bus.items():
                if isinstance(output, dict):
                    input_data.update(output)
                node_obj = Node.query.filter_by(flow_id=flow_id, node_id=nid).first()
                if node_obj and node_obj.label.strip():
                    input_data[f"{nid}_{node_obj.label.strip()}"] = output
            if "data" in data_bus:
                input_data["data"] = input_data["session"] = data_bus["data"]

    if input_data:
        title = _resolve(title, input_data)
        description = _resolve(description, input_data)
        for el in elements:
            for k in ("label", "content", "placeholder"):
                if el.get(k) and "{{" in el[k]:
                    try:
                        el[k] = _resolve(el[k], input_data)
                    except Exception:
                        pass

    return jsonify({"title": title, "description": description, "elements": elements})


# ── Public forms ──────────────────────────────────────────────────────────────
@api.route("/forms/<node_id>/submit", methods=["POST"])
@limiter.limit("5/second")
def submit_form(node_id):
    node = Node.query.filter_by(id=node_id).first()
    if not node:
        return jsonify({"error": "Form not found"}), 404
    _enforce_form_auth(node)
    flow = Flow.query.get(node.flow_id)
    if not flow.is_published:
        return jsonify({"error": "This form is not yet published"}), 403
    return _do_submit_form(flow.tenant_id, node.flow_id, node.node_id)

@api.route("/forms/<node_id>", methods=["GET"])
@limiter.limit("5/second")
def get_public_form(node_id):
    node = Node.query.filter_by(id=node_id).first()
    if not node:
        return jsonify({"error": "Form not found"}), 404
    _enforce_form_auth(node)
    nc = NodeConfig.query.filter_by(flow_id=node.flow_id, node_id=node.node_id).first()
    form = json.loads(nc.form_json) if nc and nc.form_json else {}

    title = form.get("form_title", "")
    description = form.get("form_description", "")
    elements = form.get("form_elements", [])

    input_data = {}
    url_params = {k: v for k, v in request.args.items() if k not in ("session", "form_password")}

    # Synthesize current node's output from URL params so templates like
    # {{1_Web Form.session_data.name}} resolve on the node's own load
    if url_params and node.label and node.label.strip():
        synthetic = {"session_data": url_params, "form_data": {}}
        input_data[f"{node.node_id}_{node.label.strip()}"] = synthetic

    session_token = request.args.get("session")
    if session_token:
        flow = Flow.query.get(node.flow_id)
        if flow:
            fs = FormSession.query.filter_by(token=session_token, tenant_id=flow.tenant_id).first()
            if fs and fs.status == "active":
                data_bus = fs.get_data_bus()
                for nid, output in data_bus.items():
                    if isinstance(output, dict):
                        input_data.update(output)
                    node_obj = Node.query.filter_by(flow_id=node.flow_id, node_id=nid).first()
                    if node_obj and node_obj.label.strip():
                        input_data[f"{nid}_{node_obj.label.strip()}"] = output

    if input_data:
        title = _resolve(title, input_data)
        description = _resolve(description, input_data)
        for el in elements:
            for k in ("label", "content", "placeholder"):
                if el.get(k) and "{{" in el[k]:
                    try:
                        el[k] = _resolve(el[k], input_data)
                    except Exception:
                        pass

    return jsonify({
        "title": title,
        "description": description,
        "elements": elements,
    })

@api.route("/form-jobs/<job_id>", methods=["GET"])
def get_form_job_status(job_id):
    """Public endpoint for polling form submission job status."""
    try:
        from rq.job import Job as RqJob
        job = RqJob.fetch(job_id, connection=current_app.redis_conn)
    except Exception:
        return jsonify({"status": "not_found"}), 404

    status = job.get_status()
    result = None
    error = None

    if status == "finished":
        result = job.result
    elif status == "failed":
        error = str(job.exc_info) if job.exc_info else "Submission failed"

    return jsonify({
        "job_id": job_id,
        "status": status,
        "result": result,
        "error": error,
    })

@api.route("/form-sessions/<token>", methods=["GET"])
def get_form_session(token):
    fs = FormSession.query.filter_by(token=token).first()
    if not fs or fs.status != "active":
        return jsonify({"error": "Session not found or expired"}), 404
    data_bus = fs.get_data_bus()
    prefill = {}
    for nid, output in data_bus.items():
        if isinstance(output, dict) and output.get("form_data"):
            prefill.update(output["form_data"])
    return jsonify({
        "token": fs.token,
        "flow_id": fs.flow_id,
        "current_node_id": fs.current_node_id,
        "step_index": fs.step_index,
        "total_steps": fs.total_steps,
        "prefill": prefill,
    })

# ── Webhook ───────────────────────────────────────────────────────────────────
@api.route("/webhook/<node_id>", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def handle_webhook(node_id):
    node = Node.query.filter_by(id=node_id, node_type="webhook").first()
    if not node:
        return jsonify({"error": "Webhook not found"}), 404

    nc = NodeConfig.query.filter_by(flow_id=node.flow_id, node_id=node.node_id).first()
    webhook_config = json.loads(nc.config_json) if nc and nc.config_json else {}

    allowed = webhook_config.get("method", "POST").upper()
    if allowed != "ANY" and request.method != allowed:
        return jsonify({"error": f"Method {request.method} not allowed"}), 405

    auth_header = webhook_config.get("authentication", "").strip()
    if auth_header:
        set_resolve_context(tenant_id=node.flow.tenant_id)
        try:
            auth_header = _resolve(auth_header, {})
        except Exception:
            return jsonify({"error": "Failed to resolve authentication config"}), 500
        provided = request.headers.get("Authorization", "").strip()
        if not provided or provided != auth_header:
            return jsonify({"error": "Unauthorized"}), 401

    try:
        body = request.get_json(force=True, silent=True) or {}
    except Exception:
        body = request.get_data(as_text=True)

    input_data = {"method": request.method, "path": node_id,
                  "headers": dict(request.headers), "query": dict(request.args), "body": body}

    job_id = queue_flow_run(
        flow_id=node.flow_id,
        input_data=input_data,
        start_node_id=node.node_id,
        webhook=True,
    )

    res_status = int(webhook_config.get("res_status", "200") or "200")
    res_body_str = webhook_config.get("res_body", '{"ok": true}').strip()
    try:
        res_body = json.loads(res_body_str)
    except Exception:
        res_body = {"message": res_body_str} if res_body_str else {"ok": True}
    return jsonify(res_body), res_status

# ── Tickets ───────────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/tickets", methods=["GET"])
@login_required
def api_list_tickets(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    status = request.args.get("status")
    priority = request.args.get("priority")
    assignee = request.args.get("assignee")
    limit = request.args.get("limit", 50, type=int)
    q = Ticket.query.filter_by(tenant_id=tenant_id).order_by(Ticket.created_at.desc())
    if status:
        q = q.filter_by(status=status)
    if priority:
        q = q.filter_by(priority=priority)
    if assignee:
        q = q.filter_by(assignee=assignee)
    return jsonify([t.to_dict() for t in q.limit(limit).all()])


@api.route("/tenants/<tenant_id>/tickets", methods=["POST"])
@login_required
def api_create_ticket(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    data = request.get_json() or {}
    ticket = Ticket(
        tenant_id=tenant_id, title=data.get("title", "Untitled"),
        status=data.get("status", "open"), priority=data.get("priority", "medium"),
        assignee=data.get("assignee"), tags=data.get("tags", []),
        content=data.get("content", {}), meta=data.get("meta", {}),
        flow_id=data.get("flow_id"), flow_run_id=data.get("flow_run_id"), node_id=data.get("node_id"),
    )
    db.session.add(ticket)
    db.session.commit()
    return jsonify(ticket.to_dict()), 201


@api.route("/tenants/<tenant_id>/tickets/<ticket_id>", methods=["GET"])
@login_required
def api_get_ticket(tenant_id, ticket_id):
    auth = Authorizer(current_user)
    return jsonify(auth.ticket(ticket_id, role="viewer", tenant_id=tenant_id).to_dict())


@api.route("/tenants/<tenant_id>/tickets/<ticket_id>", methods=["PUT"])
@login_required
def api_update_ticket(tenant_id, ticket_id):
    auth = Authorizer(current_user)
    ticket = auth.ticket(ticket_id, role="editor", tenant_id=tenant_id)
    data = request.get_json() or {}
    for field in ("title", "priority", "assignee", "tags", "content"):
        if field in data:
            setattr(ticket, field, data[field])
    if "status" in data:
        ticket.status = data["status"]
        ticket.closed_at = (db.func.now() if data["status"] == "closed" and not ticket.closed_at
                            else (None if data["status"] != "closed" else ticket.closed_at))
    if "meta" in data:
        existing = ticket.meta or {}
        existing.update(data["meta"])
        ticket.meta = existing
    db.session.commit()
    return jsonify(ticket.to_dict())


@api.route("/tenants/<tenant_id>/tickets/<ticket_id>", methods=["DELETE"])
@login_required
def api_delete_ticket(tenant_id, ticket_id):
    auth = Authorizer(current_user)
    db.session.delete(auth.ticket(ticket_id, role="editor", tenant_id=tenant_id))
    db.session.commit()
    return jsonify({"success": True})


@api.route("/tenants/<tenant_id>/tickets/<ticket_id>/comments", methods=["GET"])
@login_required
def api_list_comments(tenant_id, ticket_id):
    auth = Authorizer(current_user)
    ticket = auth.ticket(ticket_id, role="viewer", tenant_id=tenant_id)
    return jsonify([c.to_dict() for c in TicketComment.query.filter_by(ticket_id=ticket.id).order_by(TicketComment.created_at).all()])


@api.route("/tenants/<tenant_id>/tickets/<ticket_id>/comments", methods=["POST"])
@login_required
def api_create_comment(tenant_id, ticket_id):
    auth = Authorizer(current_user)
    ticket = auth.ticket(ticket_id, role="editor", tenant_id=tenant_id)
    data = request.get_json() or {}
    comment = TicketComment(ticket_id=ticket.id, author=data.get("author", "Anonymous"), body=data.get("body", ""))
    db.session.add(comment)
    db.session.commit()
    return jsonify(comment.to_dict()), 201


@api.route("/tenants/<tenant_id>/tickets/<ticket_id>/comments/<comment_id>", methods=["DELETE"])
@login_required
def api_delete_comment(tenant_id, ticket_id, comment_id):
    auth = Authorizer(current_user)
    ticket = auth.ticket(ticket_id, role="editor", tenant_id=tenant_id)
    comment = TicketComment.query.filter_by(id=comment_id, ticket_id=ticket.id).first()
    if not comment:
        return jsonify({"error": "Comment not found"}), 404
    db.session.delete(comment)
    db.session.commit()
    return jsonify({"success": True})


# ── Dashboards ────────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/dashboards", methods=["GET"])
@login_required
def api_list_dashboards(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    limit = request.args.get("limit", 50, type=int)
    q = Dashboard.query.filter_by(tenant_id=tenant_id).order_by(Dashboard.updated_at.desc())
    if limit:
        q = q.limit(limit)
    return jsonify([d.to_dict() for d in q.all()])

@api.route("/tenants/<tenant_id>/dashboards", methods=["POST"])
@login_required
def api_create_dashboard(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="editor")
    data = request.get_json() or {}
    dashboard = Dashboard(tenant_id=tenant_id, name=data.get("name", "Untitled Dashboard"),
                          widgets=data.get("widgets", []))
    db.session.add(dashboard)
    db.session.commit()
    return jsonify(dashboard.to_dict()), 201


@api.route("/tenants/<tenant_id>/dashboards/<dashboard_id>", methods=["GET"])
@login_required
def api_get_dashboard(tenant_id, dashboard_id):
    auth = Authorizer(current_user)
    return jsonify(auth.dashboard(dashboard_id, role="viewer", tenant_id=tenant_id).to_dict())


@api.route("/tenants/<tenant_id>/dashboards/<dashboard_id>", methods=["PUT"])
@login_required
def api_update_dashboard(tenant_id, dashboard_id):
    auth = Authorizer(current_user)
    dashboard = auth.dashboard(dashboard_id, role="editor", tenant_id=tenant_id)
    data = request.get_json() or {}
    if "name" in data:
        dashboard.name = data["name"]
    if "widgets" in data:
        dashboard.widgets = data["widgets"]
    db.session.commit()
    return jsonify(dashboard.to_dict())


@api.route("/tenants/<tenant_id>/dashboards/<dashboard_id>", methods=["DELETE"])
@login_required
def api_delete_dashboard(tenant_id, dashboard_id):
    auth = Authorizer(current_user)
    db.session.delete(auth.dashboard(dashboard_id, role="editor", tenant_id=tenant_id))
    db.session.commit()
    return jsonify({"success": True})


# ── Dev/test ──────────────────────────────────────────────────────────────────

@api.route("/tenants/<tenant_id>/test", methods=["GET"])
@login_required
def get_test(tenant_id):
    Authorizer(current_user).assert_tenant(tenant_id, role="viewer")
    statuses = ["open", "in_progress", "resolved", "closed"]
    priorities = ["low", "medium", "high", "critical"]
    categories = ["Bug", "Feature", "Support", "Documentation", "Infrastructure"]
    assignees = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"]
    regions = ["US-East", "US-West", "EU-West", "EU-Central", "APAC", "LATAM"]
    tags = ["frontend", "backend", "api", "database", "auth", "ui", "performance", "security"]
    now = datetime.utcnow()
    data = []
    for i in range(50):
        created = now - timedelta(days=random.randint(0, 90), hours=random.randint(0, 23))
        status = random.choice(statuses)
        data.append({
            "id": i + 1,
            "title": f"Ticket {i+1}: {random.choice(['Fix','Add','Update','Remove'])} "
                     f"{random.choice(['login flow','dashboard','API endpoint'])}",
            "status": status, "priority": random.choice(priorities),
            "category": random.choice(categories), "assignee": random.choice(assignees),
            "region": random.choice(regions), "tags": random.choice(tags),
            "hours_spent": round(random.uniform(0.5, 40), 1),
            "story_points": random.choice([1, 2, 3, 5, 8, 13]),
            "date": created.strftime("%Y-%m-%d"), "created_at": created.isoformat(),
            "resolved_at": (created + timedelta(days=random.randint(1, 14))).isoformat()
                           if status in ("resolved", "closed") else None,
        })
    return jsonify({"count": len(data), "data": data})