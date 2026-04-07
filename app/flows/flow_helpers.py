import json
import traceback
import uuid
from datetime import datetime

from flask import abort, jsonify, request
from flask_login import current_user

from app import db
from app.models import TenantMember, Edge, Flow, FormSession, Node, NodeConfig, Run, RunLog
from flowcore.resolve import _resolve


def _get_form_config(flow_id, node_id):
    """Return parsed node config for a form node."""
    nc = NodeConfig.query.filter_by(flow_id=flow_id, node_id=node_id).first()
    return json.loads(nc.config_json) if nc else {}

def _enforce_form_auth(node):
    """Enforce access control for a form node."""
    flow = Flow.query.get(node.flow_id)
    if not flow:
        abort(404)
    tenant_id = flow.tenant_id
    cfg = _get_form_config(node.flow_id, node.node_id)
    access_control = cfg.get("access_control", "Public")
    form_password = cfg.get("form_password", "").strip()

    if access_control == "Tenant Users":
        if not current_user.is_authenticated or not current_user.is_active:
            abort(401, description="This form requires you to be logged in")
        if not current_user.super and not TenantMember.query.filter_by(user_id=current_user.id,
                                                                       tenant_id=tenant_id).first():
            abort(403, description="You do not have access to this form")

    if form_password:
        submitted = request.args.get("form_password", "").strip()
        if submitted != form_password:
            abort(401, description="__password_required__")


def load_flow_data(flow_id):
    flow = Flow.query.get(flow_id)
    if not flow:
        return None, None, None
    node_configs = {nc.node_id: json.loads(nc.config_json) for nc in NodeConfig.query.filter_by(flow_id=flow_id).all()}
    nodes = []
    for n in Node.query.filter_by(flow_id=flow_id).all():
        nd = n.to_dict()
        nd["config"] = node_configs.get(n.node_id, {})
        if nd.get("action_id"):
            nd["config"]["action_id"] = nd["action_id"]
        nodes.append(nd)
    return nodes, [e.to_dict() for e in Edge.query.filter_by(flow_id=flow_id).all()], flow.tenant_id


def _do_submit_form(tenant_id, flow_id, node_id):
    from app.flows.job_queue import queue_form_run

    flow = Flow.query.filter_by(id=flow_id, tenant_id=tenant_id).first()
    if not flow:
        abort(404)

    data = request.json or {}
    form_data = data.get("form_data", {})
    session_token = data.get("session_token")
    nodes_list, edges, tenant_id = load_flow_data(flow_id)
    node_configs = {n["node_id"]: n.get("config", {}) for n in nodes_list}

    session_data = data.get("session_data", {})
    if session_token:
        fs = FormSession.query.filter_by(token=session_token, tenant_id=tenant_id, flow_id=flow_id).first()
        if fs and fs.status == "active":
            stored = fs.get_data_bus().get("data", {})
            session_data = {**stored, **session_data}

    authed_user_info = None
    if current_user.is_authenticated and current_user.is_active:
        authed_user_info = {
            "id": current_user.id,
            "email": current_user.email,
            "display_name": current_user.display_name,
        }

    _SAFE_HEADERS = {"user-agent", "referer", "origin", "accept-language", "content-type"}
    form_output = {
        "__trigger__": "form", "__node_id__": node_id, "form_data": form_data,
        "session_data": session_data, "submitted_at": data.get("submitted_at"),
        "user": authed_user_info,
        "headers": {k: v for k, v in request.headers if k.lower() in _SAFE_HEADERS},
    }

    data_bus = {}
    if session_token:
        fs = FormSession.query.filter_by(token=session_token, tenant_id=tenant_id, flow_id=flow_id).first()
        if fs and fs.status == "active":
            data_bus = fs.get_data_bus()

    # Find existing run from session
    existing_run_id = None
    if session_token:
        fs = FormSession.query.filter_by(token=session_token, tenant_id=tenant_id, flow_id=flow_id).first()
        if fs and fs.run_id:
            existing_run_id = fs.run_id

    job_id = queue_form_run(
        tenant_id=tenant_id,
        flow_id=flow_id,
        node_id=node_id,
        form_output=form_output,
        data_bus=data_bus,
        session_token=session_token,
        existing_run_id=existing_run_id,
    )

    return jsonify({"ok": True, "job_id": job_id, "status": "processing"})