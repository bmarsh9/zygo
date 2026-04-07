"""
integrations.py
===============
All integration definitions and their action configs.
Returned by GET /api/integrations

Adding a new integration:
  1. Define actions using _section() and _field() helpers
  2. Append to INTEGRATIONS list
"""

from __future__ import annotations

import json
import uuid as _uuid

from flask import current_app

from app import db
from app.models import FormSession
from flowcore.resolve import _resolve


# ── Action registry ───────────────────────────────────────────────────────────

_ACTION_REGISTRY = {}


def _register_actions(integrations_list):
    for intg in integrations_list:
        intg_meta = {k: v for k, v in intg.items() if k != "actions"}
        for group in intg.get("actions", []):
            for item in group.get("items", []):
                _ACTION_REGISTRY[item["id"]] = {**item, "_integration": intg_meta}


def get_action_def(action_id):
    return _ACTION_REGISTRY.get(action_id)


def _section(fields: list) -> dict:
    return {"sections": [{"title": "Options", "fields": fields}]}


def _field(id: str, label: str, type: str, **kwargs) -> dict:
    return {"id": id, "label": label, "type": type, **kwargs}


# ── Built-in handlers ─────────────────────────────────────────────────────────
def _fc_create_form_session(config, input_data):
    flow_id = _resolve(config.get("flow_id", ""), input_data).strip()
    node_id = _resolve(config.get("node_id", ""), input_data).strip()

    if not flow_id or not node_id:
        raise ValueError("Create Form Session: flow_id and node_id are required")

    session_data = {}
    for row in config.get("session_data", []):
        if isinstance(row, dict) and row.get("k"):
            val = _resolve(row.get("v", ""), input_data)
            try:
                session_data[row["k"]] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                session_data[row["k"]] = val

    token = _uuid.uuid4().hex[:16]
    session = FormSession(
        token=token, flow_id=flow_id,
        current_node_id=node_id, step_index=0,
        total_steps=1, status="active",
    )
    session.set_data_bus({"data": session_data})
    db.session.add(session)
    db.session.commit()

    base_url = current_app.config.get("HOST_NAME", "")
    form_url = f"{base_url}/form/{flow_id}/{node_id}?session={token}"

    return {"token": token, "form_url": form_url, "flow_id": flow_id, "node_id": node_id}


def _fc_generate_uuid(config, input_data):
    return {"uuid": _uuid.uuid4().hex}


# ── Zygo built-in ────────────────────────────────────────────────────────

_ZYGO = {
    "id": "zygo",
    "name": "Zygo",
    "desc": "Built-in Zygo actions",
    "actions": [
        {
            "group": "Forms",
            "items": [
                {
                    "id": "fc_create_form_session",
                    "name": "Create Form Session",
                    "desc": "Pre-fill a form with data and get a shareable URL",
                    "handler": _fc_create_form_session,
                    "config": {
                        "sections": [
                            {
                                "title": "Target Form",
                                "fields": [
                                    {"id": "form_id", "label": "Form ID", "type": "input",
                                     "placeholder": "contact-form"},
                                ],
                            },
                            {
                                "title": "Data",
                                "fields": [
                                    {"id": "session_data", "label": "Preload data",
                                     "type": "kv", "keyPlaceholder": "field name",
                                     "valPlaceholder": "{{variable}} or value"},
                                ],
                            },
                        ]
                    },
                }
            ],
        },
        {
            "group": "Utilities",
            "items": [
                {
                    "id": "fc_generate_uuid",
                    "name": "Generate UUID",
                    "desc": "Generate a unique identifier",
                    "handler": _fc_generate_uuid,
                    "config": None,
                },
            ],
        },
    ],
}


# ── GitHub ────────────────────────────────────────────────────────────────────

_GITHUB = {
    "id": "github",
    "name": "GitHub",
    "desc": "Repos, PRs, Issues",
    "auth": "bearer",
    "base_url": "https://api.github.com",
    "default_headers": {"Accept": "application/vnd.github.v3+json"},
    "actions": [
        {
            "group": "Repositories",
            "items": [
                {
                    "id": "gh_list_repos",
                    "name": "List Repos",
                    "desc": "List user / org repos",
                    "method": "GET",
                    "url": "/users/{{owner}}/repos",
                    "params": {"type": "{{type}}", "sort": "{{sort}}", "per_page": "{{per_page}}"},
                    "config": _section([
                        _field("owner",    "Owner / Org", "input",  placeholder="octocat"),
                        _field("type",     "Repo type",   "select", options=["all", "owner", "public", "private", "forks", "sources", "member"]),
                        _field("sort",     "Sort by",     "select", options=["created", "updated", "pushed", "full_name"]),
                        _field("per_page", "Per page",    "input",  placeholder="30"),
                    ]),
                }
            ],
        }
    ],
}


# ── GitLab ────────────────────────────────────────────────────────────────────

_GITLAB = {
    "id": "gitlab",
    "name": "GitLab",
    "desc": "CI/CD, MRs, Issues",
    "auth": "header",
    "auth_header": "PRIVATE-TOKEN",
    "base_url": "https://gitlab.com/api/v4",
    "default_headers": {"Content-Type": "application/json"},
    "actions": [
        {
            "group": "Projects",
            "items": [
                {
                    "id": "gl_list_projects",
                    "name": "List Projects",
                    "desc": "List accessible projects",
                    "method": "GET",
                    "url": "/projects",
                    "params": {"search": "{{search}}", "per_page": "{{per_page}}",
                               "owned": "{{owned}}", "visibility": "{{visibility}}"},
                    "config": _section([
                        _field("search",     "Search",     "input",  placeholder="my-project", hint="optional"),
                        _field("per_page",   "Per page",   "input",  placeholder="20"),
                        _field("owned",      "Owned only", "select", options=["Yes", "No"]),
                        _field("visibility", "Visibility", "select", options=["any", "public", "internal", "private"]),
                    ]),
                }
            ],
        }
    ],
}


# ── Registry ──────────────────────────────────────────────────────────────────

INTEGRATIONS = [_ZYGO, _GITHUB, _GITLAB]
_register_actions(INTEGRATIONS)
