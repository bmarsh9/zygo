"""
nodes.py
========
All node implementations plus the dispatcher.

Node contract:
    Every node is a function:  run_<type>(flow_id, config, input_data, resolve_data) -> dict
    - config       : field values from the right-panel (what the user typed)
    - input_data   : merged output from all upstream nodes (clean, no ref keys)
    - resolve_data : full data including {nodeId}_{label} keys for template resolution
    - return       : a plain dict that becomes the input for downstream nodes

Dispatcher:
    dispatch(flow_id, node_type, config, input_data, ref_data) -> dict

No database access. Nodes that need persistent data (Table, Ticket,
Integration credentials) call back to the Flask app's internal API via HTTP.
The base URL is read from the APP_BASE_URL environment variable.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time as _time
from typing import Any
import httpx
from flowcore.resolve import _get_path, _resolve

_INTERNAL_SECRET = os.environ.get("INTERNAL_API_SECRET", "internal-secret-change-me")


# ── Internal API client ───────────────────────────────────────────────────────

def _app_url(path: str) -> str:
    base = os.environ.get("APP_BASE_URL", "http://localhost:9000").rstrip("/")
    return f"{base}{path}"


def _internal_get(path: str, **kwargs) -> dict:
    headers = kwargs.pop("headers", {})
    headers["X-Internal-Secret"] = _INTERNAL_SECRET
    resp = httpx.get(_app_url(path), timeout=15, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _internal_post(path: str, body: dict, **kwargs) -> dict:
    headers = kwargs.pop("headers", {})
    headers["X-Internal-Secret"] = _INTERNAL_SECRET
    resp = httpx.post(_app_url(path), json=body, timeout=15, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _internal_patch(path: str, body: dict, **kwargs) -> dict:
    headers = kwargs.pop("headers", {})
    headers["X-Internal-Secret"] = _INTERNAL_SECRET
    resp = httpx.patch(_app_url(path), json=body, timeout=15, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _internal_delete(path: str, **kwargs) -> dict:
    headers = kwargs.pop("headers", {})
    headers["X-Internal-Secret"] = _INTERNAL_SECRET
    resp = httpx.delete(_app_url(path), timeout=15, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json()

def _assert_safe_url(url: str) -> None:
    import ipaddress as _ipaddress
    import socket
    from urllib.parse import urlparse

    _BLOCKED_RANGES = [
        _ipaddress.ip_network("10.0.0.0/8"),
        _ipaddress.ip_network("172.16.0.0/12"),
        _ipaddress.ip_network("192.168.0.0/16"),
        _ipaddress.ip_network("127.0.0.0/8"),
        _ipaddress.ip_network("169.254.0.0/16"),
        _ipaddress.ip_network("::1/128"),
        _ipaddress.ip_network("fc00::/7"),
    ]
    _BLOCKED_HOSTS = {"localhost", "postgres", "redis", "app", "worker"}

    parsed = urlparse(url)
    host = parsed.hostname

    if not host:
        raise ValueError("HTTP Request: invalid URL.")

    if host.lower() in _BLOCKED_HOSTS:
        raise ValueError(f"HTTP Request: host '{host}' is not allowed.")

    # Resolve hostname to IP and check all returned addresses
    try:
        results = socket.getaddrinfo(host, None)
    except socket.gaierror:
        raise ValueError(f"HTTP Request: could not resolve host '{host}'.")

    for result in results:
        ip_str = result[4][0]
        try:
            ip = _ipaddress.ip_address(ip_str)
        except ValueError:
            continue
        for blocked in _BLOCKED_RANGES:
            if ip in blocked:
                raise ValueError(f"HTTP Request: host '{host}' resolves to a blocked IP address.")

# ── Dispatcher ────────────────────────────────────────────────────────────────

def dispatch(tenant_id: str, flow_id: int, node_type: str, config: dict, input_data: dict, ref_data: dict = None) -> Any:
    """
    Dispatch a node for execution.

    input_data: clean merged upstream data (no {id}_{label} keys)
    ref_data:   {id}_{label} -> output mapping for template resolution
    """
    # Build resolve_data: full data for template resolution
    resolve_data = {**input_data, **(ref_data or {})}

    handlers = {
        "webhook":     run_webhook,
        "webform":     run_webform,
        "httprequest": run_httprequest,
        "email":       run_email,
        "condition":   run_condition,
        "transform":   run_transform,
        "python":      run_python,
        "table":       run_table,
        "ticket":      run_ticket,
    }
    fn = handlers.get(node_type)
    if fn:
        return fn(tenant_id, flow_id, config, input_data, resolve_data)

    action_id = config.get("action_id", "")
    if action_id:
        return run_integration(tenant_id, flow_id, node_type, action_id, config, input_data, resolve_data)

    raise NotImplementedError(f"Unknown node type: '{node_type}'")


# ── Template resolution ───────────────────────────────────────────────────────

def _parse_args(raw_args: str, data: dict) -> list:
    args = []
    for part in re.split(r',(?=(?:[^"\']*["\'][^"\']*["\'])*[^"\']*$)', raw_args):
        part = part.strip()
        if not part:
            continue
        if (part.startswith('"') and part.endswith('"')) or \
           (part.startswith("'") and part.endswith("'")):
            args.append(part[1:-1])
        elif re.match(r'^-?\d+(\.\d+)?$', part):
            args.append(float(part) if '.' in part else int(part))
        else:
            args.append(_get_path(part, data))
    return args


def _resolve_config(config: dict, data: dict) -> dict:
    out = {}
    for k, v in config.items():
        if isinstance(v, str):
            out[k] = _resolve(v, data)
        elif isinstance(v, list):
            out[k] = [_resolve(i, data) if isinstance(i, str) else i for i in v]
        else:
            out[k] = v
    return out


# ── 1. Webhook ────────────────────────────────────────────────────────────────

def run_webhook(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    return dict(input_data)


# ── 2. Web Form ───────────────────────────────────────────────────────────────

def run_webform(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    return {
        "form_data": input_data.get("form_data", {}),
        "session_data": input_data.get("session_data", {}),
        "submitted_at": input_data.get("submitted_at"),
        "user": input_data.get("user"),
        "headers": input_data.get("headers", {}),
    }


# ── 3. HTTP Request ───────────────────────────────────────────────────────────

def run_httprequest(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    rd = resolve_data or input_data
    cfg = _resolve_config(config, rd)
    method = cfg.get("method", "GET").upper()
    url = cfg.get("url", "").strip()
    timeout = max(1.0, min(float(cfg.get("timeout") or 30), 30.0))

    if not url:
        raise ValueError("HTTP Request: 'url' is required.")

    _assert_safe_url(url)

    headers: dict[str, str] = {}
    for row in cfg.get("headers", []):
        if isinstance(row, dict) and row.get("k"):
            headers[row["k"]] = row.get("v", "")

    auth_type = cfg.get("auth_type", "None")
    auth_value = cfg.get("auth_value", "")
    if auth_type == "Bearer Token" and auth_value:
        headers["Authorization"] = f"Bearer {auth_value}"
    elif auth_type == "API Key" and auth_value:
        headers["X-API-Key"] = auth_value

    params: dict[str, str] = {}
    for row in cfg.get("params", []):
        if isinstance(row, dict) and row.get("k"):
            params[row["k"]] = row.get("v", "")

    body_type = cfg.get("body_type", "None")
    raw_body = cfg.get("body", "")
    json_body = None
    content = None
    if body_type == "JSON" and raw_body:
        try:
            json_body = json.loads(raw_body)
        except json.JSONDecodeError:
            json_body = raw_body
    elif body_type in ("Raw", "Form Data") and raw_body:
        content = raw_body.encode()

    basic_auth = None
    if auth_type == "Basic Auth" and ":" in auth_value:
        user, pwd = auth_value.split(":", 1)
        basic_auth = (user, pwd)

    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.request(
                method=method, url=url,
                headers=headers or None,
                params=params or None,
                json=json_body,
                content=content,
                auth=basic_auth,
            )
    except httpx.ConnectError:
        raise ConnectionError(f"Could not connect to {url}?")
    except httpx.TimeoutException:
        raise TimeoutError(f"Request to {url} timed out after {timeout}s")
    except httpx.RequestError as exc:
        raise ConnectionError(f"Request to {url} failed: {exc}")

    try:
        body = resp.json()
    except Exception:
        body = resp.text

    return {
        "status_code": resp.status_code,
        "ok": resp.is_success,
        "body": body,
        "headers": dict(resp.headers),
    }


# ── 4. Email ──────────────────────────────────────────────────────────────────

def run_email(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    raise ValueError("Not implemented")


# ── 5. Condition ──────────────────────────────────────────────────────────────

_OPS: dict[str, Any] = {
    "equals":       lambda a, b: str(a).lower() == str(b).lower(),
    "not equals":   lambda a, b: str(a).lower() != str(b).lower(),
    "contains":     lambda a, b: str(b).lower() in str(a).lower(),
    "starts with":  lambda a, b: str(a).lower().startswith(str(b).lower()),
    "ends with":    lambda a, b: str(a).lower().endswith(str(b).lower()),
    "greater than": lambda a, b: float(a) > float(b),
    "less than":    lambda a, b: float(a) < float(b),
    "is empty":     lambda a, b: not str(a).strip(),
    "is not empty": lambda a, b: bool(str(a).strip()),
}


class ConditionFalseError(Exception):
    """Raised when a condition evaluates to false — triggers the failure path."""
    def __init__(self, output: dict):
        self.output = output
        super().__init__("Condition evaluated to false")


def run_condition(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    rd = resolve_data or input_data
    match_mode = config.get("match_mode", "All rules")
    rules = config.get("rules", [])

    if not rules:
        rules = [{
            "field": config.get("field", ""),
            "operator": config.get("operator", "equals"),
            "value": config.get("value", ""),
        }]

    results = []
    for rule in rules:
        field = _resolve(rule.get("field", ""), rd)
        op_name = rule.get("operator", "equals")
        value = _resolve(rule.get("value", ""), rd)

        op_fn = _OPS.get(op_name)
        if op_fn is None:
            raise ValueError(f"Condition: unknown operator '{op_name}'")

        try:
            results.append(op_fn(field, value))
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Condition evaluation failed: {exc}") from exc

    passed_count = sum(results)
    total_count = len(results)

    if match_mode == "All rules":
        result = all(results)
    elif match_mode == "At least":
        min_count = int(config.get("match_count", 1) or 1)
        result = passed_count >= min_count
    elif match_mode == "None":
        result = not any(results)
    else:
        result = all(results)

    output = {
        "result": result,
        "branch": "true" if result else "false",
        "rules_passed": passed_count,
        "rules_total": total_count,
        "__branch__": "true" if result else "false",
        "__condition__": result,
    }

    if not result:
        raise ConditionFalseError(output)

    return output


# ── 6. Filter ─────────────────────────────────────────────────────────────────

def run_filter(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    rd = resolve_data or input_data
    field = _resolve(config.get("field", ""), rd)
    op_name = config.get("operator", "equals")
    value = _resolve(config.get("value", ""), rd)
    mode = config.get("mode", "Continue")

    op_fn = _OPS.get(op_name)
    if op_fn is None:
        raise ValueError(f"Filter: unknown operator '{op_name}'")

    try:
        passed = op_fn(field, value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Filter evaluation failed: {exc}") from exc

    if not passed:
        if mode == "Stop execution":
            raise StopIteration("Filter did not match — execution stopped.")
        return {**input_data, "__passed__": False}

    return {**input_data, "__passed__": True}


# ── 7. Transform ──────────────────────────────────────────────────────────────

def run_transform(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    mode = config.get("mode", "message")
    if mode == "message":
        return _transform_message(config, input_data, resolve_data)
    elif mode == "iterate":
        return _transform_iterate(config, input_data, resolve_data)
    elif mode == "gather":
        return _transform_gather(config, input_data, resolve_data)
    elif mode == "delay":
        seconds = int(config.get("delay_seconds", 5) or 5)
        seconds = max(1, min(seconds, 3600))
        _time.sleep(seconds)
        return {"delayed": seconds}
    else:
        raise ValueError(f"Transform: unknown mode '{mode}'")


def _transform_message(config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    rd = resolve_data or input_data
    output_json = config.get("output_json", "").strip()
    pairs = config.get("pairs", [])

    if not output_json and pairs:
        output = {}
        for pair in pairs:
            key = pair.get("key", "").strip()
            val = pair.get("value", "")
            if not key:
                continue
            resolved = _resolve(val, rd) if isinstance(val, str) else val
            try:
                output[key] = json.loads(resolved)
            except (json.JSONDecodeError, TypeError):
                output[key] = resolved
        return output

    if not output_json:
        return {}

    resolved = _resolve(output_json, rd)
    try:
        result = json.loads(resolved)
    except json.JSONDecodeError as e:
        raise ValueError(f"Transform Message: invalid JSON output — {e}")

    return result if isinstance(result, dict) else {"output": result}


def _transform_iterate(config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    rd = resolve_data or input_data
    array_path = config.get("array_path", "").strip()
    variable = config.get("variable", "item").strip() or "item"
    clean_path = array_path.replace("{{", "").replace("}}", "").strip()
    arr = _get_path(clean_path, rd)
    if not isinstance(arr, list):
        raise ValueError(
            f"Transform Iterate: '{array_path}' did not resolve to an array "
            f"(got {type(arr).__name__}: {str(arr)[:100]})"
        )
    return {
        **input_data,
        "__iterate__": True,
        "__array__": arr,
        "__variable__": variable,
        "__count__": len(arr),
        "__display__": {"mode": "iterate", "variable": variable, "count": len(arr)},
    }


def _transform_gather(config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    rd = resolve_data or input_data
    raw_items = input_data.get("__gather_items__", [])
    per_item_bus = input_data.get("__per_item_label_bus__", [])
    collect_from = config.get("collect_from", "").strip()
    group_by_keys = config.get("group_by_keys", False)
    include_fields_raw = config.get("include_fields", "")
    include_fields = [f.strip() for f in (
        include_fields_raw if isinstance(include_fields_raw, list)
        else include_fields_raw.split(",")
    ) if f.strip()]

    expr = collect_from.replace("{{", "").replace("}}", "").strip() if collect_from else ""

    items = []
    for i, raw in enumerate(raw_items):
        if expr and i < len(per_item_bus):
            value = _get_path(expr, per_item_bus[i])
            items.append(value if value != "" else raw)
        else:
            if isinstance(raw, dict):
                items.append({k: v for k, v in raw.items() if not k.startswith("__")})
            else:
                items.append(raw)

    if group_by_keys:
        grouped = {}
        for item in items:
            if isinstance(item, dict):
                for k, v in item.items():
                    if not include_fields or k in include_fields:
                        grouped.setdefault(k, []).append(v)
            else:
                grouped.setdefault("value", []).append(item)
        return {"items": grouped, "count": len(raw_items)}

    return {"items": items, "count": len(raw_items)}


# ── 8. Python ─────────────────────────────────────────────────────────────────
def run_python(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    from flowcore.python_runner import python_runner
    PYTHON_ENV = os.environ.get("PYTHON_ENV", "local").lower()
    if PYTHON_ENV == "aws":
        raise ValueError("AWS Lambda is not implemented")
    return python_runner(flow_id, config, input_data, resolve_data)


# ── 9. Table ──────────────────────────────────────────────────────────────────

def run_table(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    """
    Delegates all table operations to the Flask internal API.
    """
    rd = resolve_data or input_data
    action = config.get("action", "list").lower()
    table_name = _resolve(config.get("table_name", ""), rd).strip()

    if not table_name:
        raise ValueError("Table: 'table_name' is required.")

    if action == "insert":
        record_data = {}
        for row in config.get("record_data", []):
            if isinstance(row, dict) and row.get("k"):
                val = _resolve(row.get("v", ""), rd)
                try:
                    record_data[row["k"]] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    record_data[row["k"]] = val

        record_key = _resolve(config.get("record_key", ""), rd).strip() or None
        return _internal_post("/internal/api/tables/records", {
            "tenant_id": tenant_id,
            "table_name": table_name,
            "record_key": record_key,
            "data": record_data,
        })

    elif action == "get":
        record_key = _resolve(config.get("record_key", ""), rd).strip()
        filters = {
            row["k"].strip(): _resolve(row.get("v", ""), rd).strip()
            for row in config.get("filters", [])
            if isinstance(row, dict) and row.get("k")
        }
        return _internal_post("/internal/api/tables/records/get", {
            "table_name": table_name,
            "record_key": record_key or None,
            "filters": filters,
        })

    elif action == "list":
        limit = int(_resolve(config.get("limit", "100"), rd) or 100)
        filters = {
            row["k"].strip(): _resolve(row.get("v", ""), rd).strip()
            for row in config.get("filters", [])
            if isinstance(row, dict) and row.get("k")
        }
        return _internal_post("/internal/api/tables/records/list", {
            "table_name": table_name,
            "filters": filters,
            "limit": limit,
        })

    elif action == "update":
        record_key = _resolve(config.get("record_key", ""), rd).strip()
        if not record_key:
            raise ValueError("Table Update: 'record_key' is required.")
        record_data = {}
        for row in config.get("record_data", []):
            if isinstance(row, dict) and row.get("k"):
                val = _resolve(row.get("v", ""), rd)
                try:
                    record_data[row["k"]] = json.loads(val)
                except (json.JSONDecodeError, TypeError):
                    record_data[row["k"]] = val
        return _internal_post("/internal/api/tables/records/update", {
            "table_name": table_name,
            "record_key": record_key,
            "data": record_data,
        })

    elif action == "delete":
        record_key = _resolve(config.get("record_key", ""), rd).strip()
        return _internal_post("/internal/api/tables/records/delete", {
            "table_name": table_name,
            "record_key": record_key or None,
        })

    else:
        raise ValueError(f"Table: unknown action '{action}'. Use insert, get, list, update, or delete.")


# ── 10. Integration ───────────────────────────────────────────────────────────

def run_integration(tenant_id: str, flow_id: int, node_type: str, action_id: str, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    """
    Fetches the action definition and credential from the Flask internal API,
    then executes the HTTP request.
    """
    rd = resolve_data or input_data

    # Fetch action definition from Flask app
    action_def_resp = _internal_get(f"/internal/api/integrations/{action_id}")
    action_def = action_def_resp.get("action")
    if not action_def:
        raise NotImplementedError(f"Unknown action: '{action_id}'")

    # Built-in handler actions are handled server-side
    if action_def.get("has_handler"):
        return _internal_post(f"/internal/api/integrations/{action_id}/run", {
            "config": config,
            "input_data": input_data,
        })

    cfg = _resolve_config(config, rd)
    method = action_def.get("method", "GET")
    url = _resolve(action_def.get("url", ""), {**rd, **cfg})
    timeout = float(cfg.get("timeout", 30))

    headers = {}
    for k, v in action_def.get("default_headers", {}).items():
        headers[k] = _resolve(v, {**rd, **cfg})

    auth_type = action_def.get("auth", "")
    if auth_type:
        cred_resp = _internal_get(f"/internal/api/credentials/{node_type}")
        tokens = cred_resp.get("tokens", {})
        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {tokens.get('token', '')}"
        elif auth_type == "basic":
            pair = f"{tokens.get('username', '')}:{tokens.get('password', '')}"
            headers["Authorization"] = f"Basic {base64.b64encode(pair.encode()).decode()}"
        elif auth_type == "header":
            headers[action_def.get("auth_header", "X-API-Key")] = tokens.get("token", "")

    json_body = None
    body_template = action_def.get("body")
    if body_template:
        resolved_body = _resolve(json.dumps(body_template), {**rd, **cfg})
        try:
            json_body = json.loads(resolved_body)
        except json.JSONDecodeError:
            json_body = resolved_body

    params = {k: _resolve(v, {**rd, **cfg}) for k, v in action_def.get("params", {}).items()}

    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.request(
                method=method, url=url,
                headers=headers or None,
                params=params or None,
                json=json_body,
            )
    except httpx.ConnectError:
        raise ConnectionError(f"Could not connect to {url}")
    except httpx.TimeoutException:
        raise TimeoutError(f"Request to {url} timed out")
    except httpx.RequestError as exc:
        raise ConnectionError(f"Request failed: {exc}")

    try:
        body = resp.json()
    except Exception:
        body = resp.text

    return {"status_code": resp.status_code, "ok": resp.is_success,
            "body": body, "headers": dict(resp.headers)}


# ── 11. Ticket ────────────────────────────────────────────────────────────────

def run_ticket(tenant_id: str, flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    """
    Delegates all ticket operations to the Flask internal API.
    """
    rd = resolve_data or input_data
    action = _resolve(config.get("action", "list"), rd)
    ticket_id = _resolve(config.get("ticket_id", ""), rd)

    if action == "create":
        title = _resolve(config.get("title", "Untitled"), rd)
        status = _resolve(config.get("status", "open"), rd)
        priority = _resolve(config.get("priority", "medium"), rd)
        assignee = _resolve(config.get("assignee", ""), rd) or None

        tags_raw = _resolve(config.get("tags", ""), rd)
        tags = ([t.strip() for t in tags_raw.split(",")]
                if isinstance(tags_raw, str) and tags_raw
                else (tags_raw if isinstance(tags_raw, list) else []))

        blocks = []
        for pair in config.get("content_blocks", []):
            block_type = _resolve(pair.get("k", "paragraph"), rd)
            block_text = _resolve(pair.get("v", ""), rd)
            if block_type == "header":
                blocks.append({"type": "header", "data": {"text": block_text, "level": 2}})
            elif block_type == "code":
                blocks.append({"type": "code", "data": {"code": block_text}})
            elif block_type == "checklist":
                items = [{"text": i.strip(), "checked": False} for i in block_text.split(",") if i.strip()]
                blocks.append({"type": "checklist", "data": {"items": items}})
            else:
                blocks.append({"type": "paragraph", "data": {"text": block_text}})

        content = {"time": int(_time.time() * 1000), "blocks": blocks, "version": "2.30.8"}
        meta = {}
        for pair in config.get("meta", []):
            k = _resolve(pair.get("k", ""), rd)
            v = _resolve(pair.get("v", ""), rd)
            if k:
                meta[k] = v

        return _internal_post("/internal/api/tickets", {
            "title": title, "status": status, "priority": priority,
            "assignee": assignee, "tags": tags,
            "content": content, "meta": meta, "flow_id": flow_id,
            "tenant_id": tenant_id
        })

    elif action == "update":
        if not ticket_id:
            return {"success": False, "error": "ticket_id is required for update"}

        update_data: dict = {}
        for field in ("title", "status", "priority", "assignee"):
            val = config.get(field, "")
            if val:
                update_data[field] = _resolve(val, rd)

        tags_raw = config.get("tags", "")
        if tags_raw:
            tags_val = _resolve(tags_raw, rd)
            update_data["tags"] = [t.strip() for t in tags_val.split(",")] if isinstance(tags_val, str) else tags_val

        if config.get("meta"):
            meta = {}
            for pair in config["meta"]:
                k = _resolve(pair.get("k", ""), rd)
                v = _resolve(pair.get("v", ""), rd)
                if k:
                    meta[k] = v
            update_data["meta"] = meta

        if config.get("content_blocks"):
            blocks = []
            for pair in config["content_blocks"]:
                block_type = _resolve(pair.get("k", "paragraph"), rd)
                block_text = _resolve(pair.get("v", ""), rd)
                if block_type == "header":
                    blocks.append({"type": "header", "data": {"text": block_text, "level": 2}})
                elif block_type == "code":
                    blocks.append({"type": "code", "data": {"code": block_text}})
                else:
                    blocks.append({"type": "paragraph", "data": {"text": block_text}})
            update_data["content_blocks"] = blocks

        return _internal_patch(f"/internal/api/tickets/{ticket_id}", update_data)

    elif action == "get":
        if not ticket_id:
            return {"success": False, "error": "ticket_id is required for get"}
        return _internal_get(f"/internal/api/tickets/{ticket_id}")

    elif action == "list":
        params = {}
        filter_status = _resolve(config.get("filter_status", ""), rd)
        filter_priority = _resolve(config.get("filter_priority", ""), rd)
        filter_assignee = _resolve(config.get("filter_assignee", ""), rd)
        limit = int(_resolve(config.get("limit", "100"), rd) or 100)
        if filter_status:
            params["status"] = filter_status
        if filter_priority:
            params["priority"] = filter_priority
        if filter_assignee:
            params["assignee"] = filter_assignee
        params["limit"] = limit
        return _internal_get("/internal/api/tickets", params=params)

    elif action == "delete":
        if not ticket_id:
            return {"success": False, "error": "ticket_id is required for delete"}
        return _internal_delete(f"/internal/api/tickets/{ticket_id}")

    else:
        return {"success": False, "error": f"Unknown ticket action: {action}"}