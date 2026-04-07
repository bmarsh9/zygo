import json
import re

from flowcore.operators import REGISTRY

_current_tenant_id = None
_cred_cache = {}


def set_resolve_context(tenant_id=None):
    global _current_tenant_id, _cred_cache
    _current_tenant_id = tenant_id
    _cred_cache = {}


def _get_path(path: str, data: dict):
    """Resolve a dot-notation path against a nested dict."""
    keys = path.strip().split(".")
    for i in range(len(keys), 0, -1):
        root = ".".join(keys[:i])
        if isinstance(data, dict) and root in data:
            val = data[root]
            for k in keys[i:]:
                if isinstance(val, dict) and k in val:
                    val = val[k]
                else:
                    return ""
            return val
    return ""


def _split_args(raw: str) -> list:
    if not raw.strip():
        return []
    args = []
    i, n = 0, len(raw)
    while i <= n:
        while i < n and raw[i] == ' ':
            i += 1
        if i >= n:
            break
        if raw[i] in ('"', "'"):
            q = raw[i]
            i += 1
            start = i
            while i < n and raw[i] != q:
                i += 1
            args.append(raw[start:i])
            i += 1
            while i < n and raw[i] == ' ':
                i += 1
            if i < n and raw[i] == ',':
                i += 1
        else:
            start = i
            while i < n and raw[i] != ',':
                i += 1
            args.append(raw[start:i].strip())
            i += 1
    return args


def _parse_expr(expr: str):
    m = re.match(r'^([A-Z_][A-Z0-9_]*)\((.*)\)$', expr.strip(), re.DOTALL)
    if m:
        op = m.group(1)
        if op in REGISTRY:
            return op, _split_args(m.group(2))
        raise ValueError(
            f"Unknown operator '{op}' in expression '{{{{{expr}}}}}'."
        )
    return None, [expr.strip()]


def _resolve_arg(arg: str, data: dict):
    resolved = _get_path(arg, data)
    if resolved != "" or arg in data:
        return resolved
    return arg


def _resolve_cred(cred_name: str, field_name: str) -> str:
    """Resolve a credential field via the internal API, with caching."""
    global _cred_cache
    if not _current_tenant_id:
        return ""
    if cred_name not in _cred_cache:
        try:
            from flowcore.nodes import _internal_get
            resp = _internal_get(f"/internal/api/credentials/{_current_tenant_id}/{cred_name}")
            _cred_cache[cred_name] = resp.get("fields", {})
        except Exception:
            _cred_cache[cred_name] = {}
    return str(_cred_cache[cred_name].get(field_name, ""))


def _resolve(value: str, data: dict) -> str:
    """Replace all {{expressions}} in value using data."""
    errors = []

    def _replace(m):
        expr = m.group(1).strip()

        # Credential resolution: {{cred.<name>.<field>}}
        if expr.startswith("cred."):
            parts = expr.split(".", 2)
            if len(parts) == 3:
                return _resolve_cred(parts[1], parts[2])
            return ""

        try:
            op, raw_args = _parse_expr(expr)
        except ValueError as e:
            errors.append(str(e))
            return ""

        if op:
            resolved_args = [_resolve_arg(a, data) for a in raw_args]
            try:
                result = REGISTRY[op](resolved_args)
                if isinstance(result, (dict, list)):
                    return json.dumps(result, default=str)
                return str(result) if not isinstance(result, str) else result
            except Exception:
                return str(resolved_args[0]) if resolved_args else ""

        result = _get_path(raw_args[0], data)
        if isinstance(result, (dict, list)):
            return json.dumps(result, default=str)
        return str(result) if not isinstance(result, str) else result

    # Dry run to collect errors
    re.sub(r"\{\{(.*?)\}\}", _replace, value)

    if errors:
        raise ValueError(errors[0])

    return re.sub(r"\{\{(.*?)\}\}", _replace, value)