import json
import os
import sys
import subprocess
import tempfile
import ast
from RestrictedPython import compile_restricted, safe_globals, safe_builtins
from RestrictedPython.Guards import guarded_iter_unpack_sequence

_PYTHON_TIMEOUT = 60

# ── Injected into the subprocess wrapper as a literal string ──
# Every line must have ZERO leading indentation. If any line here has leading
# spaces and the wrapper f-string interpolates it, the resulting script will
# have mixed indentation causing an IndentationError on execution.
_REQUESTS_GUARD = """\
import requests as _real_requests
import ipaddress as _ipaddress
import socket as _socket
from urllib.parse import urlparse as _urlparse

class _SafeSession(_real_requests.Session):
    _BLOCKED_RANGES = [
        _ipaddress.ip_network("10.0.0.0/8"),
        _ipaddress.ip_network("172.16.0.0/12"),
        _ipaddress.ip_network("192.168.0.0/16"),
        _ipaddress.ip_network("127.0.0.0/8"),
        _ipaddress.ip_network("169.254.0.0/16"),
        _ipaddress.ip_network("::1/128"),
        _ipaddress.ip_network("fc00::/7"),
    ]
    _BLOCKED_HOSTS = {"localhost", "postgres", "redis", "app"}

    def request(self, method, url, **kwargs):
        parsed = _urlparse(url)
        host = (parsed.hostname or "").lower()
        if host in self._BLOCKED_HOSTS:
            raise ConnectionError(f"Requests to internal host '{host}' are not allowed.")
        try:
            ip = _ipaddress.ip_address(_socket.gethostbyname(host))
            for blocked in self._BLOCKED_RANGES:
                if ip in blocked:
                    raise ConnectionError(
                        f"Requests to internal IP range '{blocked}' are not allowed."
                    )
        except (_socket.gaierror, ValueError):
            pass
        if "timeout" not in kwargs:
            kwargs["timeout"] = 10
        return super().request(method, url, **kwargs)

class _SafeRequests:
    _session = _SafeSession()
    def get(self, *a, **kw):      return self._session.get(*a, **kw)
    def post(self, *a, **kw):     return self._session.post(*a, **kw)
    def put(self, *a, **kw):      return self._session.put(*a, **kw)
    def patch(self, *a, **kw):    return self._session.patch(*a, **kw)
    def delete(self, *a, **kw):   return self._session.delete(*a, **kw)
    def head(self, *a, **kw):     return self._session.head(*a, **kw)
    def request(self, *a, **kw):  return self._session.request(*a, **kw)

_safe_requests = _SafeRequests()
"""


def _check_syntax(code: str) -> tuple[bool, str | None, int | None]:
    """Check syntax and policy before execution."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return False, str(e.msg), e.lineno

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return False, "Import statements are not allowed — libraries are pre-loaded automatically", node.lineno

    try:
        result = compile_restricted(code, filename="<user>", mode="exec")
        if result is None:
            return False, "Code failed policy check", None
        return True, None, None
    except SyntaxError as e:
        return False, str(e.msg), e.lineno
    except Exception as e:
        return False, str(e), None


def _clean_restricted_error(err: str) -> str:
    """Strip RestrictedPython internal frame noise from tracebacks."""
    lines = err.strip().splitlines()
    cleaned = [
        l for l in lines
        if "RestrictedPython" not in l
        and "compile_restricted" not in l
        and "_rglobs" not in l
        and "exec(_compiled" not in l
    ]
    return "\n".join(cleaned) if cleaned else err


def _resolve(val, input_data):
    """Resolve {{variable}} references from input_data."""
    if not isinstance(val, str):
        return val
    import re
    def replacer(m):
        key = m.group(1).strip()
        parts = key.split(".")
        obj = input_data
        for part in parts:
            if isinstance(obj, dict):
                obj = obj.get(part, m.group(0))
            else:
                return m.group(0)
        return str(obj) if not isinstance(obj, str) else obj
    return re.sub(r"\{\{(.+?)\}\}", replacer, val)


def python_runner(flow_id: int, config: dict, input_data: dict, resolve_data: dict = None) -> dict:
    code = config.get("code", "").strip()
    timeout = min(float(config.get("timeout") or 30), _PYTHON_TIMEOUT)
    packages = config.get("packages", "").strip()

    # Use resolve_data for template resolution in KV config,
    # but pass clean input_data to the subprocess
    rd = resolve_data or input_data

    # ── Resolve explicit input_data overrides ──
    explicit_input = {}
    for row in config.get("input_data", []):
        if isinstance(row, dict) and row.get("k"):
            val = _resolve(row.get("v", ""), rd)
            try:
                explicit_input[row["k"]] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                explicit_input[row["k"]] = val
    if explicit_input:
        input_data = explicit_input

    if not code:
        raise ValueError("Python: 'code' is empty.")

    # ── Step 1: Syntax + policy check ──
    ok, err_msg, err_line = _check_syntax(code)
    if not ok:
        return {"success": False, "error": err_msg, "line": err_line}

    # ── Step 2: Install packages ──
    if packages:
        _blocked_pkgs = {
            "os", "sys", "subprocess", "ctypes", "cffi",
            "socket", "shutil", "pathlib", "importlib",
        }
        pkgs = [
            p.strip() for p in packages.split(",")
            if p.strip() and p.strip().lower() not in _blocked_pkgs
        ]
        if pkgs:
            try:
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "--quiet", *pkgs],
                    check=True, capture_output=True,
                )
            except subprocess.CalledProcessError as e:
                return {
                    "success": False,
                    "error": f"Failed to install packages: {e.stderr.decode().strip()}",
                }

    # ── Step 3: Build the restricted execution wrapper ──
    wrapper = (
        "import json, sys, io\n"
        "import csv, datetime, statistics, itertools\n"
        "import math, re, collections, functools, string, decimal\n"
        "import uuid, base64, hashlib, time\n"
        "from RestrictedPython import compile_restricted, safe_globals, safe_builtins\n"
        "from RestrictedPython.Guards import guarded_iter_unpack_sequence\n"
        "\n"
        + _REQUESTS_GUARD +
        "\n"
        "_code_src = " + repr(code) + "\n"
        "_compiled = compile_restricted(_code_src, filename='<user>', mode='exec')\n"
        "\n"
        "if _compiled is None:\n"
        "    sys.stdout.write(json.dumps({'__error__': 'Code failed RestrictedPython policy check'}) + chr(10))\n"
        "    sys.exit(1)\n"
        "\n"
        "_rglobs = dict(safe_globals)\n"
        "_rglobs['__builtins__'] = dict(safe_builtins)\n"
        "_rglobs['__builtins__']['__import__'] = lambda *a, **kw: (_ for _ in ()).throw(\n"
        "    ImportError('Imports are not allowed — use the pre-loaded libraries.')\n"
        ")\n"
        "_rglobs['_getiter_']              = iter\n"
        "_rglobs['_getattr_']              = getattr\n"
        "_rglobs['_getitem_']              = lambda obj, key: obj[key]\n"
        "_rglobs['_write_']                = lambda x: x\n"
        "_rglobs['_iter_unpack_sequence_'] = guarded_iter_unpack_sequence\n"
        "\n"
        "class _PrintCollector:\n"
        "    def __init__(self, _getiter_=None):\n"
        "        self._lines = []\n"
        "    def __call__(self, *args, **kwargs):\n"
        "        return self\n"
        "    def _call_print(self, *args, **kwargs):\n"
        "        sep = kwargs.get('sep', ' ')\n"
        "        end = kwargs.get('end', '')\n"
        "        self._lines.append(sep.join(str(a) for a in args) + end)\n"
        "    def __enter__(self):\n"
        "        return self\n"
        "    def __exit__(self, *a):\n"
        "        pass\n"
        "    @property\n"
        "    def printed(self):\n"
        "        return ''.join(self._lines)\n"
        "\n"
        "_print_collector = _PrintCollector()\n"
        "_rglobs['_print_'] = _print_collector\n"
        "_rglobs['__builtins__']['print'] = _print_collector\n"
        "\n"
        "def _inplacevar(op, x, y):\n"
        "    ops = {\n"
        "        '+=':  lambda a, b: a + b,\n"
        "        '-=':  lambda a, b: a - b,\n"
        "        '*=':  lambda a, b: a * b,\n"
        "        '/=':  lambda a, b: a / b,\n"
        "        '//=': lambda a, b: a // b,\n"
        "        '**=': lambda a, b: a ** b,\n"
        "        '%=':  lambda a, b: a % b,\n"
        "        '|=':  lambda a, b: a | b,\n"
        "        '&=':  lambda a, b: a & b,\n"
        "        '^=':  lambda a, b: a ^ b,\n"
        "        '>>=': lambda a, b: a >> b,\n"
        "        '<<=': lambda a, b: a << b,\n"
        "    }\n"
        "    if op not in ops:\n"
        "        raise NotImplementedError(f'Operator {op!r} is not supported.')\n"
        "    return ops[op](x, y)\n"
        "_rglobs['_inplacevar_'] = _inplacevar\n"
        "\n"
        "_rglobs['requests']    = _safe_requests\n"
        "_rglobs['csv']         = csv\n"
        "_rglobs['json']        = json\n"
        "_rglobs['datetime']    = datetime\n"
        "_rglobs['statistics']  = statistics\n"
        "_rglobs['itertools']   = itertools\n"
        "_rglobs['math']        = math\n"
        "_rglobs['re']          = re\n"
        "_rglobs['collections'] = collections\n"
        "_rglobs['functools']   = functools\n"
        "_rglobs['string']      = string\n"
        "_rglobs['decimal']     = decimal\n"
        "_rglobs['uuid']        = uuid\n"
        "_rglobs['base64']      = base64\n"
        "_rglobs['hashlib']     = hashlib\n"
        "_rglobs['io']          = io\n"
        "_rglobs['time']        = time\n"
        "\n"
        "try:\n"
        "    exec(_compiled, _rglobs)\n"
        "except Exception as e:\n"
        "    sys.stdout.write(json.dumps({'__error__': str(e)}) + chr(10))\n"
        "    sys.exit(1)\n"
        "\n"
        "if 'run' not in _rglobs or not callable(_rglobs['run']):\n"
        "    sys.stdout.write(json.dumps({'__error__': \"No callable 'run(input_data)' function found. Define: def run(input_data):\"}) + chr(10))\n"
        "    sys.exit(1)\n"
        "\n"
        "_input = json.loads(sys.stdin.read())\n"
        "try:\n"
        "    _result = _rglobs['run'](_input)\n"
        "except Exception as e:\n"
        "    sys.stdout.write(json.dumps({'__error__': str(e)}) + chr(10))\n"
        "    sys.exit(1)\n"
        "\n"
        "sys.stdout.write(json.dumps({\n"
        "    '__result__': _result,\n"
        "    '__logs__': _print_collector._lines,\n"
        "}, default=str) + chr(10))\n"
    )

    # ── Step 4: Write wrapper to temp file and execute ──
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(wrapper)
        tmp = f.name

    try:
        proc = subprocess.run(
            [sys.executable, tmp],
            input=json.dumps(input_data),
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": f"Script timed out after {timeout}s."}
    finally:
        os.unlink(tmp)

    # ── Step 5: Parse output ──
    if proc.returncode != 0:
        err = _clean_restricted_error(proc.stderr.strip())
        try:
            inner = json.loads(proc.stdout)
            if isinstance(inner, dict) and "__error__" in inner:
                err = inner["__error__"]
        except (json.JSONDecodeError, ValueError):
            pass
        return {
            "success": False,
            "error": err or "Script exited with a non-zero status.",
            "__stdout__": proc.stdout.strip(),
        }

    try:
        raw = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return {
            "success": False,
            "error": "Script produced invalid JSON output.",
            "output": proc.stdout.strip(),
        }

    if isinstance(raw, dict) and "__error__" in raw:
        return {"success": False, "error": raw["__error__"]}

    if isinstance(raw, dict) and "__result__" in raw:
        result = raw["__result__"]
        logs = raw.get("__logs__", [])
        out = {"success": True, "output": result}
        if logs:
            out["logs"] = logs
        return out

    return {"success": True, "output": raw}