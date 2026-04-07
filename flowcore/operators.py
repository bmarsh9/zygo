"""
operators.py
============
Registry of all template operators available in flow config expressions.

Adding a new operator:
  1. Write a function:  def _op_myop(args: list) -> Any
  2. Add to REGISTRY:   "MYOP": _op_myop
  Nothing else needs to change.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


# ── Operator functions ────────────────────────────────────────────────────────

def _op_string(args: list) -> Any:
    return str(args[0])

def _op_int(args: list) -> Any:
    try:    return int(float(str(args[0])))
    except: return args[0]

def _op_float(args: list) -> Any:
    try:    return float(str(args[0]))
    except: return args[0]

def _op_bool(args: list) -> Any:
    v = str(args[0]).lower()
    return v not in ("false", "0", "", "none", "null")

def _op_upper(args: list) -> Any:
    return str(args[0]).upper()

def _op_lower(args: list) -> Any:
    return str(args[0]).lower()

def _op_trim(args: list) -> Any:
    return str(args[0]).strip()

def _op_replace(args: list) -> Any:
    return str(args[0]).replace(str(args[1]), str(args[2]))

def _op_slice(args: list) -> Any:
    try:    return str(args[0])[int(args[1]):int(args[2])]
    except: return args[0]

def _op_concat(args: list) -> Any:
    return "".join(str(a) for a in args)

def _op_join(args: list) -> Any:
    arr, sep = args[0], str(args[1]) if len(args) > 1 else ""
    if isinstance(arr, list):
        return sep.join(str(i) for i in arr)
    return str(arr)

def _op_if(args: list) -> Any:
    cond      = args[0]
    true_val  = args[1] if len(args) > 1 else ""
    false_val = args[2] if len(args) > 2 else ""
    if isinstance(cond, str):
        cond = cond.lower() not in ("false", "0", "", "none", "null")
    return true_val if cond else false_val

def _op_now(args: list) -> Any:
    return datetime.now(timezone.utc).isoformat()

def _op_date(args: list) -> Any:
    try:
        fmt = str(args[1]) if len(args) > 1 else "%Y-%m-%d"
        return datetime.fromisoformat(str(args[0]).replace("Z", "+00:00")).strftime(fmt)
    except:
        return str(args[0])


# ── Registry ──────────────────────────────────────────────────────────────────

REGISTRY: dict[str, callable] = {
    "STRING":  _op_string,
    "INT":     _op_int,
    "FLOAT":   _op_float,
    "BOOL":    _op_bool,
    "UPPER":   _op_upper,
    "LOWER":   _op_lower,
    "TRIM":    _op_trim,
    "REPLACE": _op_replace,
    "SLICE":   _op_slice,
    "CONCAT":  _op_concat,
    "JOIN":    _op_join,
    "IF":      _op_if,
    "NOW":     _op_now,
    "DATE":    _op_date,
}


# ── API schema ────────────────────────────────────────────────────────────────
# Returned by GET /api/operators — drives the frontend autocomplete

API_SCHEMA = [
    {
        "name": "STRING",
        "description": "Convert a value to a string",
        "signature": "STRING(value)",
        "args": [{"name": "value", "description": "The value to convert", "type": "any"}],
        "example": {"expression": "STRING(status_code)", "input": "200", "output": "\"200\""},
    },
    {
        "name": "INT",
        "description": "Convert a value to an integer",
        "signature": "INT(value)",
        "args": [{"name": "value", "description": "The value to convert", "type": "any"}],
        "example": {"expression": "INT(status_code_str)", "input": "\"200\"", "output": "200"},
    },
    {
        "name": "FLOAT",
        "description": "Convert a value to a float",
        "signature": "FLOAT(value)",
        "args": [{"name": "value", "description": "The value to convert", "type": "any"}],
        "example": {"expression": "FLOAT(price)", "input": "\"3.99\"", "output": "3.99"},
    },
    {
        "name": "BOOL",
        "description": "Convert a value to a boolean",
        "signature": "BOOL(value)",
        "args": [{"name": "value", "description": "The value to convert", "type": "any"}],
        "example": {"expression": "BOOL(flag)", "input": "\"true\"", "output": "true"},
    },
    {
        "name": "UPPER",
        "description": "Convert a string to uppercase",
        "signature": "UPPER(value)",
        "args": [{"name": "value", "description": "The string to uppercase", "type": "str"}],
        "example": {"expression": "UPPER(body.name)", "input": "\"hello\"", "output": "\"HELLO\""},
    },
    {
        "name": "LOWER",
        "description": "Convert a string to lowercase",
        "signature": "LOWER(value)",
        "args": [{"name": "value", "description": "The string to lowercase", "type": "str"}],
        "example": {"expression": "LOWER(body.name)", "input": "\"HELLO\"", "output": "\"hello\""},
    },
    {
        "name": "TRIM",
        "description": "Remove leading and trailing whitespace",
        "signature": "TRIM(value)",
        "args": [{"name": "value", "description": "The string to trim", "type": "str"}],
        "example": {"expression": "TRIM(body.name)", "input": "\"  hello  \"", "output": "\"hello\""},
    },
    {
        "name": "REPLACE",
        "description": "Replace all occurrences of a substring",
        "signature": "REPLACE(value, find, replacement)",
        "args": [
            {"name": "value",       "description": "The source string",     "type": "str"},
            {"name": "find",        "description": "Substring to find",     "type": "str"},
            {"name": "replacement", "description": "String to replace with","type": "str"},
        ],
        "example": {"expression": "REPLACE(body.slug, \"-\", \"_\")", "input": "\"hello-world\"", "output": "\"hello_world\""},
    },
    {
        "name": "SLICE",
        "description": "Extract a substring by start and end index",
        "signature": "SLICE(value, start, end)",
        "args": [
            {"name": "value", "description": "The source string", "type": "str"},
            {"name": "start", "description": "Start index",       "type": "int"},
            {"name": "end",   "description": "End index",         "type": "int"},
        ],
        "example": {"expression": "SLICE(body.name, 0, 3)", "input": "\"hello\"", "output": "\"hel\""},
    },
    {
        "name": "CONCAT",
        "description": "Join multiple values into a single string",
        "signature": "CONCAT(a, b, ...)",
        "args": [{"name": "a", "description": "Values to join", "type": "any", "variadic": True}],
        "example": {"expression": "CONCAT(body.first_name, \" \", body.last_name)", "input": "{}", "output": "\"John Smith\""},
    },
    {
        "name": "JOIN",
        "description": "Join an array into a string with a separator",
        "signature": "JOIN(array, separator)",
        "args": [
            {"name": "array",     "description": "The array to join", "type": "arr"},
            {"name": "separator", "description": "Separator string",  "type": "str"},
        ],
        "example": {"expression": "JOIN(body.tags, \", \")", "input": "[\"a\",\"b\"]", "output": "\"a, b\""},
    },
    {
        "name": "IF",
        "description": "Return one of two values based on a condition",
        "signature": "IF(condition, true_value, false_value)",
        "args": [
            {"name": "condition",   "description": "The condition",    "type": "any"},
            {"name": "true_value",  "description": "Value if true",    "type": "any"},
            {"name": "false_value", "description": "Value if false",   "type": "any"},
        ],
        "example": {"expression": "IF(ok, \"success\", \"failed\")", "input": "true", "output": "\"success\""},
    },
    {
        "name": "NOW",
        "description": "Return the current UTC timestamp as an ISO string",
        "signature": "NOW()",
        "args": [],
        "example": {"expression": "NOW()", "input": "null", "output": "\"2026-03-04T12:00:00Z\""},
    },
    {
        "name": "DATE",
        "description": "Format a timestamp using a strftime format string",
        "signature": "DATE(value, format)",
        "args": [
            {"name": "value",  "description": "ISO timestamp string", "type": "str"},
            {"name": "format", "description": "strftime format",      "type": "str"},
        ],
        "example": {"expression": "DATE(body.created_at, \"%d %b %Y\")", "input": "\"2026-03-04\"", "output": "\"04 Mar 2026\""},
    },
]