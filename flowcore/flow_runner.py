"""
FlowRunner
==========
Takes the flow JSON and runs it. No database, no magic.
Supports nested iteration — each log entry carries an `iteration_path`
(a list of ints) so the frontend can reconstruct which iteration context
any log belongs to.  e.g. [2] means "outer iteration index 2",
[2, 4] means "inner iteration index 4 within outer iteration index 2".
"""

from __future__ import annotations

import json
import traceback
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any
from collections import deque
import time
import threading
from flowcore.nodes import dispatch, ConditionFalseError
from flowcore.resolve import set_resolve_context


# ── Helpers ──────────────────────────────────────────────────────────────────

class LogList(list):
    """A list that calls a callback on every append for real-time log streaming."""
    def __init__(self, callback=None):
        super().__init__()
        self._callback = callback

    def append(self, entry):
        super().append(entry)
        if self._callback and isinstance(entry, dict):
            try:
                self._callback(entry)
            except Exception:
                pass

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]


def _log_entry(node_id, level, message, detail=None, iteration_path=None):
    entry = {
        "node_id":    node_id,
        "level":      level,
        "message":    message,
        "detail":     json.dumps(detail, default=str) if detail and not isinstance(detail, str) else (detail or ""),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    if iteration_path is not None and len(iteration_path) > 0:
        entry["iteration_path"] = list(iteration_path)
        entry["iteration"] = iteration_path[-1]
    return entry


# ── Runner ───────────────────────────────────────────────────────────────────

class FlowRunner:

    def __init__(self, flow_json: dict, start_node_id: str | None = None, cancel_check=None):
        self._cancel_check = cancel_check
        self.flow_id = flow_json.get("id")
        self.tenant_id = flow_json.get("tenant_id")

        self.nodes = flow_json.get("nodes", [])
        self.edges = flow_json.get("edges", [])
        self.start_node_id = str(start_node_id) if start_node_id else None
        self._node_map = {n["node_id"]: n for n in self.nodes}
        self._iterate_nodes: set[str] = {
            n["node_id"] for n in self.nodes
            if n.get("node_type") == "transform" and
               n.get("config", {}).get("mode") == "iterate"
        }
        self._gather_nodes: set[str] = {
            n["node_id"] for n in self.nodes
            if n.get("node_type") == "transform" and
               n.get("config", {}).get("mode") == "gather"
        }
        print(f"FlowRunner init — start_node_id: {self.start_node_id}")

        if not self.nodes:
            raise ValueError("Flow has no nodes.")

        self._success_children: dict[str, list[str]] = defaultdict(list)
        self._failure_children: dict[str, list[str]] = defaultdict(list)
        self._all_children:     dict[str, list[str]] = defaultdict(list)
        self._parents:          dict[str, list[str]] = defaultdict(list)

        for e in self.edges:
            src = e["source_node_id"]
            tgt = e["target_node_id"]
            out = e.get("source_output", "output_1")

            self._all_children[src].append(tgt)
            self._parents[tgt].append(src)

            if out == "output_2":
                self._failure_children[src].append(tgt)
            else:
                self._success_children[src].append(tgt)

        self._cycle_nodes: set[str] = self._detect_cycles()
        self.MAX_LOOP_ITERATIONS = 100

    def _detect_cycles(self) -> set[str]:
        cycle_nodes = set()
        for start_id in self._node_map.keys():
            visited = set()
            stack = list(self._success_children.get(start_id, []))
            while stack:
                nid = stack.pop()
                if nid == start_id:
                    cycle_nodes.add(start_id)
                    break
                if nid in visited:
                    continue
                visited.add(nid)
                stack.extend(self._success_children.get(nid, []))
        return cycle_nodes

    def run(self, input_data: dict | None = None, replay_data: dict | None = None) -> dict:
        logs: list[dict] = []
        output: Any = input_data or {}
        error: str | None = None

        try:
            output = self._execute(input_data or {}, logs, replay_data=replay_data)
            has_errors = any(l["level"] == "error" for l in logs)
            status = "error" if has_errors else "success"
            if has_errors:
                error = "One or more nodes failed — check node outputs for details"
        except Exception as exc:
            status = "error"
            error  = str(exc)
            logs.append(_log_entry("__runner__", "error",
                                   f"Run aborted: {exc}",
                                   json.dumps({
                                       "error": str(exc),
                                       "type": type(exc).__name__,
                                       "traceback": traceback.format_exc().split("\n"),
                                   }, default=str)))

        return {
            "status": status,
            "output": output,
            "error":  error,
            "logs":   logs,
        }

    def run_until_form(self, input_data: dict, data_bus: dict = None,
                       skip_node: str = None) -> dict:
        logs: list[dict] = []
        output: Any = input_data or {}
        error: str | None = None
        paused_at: str | None = None

        try:
            output, paused_at, data_bus = self._execute_until_form(
                input_data, logs, data_bus=data_bus or {}, skip_node=skip_node
            )
            has_errors = any(l["level"] == "error" for l in logs)
            status = "error" if has_errors else ("waiting" if paused_at else "success")
            if has_errors:
                error = "One or more nodes failed"
        except Exception as exc:
            status = "error"
            error = str(exc)
            logs.append(_log_entry("__runner__", "error",
                                   f"Run aborted: {exc}",
                                   json.dumps({"error": str(exc), "type": type(exc).__name__,
                                               "traceback": traceback.format_exc().split("\n")}, default=str)))

        result = {
            "status": status,
            "output": output,
            "error": error,
            "logs": logs,
            "data_bus": data_bus if data_bus else {},
        }
        if paused_at:
            result["paused_at"] = paused_at
        return result

    def _execute_until_form(self, initial_data: dict, logs: list,
                            data_bus: dict = None, skip_node: str = None):
        set_resolve_context(tenant_id=self.tenant_id)

        if data_bus is None:
            data_bus = {}

        all_node_ids = set(self._node_map.keys())
        has_parent = set(self._parents.keys())

        if skip_node:
            start_ids = self._success_children.get(skip_node, [])
        else:
            start_ids = list(all_node_ids - has_parent)

        if not start_ids:
            return initial_data, None, data_bus

        last_output: Any = initial_data
        failed_nodes: set[str] = set()
        visited: set[str] = set()
        queue: deque = deque()
        queued: set[str] = set(start_ids)

        for sid in start_ids:
            parent_data = data_bus.get(skip_node, initial_data) if skip_node else initial_data
            queue.append((sid, parent_data))

        while queue:
            if self._cancel_check and self._cancel_check():
                logs.append(_log_entry("__runner__", "info", "⛔  Run cancelled by user"))
                break

            node_id, input_data = queue.popleft()
            unfinished_parents = [
                p for p in self._parents.get(node_id, [])
                if p not in visited and p not in failed_nodes
                   and p != skip_node
                   and p not in data_bus
            ]
            if unfinished_parents:
                queue.append((node_id, input_data))
                continue

            visited.add(node_id)
            node = self._node_map[node_id]
            node_type = node.get("node_type", "")
            label = node.get("label") or node_type
            config = node.get("config", {})

            if node_type == "webform" and node_id != skip_node:
                logs.append(_log_entry(node_id, "info",
                                       f"⏸  {label} — waiting for form submission"))
                return last_output, node_id, data_bus

            gathered, ref_data = self._gather_inputs(node_id, data_bus, initial_data)

            logs.append(_log_entry(node_id, "info",
                                   f"▶  {label}  [{node_type}]",
                                   {"input_keys": list(gathered.keys())}))
            logs.append(_log_entry(node_id, "input",
                                   "__node_input__",
                                   json.dumps(gathered, default=str)))

            try:
                output = dispatch(self.tenant_id, self.flow_id, node_type, config, gathered, ref_data=ref_data)
                data_bus[node_id] = output
                last_output = output

                logs.append(_log_entry(node_id, "success",
                                       f"✓  {label}  [{node_type}]", output))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps(output, default=str)))

                next_nodes = self._success_children.get(node_id, [])

            except ConditionFalseError as exc:
                data_bus[node_id] = exc.output
                last_output = exc.output
                logs.append(_log_entry(node_id, "info",
                                       f"↪  {label} — condition false"))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps(exc.output, default=str)))
                next_nodes = self._failure_children.get(node_id, [])

            except Exception as exc:
                failed_nodes.add(node_id)
                logs.append(_log_entry(node_id, "error",
                                       f"✗  {label}: {exc}",
                                       json.dumps({"error": str(exc), "type": type(exc).__name__}, default=str)))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps({"success": False, "error": str(exc)}, default=str)))

                if node.get("has_failure_path"):
                    data_bus[node_id] = {"__failure__": True, "error": str(exc)}
                    next_nodes = self._failure_children.get(node_id, [])
                else:
                    next_nodes = []

            for next_id in next_nodes:
                if next_id not in queued:
                    queued.add(next_id)
                    queue.append((next_id, data_bus.get(node_id, gathered)))

        return last_output, None, data_bus

    def _execute(self, initial_data: dict, logs: list, replay_data: dict | None = None) -> Any:
        set_resolve_context(tenant_id=self.tenant_id)

        all_node_ids = set(self._node_map.keys())
        has_parent = set(self._parents.keys())
        root_node_ids = all_node_ids - has_parent

        if not root_node_ids:
            if self._cycle_nodes:
                entry = None
                for n in self.nodes:
                    nid = n["node_id"]
                    if nid in self._cycle_nodes:
                        non_cycle_parents = [p for p in self._parents.get(nid, []) if p not in self._cycle_nodes]
                        if non_cycle_parents:
                            entry = nid
                            break
                if not entry:
                    for n in self.nodes:
                        if n["node_id"] in self._cycle_nodes:
                            entry = n["node_id"]
                            break
                root_node_ids = {entry}
            else:
                raise ValueError("No root node found — possible cycle.")

        if self.start_node_id:
            if self.start_node_id not in self._node_map:
                raise ValueError(f"Start node '{self.start_node_id}' not found in flow.")
            active_roots = {self.start_node_id}
        else:
            active_roots = root_node_ids

        data_bus: dict[str, Any] = {}
        last_output: Any = initial_data
        failed_nodes: set[str] = set()
        visited: set[str] = set()
        run_count: dict[str, int] = {}
        queue: deque = deque()
        queued: set[str] = set(active_roots)

        for root_id in sorted(active_roots):
            queue.append((root_id, initial_data))

        while queue:
            node_id, input_data = queue.popleft()

            if node_id in self._cycle_nodes:
                if run_count.get(node_id, 0) >= self.MAX_LOOP_ITERATIONS:
                    logs.append(_log_entry(node_id, "error",
                                           f"⛔  Loop limit reached ({self.MAX_LOOP_ITERATIONS} iterations)"))
                    continue
                unfinished_parents = [
                    p for p in self._parents.get(node_id, [])
                    if p not in visited and p not in failed_nodes
                       and p not in self._cycle_nodes
                ]
            elif node_id in active_roots:
                unfinished_parents = []
            else:
                unfinished_parents = [
                    p for p in self._parents.get(node_id, [])
                    if p not in visited and p not in failed_nodes
                ]

            if unfinished_parents:
                queue.append((node_id, input_data))
                continue

            visited.add(node_id)
            run_count[node_id] = run_count.get(node_id, 0) + 1

            node = self._node_map[node_id]
            node_type = node.get("node_type", "")
            label = node.get("label") or node_type
            config = node.get("config", {})

            gathered, ref_data = self._gather_inputs(node_id, data_bus, initial_data)

            loop_iter = run_count[node_id] if node_id in self._cycle_nodes and run_count[node_id] > 1 else None
            iter_label = f" (loop {loop_iter})" if loop_iter else ""
            logs.append(_log_entry(node_id, "info",
                                   f"▶  {label}  [{node_type}]{iter_label}",
                                   {"input_keys": list(gathered.keys())}))
            logs.append(_log_entry(node_id, "input",
                                   "__node_input__",
                                   json.dumps(gathered, default=str)))
            logs.append(_log_entry(node_id, "debug",
                                   f"⏱  {label} starting…",
                                   json.dumps({"type": node_type, "config_keys": list(config.keys()) if config else []}, default=str)))

            try:
                if replay_data and node_id in replay_data:
                    output = replay_data[node_id]
                    logs.append(_log_entry(node_id, "info",
                                           f"↻  {label} — replayed from cache"))
                else:
                    output = self._dispatch_with_retry(
                        node_id, node_type, config, gathered, label, logs,
                        ref_data=ref_data
                    )

                data_bus[node_id] = output
                last_output = output

                logs.append(_log_entry(node_id, "success",
                                       f"✓  {label}  [{node_type}]", output))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps(output, default=str)))
                logs.append(_log_entry(node_id, "debug",
                                       f"⏱  {label} completed",
                                       json.dumps({"type": node_type, "output_keys": list(output.keys()) if isinstance(output, dict) else type(output).__name__}, default=str)))

                if output.get("__iterate__"):
                    arr = output["__array__"]
                    variable = output["__variable__"]
                    next_ids = self._success_children.get(node_id, [])

                    gather_node_id = self._find_gather_node(node_id)
                    subtree_start_ids = [nid for nid in next_ids if nid != gather_node_id]

                    display_output = {
                        "mode": "iterate",
                        "count": len(arr),
                        variable: arr[0] if arr else {},
                    }
                    logs.append(_log_entry(node_id, "output",
                                           "__node_output__",
                                           json.dumps(display_output, default=str)))

                    iteration_results = []
                    per_item_label_bus = []
                    for i, item in enumerate(arr):
                        iter_path = [i]
                        logs.append(_log_entry(node_id, "info",
                                               "__iteration_start__",
                                               {"index": i, "total": len(arr), "item": item},
                                               iteration_path=iter_path))
                        item_input = {**gathered, variable: item}
                        subtree_result = self._run_subtree(
                            subtree_start_ids, item_input, logs,
                            iteration_path=iter_path,
                            gather_node_id=gather_node_id,
                            replay_data=replay_data,
                        )
                        result, sub_bus = subtree_result if isinstance(subtree_result, tuple) else (subtree_result, {})
                        item_label_bus = {}
                        for nid, nout in sub_bus.items():
                            node_def = self._node_map.get(str(nid), {})
                            lbl = (node_def.get("label") or "").strip()
                            if lbl:
                                item_label_bus[lbl] = nout
                        if gather_node_id:
                            if result is not None and isinstance(result, dict):
                                upstream_keys = set(gathered.keys())
                                clean = {k: v for k, v in result.items()
                                         if not k.startswith("__") and k not in upstream_keys and k != variable}
                                iteration_results.append(clean if clean else item)
                            else:
                                iteration_results.append(item)
                            per_item_label_bus.append(item_label_bus)
                        else:
                            if result is not None:
                                iteration_results.append(result)

                    if gather_node_id:
                        gather_node = self._node_map[gather_node_id]
                        gather_label = gather_node.get("label") or "Gather"
                        gather_config = gather_node.get("config", {})
                        gather_input = {
                            **gathered,
                            "__gather_items__": iteration_results,
                            "__per_item_label_bus__": per_item_label_bus,
                        }

                        logs.append(_log_entry(gather_node_id, "info",
                                               f"▶  {gather_label}  [transform/gather]",
                                               {"item_count": len(iteration_results)}))
                        logs.append(_log_entry(gather_node_id, "input",
                                               "__node_input__",
                                               json.dumps({"items_count": len(iteration_results)}, default=str)))

                        try:
                            gather_ref = ref_data  # gather uses same ref context as parent
                            gather_output = dispatch(self.tenant_id, self.flow_id, "transform", gather_config, gather_input, ref_data=gather_ref)
                            data_bus[gather_node_id] = gather_output
                            last_output = gather_output
                            visited.add(gather_node_id)

                            logs.append(_log_entry(gather_node_id, "success",
                                                   f"✓  {gather_label}  [transform/gather]", gather_output))
                            logs.append(_log_entry(gather_node_id, "output",
                                                   "__node_output__",
                                                   json.dumps(gather_output, default=str)))

                            next_nodes = self._success_children.get(gather_node_id, [])
                        except Exception as exc:
                            failed_nodes.add(gather_node_id)
                            logs.append(_log_entry(gather_node_id, "error",
                                                   f"✗  {gather_label}: {exc}",
                                                   json.dumps({"error": str(exc), "type": type(exc).__name__, "traceback": traceback.format_exc().split("\n")}, default=str)))
                            next_nodes = []
                    else:
                        last_output = iteration_results
                        next_nodes = []
                else:
                    next_nodes = self._success_children.get(node_id, [])
            except StopIteration as exc:
                logs.append(_log_entry(node_id, "info",
                                       f"⏹  {label} — stopped: {exc}"))
                break

            except ConditionFalseError as exc:
                data_bus[node_id] = exc.output
                last_output = exc.output
                logs.append(_log_entry(node_id, "info",
                                       f"↪  {label} — condition false, taking failure path"))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps(exc.output, default=str)))
                next_nodes = self._failure_children.get(node_id, [])

            except Exception as exc:
                failed_nodes.add(node_id)
                error_detail = {
                    "error": str(exc),
                    "type": type(exc).__name__,
                    "traceback": traceback.format_exc().split("\n"),
                }
                logs.append(_log_entry(node_id, "error",
                                       f"✗  {label}: {exc}",
                                       json.dumps(error_detail, default=str)))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps({"success": False, "error": str(exc), "error_type": type(exc).__name__}, default=str)))

                if node.get("has_failure_path"):
                    data_bus[node_id] = {"__failure__": True, "error": str(exc), "error_type": type(exc).__name__}
                    logs.append(_log_entry(node_id, "info",
                                           f"↪  continuing via failure path"))
                    next_nodes = self._failure_children.get(node_id, [])
                else:
                    logs.append(_log_entry(node_id, "info",
                                           f"⛔  {label} — no failure path, stopping downstream"))
                    next_nodes = []

            for next_id in next_nodes:
                next_input = data_bus.get(node_id, gathered)
                if next_id in self._cycle_nodes:
                    queue.append((next_id, next_input))
                elif next_id not in queued:
                    queued.add(next_id)
                    queue.append((next_id, next_input))

        return last_output

    def _gather_inputs(self, node_id, data_bus, initial_data):
        """
        Gather inputs for a node.
        Returns (merged, ref_data) where:
          - merged: flat dict of upstream outputs for the node to use as input_data
          - ref_data: {nodeId}_{label} -> output mapping for template resolution only
        """
        upstream = self._parents.get(node_id, [])
        if not upstream:
            return dict(initial_data), {}

        visited = set()
        queue = list(upstream)
        ancestor_order = []
        while queue:
            src_id = queue.pop(0)
            if src_id in visited:
                continue
            visited.add(src_id)
            ancestor_order.append(src_id)
            for parent_id in self._parents.get(src_id, []):
                if parent_id not in visited:
                    queue.append(parent_id)

        merged = {}
        ref_data = {}
        for src_id in ancestor_order:
            out = data_bus.get(src_id)
            if out is not None:
                if isinstance(out, dict):
                    merged.update(out)
                else:
                    merged[src_id] = out

                src_node = self._node_map.get(src_id, {})
                src_label = src_node.get("label", "").strip()
                if src_label:
                    ref_data[f"{src_id}_{src_label}"] = out

        return merged, ref_data

    def _find_gather_node(self, iterate_node_id: str) -> str | None:
        visited = set()
        queue = list(self._all_children.get(iterate_node_id, []))
        while queue:
            nid = queue.pop(0)
            if nid in visited:
                continue
            visited.add(nid)
            if nid in self._gather_nodes:
                return nid
            queue.extend(self._all_children.get(nid, []))
        return None

    def _run_subtree(self, start_ids, input_data, logs, iteration_path=None,
                     gather_node_id=None, replay_data=None):
        if iteration_path is None:
            iteration_path = []

        sub_data_bus: dict[str, Any] = {}

        for sid in start_ids:
            for parent_id in self._parents.get(sid, []):
                sub_data_bus[parent_id] = input_data

        sub_visited:  set[str] = set()
        sub_failed:   set[str] = set()
        sub_queue:    deque    = deque()
        sub_queued:   set[str] = set(start_ids)
        last_output:  Any      = input_data

        for sid in start_ids:
            sub_queue.append((sid, input_data))

        while sub_queue:
            if self._cancel_check and self._cancel_check():
                return None, {}

            node_id, node_input = sub_queue.popleft()

            unfinished = [
                p for p in self._parents.get(node_id, [])
                if p in sub_queued
                and p not in sub_visited
                and p not in sub_failed
            ]
            if unfinished:
                sub_queue.append((node_id, node_input))
                continue

            sub_visited.add(node_id)

            if gather_node_id and node_id == gather_node_id:
                continue

            node = self._node_map[node_id]
            node_type = node.get("node_type", "")
            label = node.get("label") or node_type
            config = node.get("config", {})

            gathered, ref_data = self._gather_sub_inputs(node_id, sub_data_bus, node_input)

            logs.append(_log_entry(node_id, "input",
                                   "__node_input__",
                                   json.dumps(gathered, default=str),
                                   iteration_path=iteration_path))
            logs.append(_log_entry(node_id, "debug",
                                   f"  ↳ ⏱ {label} starting…",
                                   json.dumps({"type": node_type}, default=str),
                                   iteration_path=iteration_path))

            try:
                if replay_data and node_id in replay_data:
                    output = replay_data[node_id]
                    logs.append(_log_entry(node_id, "info",
                                           f"  ↳ ↻ {label} — replayed from cache",
                                           iteration_path=iteration_path))
                else:
                    output = self._dispatch_with_retry(
                        node_id, node_type, config, gathered, label, logs,
                        iteration_path=iteration_path, ref_data=ref_data
                    )

                sub_data_bus[node_id] = output
                last_output = output

                logs.append(_log_entry(node_id, "success",
                                       f"  ↳ {label}", output,
                                       iteration_path=iteration_path))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps(output, default=str),
                                       iteration_path=iteration_path))
                logs.append(_log_entry(node_id, "debug",
                                       f"  ↳ ⏱ {label} completed",
                                       json.dumps({"output_keys": list(output.keys()) if isinstance(output, dict) else type(output).__name__}, default=str),
                                       iteration_path=iteration_path))

                if output.get("__iterate__"):
                    arr = output["__array__"]
                    variable = output["__variable__"]
                    inner_next_ids = self._success_children.get(node_id, [])

                    inner_gather_id = self._find_gather_node(node_id)
                    inner_start_ids = [nid for nid in inner_next_ids if nid != inner_gather_id]

                    display_output = {
                        "mode": "iterate",
                        "count": len(arr),
                        variable: arr[0] if arr else {},
                    }
                    logs.append(_log_entry(node_id, "output",
                                           "__node_output__",
                                           json.dumps(display_output, default=str),
                                           iteration_path=iteration_path))

                    inner_results = []
                    for i, item in enumerate(arr):
                        inner_path = iteration_path + [i]
                        logs.append(_log_entry(node_id, "info",
                                               "__iteration_start__",
                                               {"index": i, "total": len(arr), "item": item},
                                               iteration_path=inner_path))
                        item_input = {**gathered, variable: item}
                        inner_subtree_result = self._run_subtree(
                            inner_start_ids, item_input, logs,
                            iteration_path=inner_path,
                            gather_node_id=inner_gather_id,
                            replay_data=replay_data,
                        )
                        result = inner_subtree_result[0] if isinstance(inner_subtree_result, tuple) else inner_subtree_result
                        if result is not None:
                            if isinstance(result, dict):
                                clean = {k: v for k, v in result.items()
                                         if not k.startswith("__")}
                                inner_results.append(clean)
                            else:
                                inner_results.append(result)
                        else:
                            inner_results.append(item)

                    if inner_gather_id:
                        gather_node = self._node_map[inner_gather_id]
                        gather_label = gather_node.get("label") or "Gather"
                        gather_config = gather_node.get("config", {})
                        gather_input = {**gathered, "__gather_items__": inner_results}

                        logs.append(_log_entry(inner_gather_id, "success",
                                               f"  ↳ {gather_label}", None,
                                               iteration_path=iteration_path))

                        try:
                            gather_output = dispatch(self.tenant_id, self.flow_id, "transform", gather_config, gather_input, ref_data=ref_data)
                            sub_data_bus[inner_gather_id] = gather_output
                            last_output = gather_output
                            sub_visited.add(inner_gather_id)

                            logs.append(_log_entry(inner_gather_id, "output",
                                                   "__node_output__",
                                                   json.dumps(gather_output, default=str),
                                                   iteration_path=iteration_path))

                            next_nodes = self._success_children.get(inner_gather_id, [])
                        except Exception as exc:
                            sub_failed.add(inner_gather_id)
                            next_nodes = []
                    else:
                        last_output = inner_results
                        next_nodes = []
                else:
                    next_nodes = self._success_children.get(node_id, [])

            except ConditionFalseError as exc:
                sub_data_bus[node_id] = exc.output
                last_output = exc.output
                logs.append(_log_entry(node_id, "info",
                                       f"  ↳ {label} — condition false",
                                       iteration_path=iteration_path))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps(exc.output, default=str),
                                       iteration_path=iteration_path))
                next_nodes = self._failure_children.get(node_id, [])

            except Exception as exc:
                sub_failed.add(node_id)
                error_detail = {
                    "error": str(exc),
                    "type": type(exc).__name__,
                    "traceback": traceback.format_exc().split("\n"),
                }
                logs.append(_log_entry(node_id, "error",
                                       f"  ↳ {label}: {exc}",
                                       json.dumps(error_detail, default=str),
                                       iteration_path=iteration_path))
                logs.append(_log_entry(node_id, "output",
                                       "__node_output__",
                                       json.dumps({"success": False, "error": str(exc), "error_type": type(exc).__name__}, default=str),
                                       iteration_path=iteration_path))
                if node.get("has_failure_path"):
                    sub_data_bus[node_id] = {"__failure__": True, "error": str(exc), "error_type": type(exc).__name__}
                    next_nodes = self._failure_children.get(node_id, [])
                else:
                    logs.append(_log_entry(node_id, "info",
                                           f"  ↳ ⛔ no failure path, stopping iteration branch",
                                           iteration_path=iteration_path))
                    return None, {}

            for next_id in next_nodes:
                if next_id not in sub_queued:
                    sub_queued.add(next_id)
                    sub_queue.append((next_id, sub_data_bus.get(node_id, gathered)))

        return last_output, sub_data_bus

    def _gather_sub_inputs(self, node_id, sub_data_bus, initial_data):
        """
        Gather inputs for a node within a subtree (iteration).
        Returns (merged, ref_data) — same contract as _gather_inputs.
        """
        upstream = self._parents.get(node_id, [])
        if not upstream:
            return dict(initial_data), {}

        visited = set()
        queue = list(upstream)
        ancestor_order = []
        while queue:
            src_id = queue.pop(0)
            if src_id in visited:
                continue
            visited.add(src_id)
            ancestor_order.append(src_id)
            for parent_id in self._parents.get(src_id, []):
                if parent_id not in visited:
                    queue.append(parent_id)

        merged = {}
        ref_data = {}
        for src_id in ancestor_order:
            out = sub_data_bus.get(src_id)
            if out is not None:
                if isinstance(out, dict):
                    merged.update(out)
                else:
                    merged[src_id] = out

                src_node = self._node_map.get(src_id, {})
                src_label = src_node.get("label", "").strip()
                if src_label:
                    ref_data[f"{src_id}_{src_label}"] = out

        if not merged:
            return dict(initial_data), ref_data

        return {**initial_data, **merged}, ref_data

    def _dispatch_with_retry(self, node_id, node_type, config, gathered, label, logs,
                             iteration_path=None, ref_data=None):

        retry_count = int(config.get("retry_count", 0) or 0)
        retry_delay = int(config.get("retry_delay", 1) or 1)
        timeout = int(config.get("node_timeout", 0) or 0)

        last_exc = None
        for attempt in range(retry_count + 1):
            try:
                if timeout > 0:
                    output = self._run_with_timeout(node_type, config, gathered, timeout, ref_data=ref_data)
                else:
                    output = dispatch(self.tenant_id, self.flow_id, node_type, config, gathered, ref_data=ref_data)
                return output
            except Exception as exc:
                last_exc = exc
                if attempt < retry_count:
                    logs.append(_log_entry(node_id, "info",
                                           f"⟳  {label} — attempt {attempt + 1} failed: {exc}, retrying in {retry_delay}s…",
                                           iteration_path=iteration_path))
                    time.sleep(retry_delay)
                    retry_delay = min(retry_delay * 2, 120)

        raise last_exc

    def _run_with_timeout(self, node_type, config, gathered, timeout, ref_data=None):
        """Run a node with a timeout using threading."""
        result = [None]
        error = [None]

        def target():
            try:
                result[0] = dispatch(self.tenant_id, self.flow_id, node_type, config, gathered, ref_data=ref_data)
            except Exception as e:
                error[0] = e

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            raise TimeoutError(f"Node timed out after {timeout}s")
        if error[0]:
            raise error[0]
        return result[0]