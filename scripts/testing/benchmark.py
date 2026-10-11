"""Report-only authenticated HTTP benchmarks; correctness failures remain fatal.

Run the same driver against --source-root base/src and head/src on one runner.
API timings include client serialization, loopback HTTP, broker and SQLite work.
They do not measure isolated broker execution, native receipt, or model latency.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import importlib
import json
import math
from pathlib import Path
import platform
import random
import sqlite3
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parents[2]
SCENARIOS = ("serial_roundtrip", "concurrent_backlog")


@dataclass(frozen=True)
class Config:
    trials: int = 3
    warmup: int = 1
    messages: int = 20
    workers: int = 4
    payload_bytes: int = 256
    seed: int = 0

    def validate(self):
        for name, low, high in (("trials", 1, 20), ("warmup", 0, 10),
                                ("messages", 1, 1000), ("workers", 1, 16),
                                ("payload_bytes", 0, 65536), ("seed", 0, 2**32 - 1)):
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer between {low} and {high}")


def stats(values):
    """Nearest-rank percentiles; retain counts to expose small sample sets."""
    values = list(values)
    if not values or any(not isinstance(v, (int, float)) or isinstance(v, bool)
                         or not math.isfinite(v) or v < 0 for v in values):
        raise ValueError("Statistics require nonempty finite nonnegative samples")
    values.sort()
    return {"count": len(values), "min": values[0], "max": values[-1],
            "p50": values[math.ceil(len(values) * .5) - 1],
            "p95": values[math.ceil(len(values) * .95) - 1]}


def fingerprint(paths, base):
    files = {str(path.relative_to(base)): hashlib.sha256(path.read_bytes()).hexdigest()
             for path in sorted(paths)}
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    return {"sha256": digest, "files": files}


def runtime(source_root):
    """Select product code before importing the shared owned-resource fixture."""
    source_root = Path(source_root).resolve()
    if not (source_root / "xordination" / "__init__.py").is_file():
        raise ValueError("source-root must contain the xordination package")
    sys.path.insert(0, str(source_root))
    sys.path.insert(1, str(ROOT / "tests"))
    modules = [importlib.import_module("xordination" + suffix)
               for suffix in ("", ".store", ".server", ".client")]
    # A PYTHONPATH or already-loaded module must not silently replace the base.
    for name, module in tuple(sys.modules.items()):
        if name == "xordination" or name.startswith("xordination."):
            if not Path(module.__file__).resolve().is_relative_to(source_root):
                raise ValueError("Product import escaped the selected source-root: " + name)
    from helpers import owned_broker
    return modules[-1].Client, owned_broker


def require(condition, message):
    # Ordinary asserts disappear under python -O; benchmark checks must not.
    if not condition:
        raise AssertionError(message)


class Calls:
    def __init__(self):
        self.samples = {}
        self.lock = threading.Lock()
        self.receipt_ids = set()

    def call(self, phase, client, operation, **arguments):
        started = time.monotonic_ns()
        result = client.call(operation, **arguments)
        elapsed = (time.monotonic_ns() - started) / 1_000_000
        with self.lock:
            self.samples.setdefault(phase, []).append(elapsed)
        return result


def check_message(message, binding, sender, body):
    require(message["from"] == sender["participant"]
            and message["from_generation"] == sender["generation"]
            and message["to"] == binding["participant"]
            and message["delivery_generation"] == binding["generation"]
            and message["body"] == body, "Message content or binding mismatch")


def acknowledge(calls, client, binding, message_id, phase="ack"):
    result = calls.call(phase, client, "ack", message_id=message_id)
    receipt = result["receipt"]
    require(result["acknowledged"] and result["message_id"] == message_id
            and receipt["message_id"] == message_id
            and receipt["observer"] == binding["participant"]
            and receipt["generation"] == binding["generation"]
            and receipt["kind"] == "client_ack", "Missing or misbound receipt")
    with calls.lock:
        require(receipt["id"] not in calls.receipt_ids, "Duplicate receipt IDs")
        calls.receipt_ids.add(receipt["id"])
    return receipt


def serial(config, pairs, calls, body):
    (sender, alice), (recipient, bob) = pairs[0]
    cursors = [None, None]
    for number in range(config.messages):
        started = time.monotonic_ns()
        sent = calls.call("send", sender, "send", to=bob["participant"],
                          body=body, key=f"question-{number}")
        page = calls.call("inbox", recipient, "inbox", cursor=cursors[1])
        require([m["id"] for m in page["messages"]] == [sent["id"]]
                and not page["has_more"], "Missing or duplicate question")
        check_message(page["messages"][0], bob, alice, body)
        cursors[1] = page["cursor"]
        receipt = acknowledge(calls, recipient, bob, sent["id"])
        reply = calls.call("reply_send", recipient, "send", to=alice["participant"],
                           body=body, key=f"reply-{number}", reply_to=sent["id"])
        page = calls.call("reply_inbox", sender, "inbox", cursor=cursors[0])
        require([m["id"] for m in page["messages"]] == [reply["id"]]
                and not page["has_more"], "Missing or duplicate reply")
        check_message(page["messages"][0], alice, bob, body)
        cursors[0] = page["cursor"]
        acknowledge(calls, sender, alice, reply["id"], "reply_ack")
        status = calls.call("status", sender, "status", message_id=sent["id"])
        require(status["acknowledged"] and status["receipts"] == [receipt]
                and [m["id"] for m in status["replies"]] == [reply["id"]]
                and status["replies"][0]["reply_to"] == sent["id"],
                "Receipt or linked reply did not survive the broker boundary")
        calls.samples.setdefault("roundtrip", []).append(
            (time.monotonic_ns() - started) / 1_000_000)
    return {"completed_work_units": config.messages, "accepted_messages": 2 * config.messages,
            "work_unit": "two-way question/reply with both receipts and linked status",
            "pending_bound_per_recipient": 1}


def concurrent_backlog(config, pairs, calls, body):
    barrier = threading.Barrier(config.workers + 1, timeout=10)

    def worker(pair):
        (sender, alice), (recipient, bob) = pair
        barrier.wait()
        sent = [calls.call("send", sender, "send", to=bob["participant"],
                           body=body, key=f"burst-{number}") for number in range(config.messages)]
        expected = [message["id"] for message in sent]
        require(len(set(expected)) == len(expected), "Duplicate accepted message IDs")
        cursor, observed = None, []
        while len(observed) < len(expected):
            page = calls.call("inbox", recipient, "inbox", cursor=cursor, limit=100,
                              unacknowledged=True)
            require(bool(page["messages"]), "Backlog vanished before receipt")
            for message in page["messages"]:
                check_message(message, bob, alice, body)
                observed.append(message["id"])
                acknowledge(calls, recipient, bob, message["id"])
            cursor = page["cursor"]
        require(observed == expected, "Backlog lost, duplicated, or reordered messages")

    with ThreadPoolExecutor(max_workers=config.workers) as pool:
        futures = [pool.submit(worker, pair) for pair in pairs]
        barrier.wait()
        for future in futures:
            future.result()
    return {"completed_work_units": config.messages * config.workers,
            "accepted_messages": config.messages * config.workers,
            "work_unit": "one-way send/read/receipt",
            # Each worker accepts its complete burst before its first drain.
            # This is a per-recipient peak, not a sampled aggregate peak.
            "pending_bound_per_recipient": config.messages}


def storage_bytes(path):
    return sum(file.stat().st_size for file in path.parent.glob(path.name + "*")
               if file.is_file() and not file.name.endswith(".broker.lock"))


def trial(config, scenario, Client, owned_broker):
    started = time.monotonic_ns()
    with tempfile.TemporaryDirectory(prefix="xordination-benchmark-") as temporary:
        path = Path(temporary) / "state.sqlite3"
        with owned_broker(path) as (store, _, endpoint):
            store.create_space("benchmark")
            pairs = []
            for number in range(config.workers if scenario == "concurrent_backlog" else 1):
                pair = []
                for role in ("sender", "recipient"):
                    binding = store.join("benchmark", f"{role}-{number}")
                    client = Client(endpoint, binding["token"], timeout=10)
                    identity = client.call("identity")
                    require(identity["generation"] == binding["generation"],
                            "Authenticated participant was not ready")
                    pair.append((client, binding))
                pairs.append(pair)
            rng = random.Random(config.seed)
            body = "".join(rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=config.payload_bytes))
            calls = Calls()
            before = storage_bytes(path)
            setup_ms = (time.monotonic_ns() - started) / 1_000_000
            steady_start = time.monotonic_ns()
            workload = serial if scenario == "serial_roundtrip" else concurrent_backlog
            counts = workload(config, pairs, calls, body)
            steady_ns = time.monotonic_ns() - steady_start
            # Exclude final auditing from steady-state operation timings.
            for pair in pairs:
                for client, unused_binding in pair:
                    require(not client.call("inbox", unacknowledged=True)["messages"],
                            "Unacknowledged messages remain after the workload")
            retained = storage_bytes(path)
            cleanup_start = time.monotonic_ns()
        closed = storage_bytes(path)
    cleanup_ms = (time.monotonic_ns() - cleanup_start) / 1_000_000
    require(steady_ns > 0, "Monotonic clock did not advance")
    return {"scenario": scenario, "correctness": "verified", **counts,
            "setup_ms": setup_ms, "steady_ms": steady_ns / 1_000_000,
            "cleanup_ms": cleanup_ms, "samples_ms": calls.samples,
            "work_units_per_second": counts["completed_work_units"] * 1e9 / steady_ns,
            "storage_bytes": {"after_setup": before, "after_work": retained,
                              "after_close": closed}, "pending_after_work": 0}


def summarize(trials):
    summary = {}
    for scenario in SCENARIOS:
        selected = [result for result in trials if result["scenario"] == scenario]
        phases = selected[0]["samples_ms"]
        summary[scenario] = {
            "work_unit": selected[0]["work_unit"],
            "latency_ms": {phase: stats([value for result in selected
                                         for value in result["samples_ms"][phase]]) for phase in phases},
            "throughput_work_units_per_second": stats([r["work_units_per_second"] for r in selected]),
            "lifecycle_ms": {phase: stats([r[phase + "_ms"] for r in selected])
                             for phase in ("setup", "steady", "cleanup")}}
    return summary


def run(config, source_root=ROOT / "src", session_id=None):
    config.validate()
    source_root = Path(source_root).resolve()
    Client, owned_broker = runtime(source_root)
    product = fingerprint((source_root / "xordination").rglob("*.py"), source_root)
    manifest = {"config": asdict(config), "product": product,
                "measurement_session_id": session_id,
                "driver": fingerprint([Path(__file__).resolve(), ROOT / "tests" / "helpers.py"], ROOT),
                "environment": {"python": platform.python_version(), "implementation": platform.python_implementation(),
                                "platform": platform.platform(), "machine": platform.machine(),
                                "sqlite": sqlite3.sqlite_version},
                "scope": "in-process broker; authenticated loopback HTTP; fresh state per trial",
                "unmeasured": ["isolated broker execution", "native submission", "native receipt",
                               "model response", "remote network", "broker process RSS"],
                "percentiles": "nearest rank; pooled operation samples; throughput across trials"}
    results = []
    for scenario in SCENARIOS:
        for number in range(config.warmup + config.trials):
            result = trial(config, scenario, Client, owned_broker)
            if number >= config.warmup:
                results.append({"trial": number - config.warmup, **result})
    loaded = {name: str(Path(module.__file__).resolve()) for name, module in tuple(sys.modules.items())
              if name == "xordination" or name.startswith("xordination.")}
    require(all(Path(path).is_relative_to(source_root) for path in loaded.values()),
            "Product import escaped the selected source-root during measurement")
    manifest["loaded_product_modules"] = loaded
    require(product == fingerprint((source_root / "xordination").rglob("*.py"), source_root),
            "Product source changed during measurement")
    return {"schema_version": 1, "kind": "xordination_benchmark", "status": "verified",
            "performance_gate": "report_only", "manifest": manifest,
            "warmup_discarded": config.warmup * len(SCENARIOS),
            "trials": results, "summary": summarize(results)}


def compare(base, head):
    for report in (base, head):
        if (report.get("schema_version") != 1 or report.get("kind") != "xordination_benchmark"
                or report.get("status") != "verified"):
            raise ValueError("Comparison requires verified benchmark reports of the same schema")
    for field in ("config", "driver", "environment", "scope", "percentiles", "measurement_session_id"):
        if base["manifest"][field] != head["manifest"][field]:
            raise ValueError("Incomparable benchmark " + field)
    deltas = {}
    for scenario in SCENARIOS:
        before, after = base["summary"][scenario], head["summary"][scenario]
        values = {}
        for phase, samples in before["latency_ms"].items():
            for percentile in ("p50", "p95"):
                old, new = samples[percentile], after["latency_ms"][phase][percentile]
                stats([old, new])
                values[phase + "." + percentile] = {
                    "base_ms": old, "head_ms": new,
                    "change_percent": (new / old - 1) * 100 if old else None}
        deltas[scenario] = {"latency": values,
                            "work_unit": before["work_unit"],
                            "throughput_base": before["throughput_work_units_per_second"],
                            "throughput_head": after["throughput_work_units_per_second"]}
    return {"schema_version": 1, "kind": "xordination_benchmark_comparison",
            "performance_gate": "report_only", "base_product": base["manifest"]["product"],
            "head_product": head["manifest"]["product"], "same_runner_required": True,
            "measurement_session_id": base["manifest"]["measurement_session_id"],
            "same_runner_verification": "externally enforced by the orchestrator; no hardware attestation",
            "interpretation": "Observed variation is not evidence of a product improvement or regression.",
            "scenarios": deltas}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", "--source", type=Path, default=ROOT / "src")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--session-id", help="shared identifier assigned to a paired measurement session")
    parser.add_argument("--compare-base", type=Path)
    parser.add_argument("--compare-head", type=Path)
    for name, default in asdict(Config()).items():
        parser.add_argument("--" + name.replace("_", "-"), type=int, default=default)
    args = parser.parse_args(argv)
    try:
        if args.compare_base or args.compare_head:
            if not args.compare_base or not args.compare_head:
                raise ValueError("Both --compare-base and --compare-head are required")
            result = compare(json.loads(args.compare_base.read_text()), json.loads(args.compare_head.read_text()))
        else:
            config = Config(**{name: getattr(args, name) for name in asdict(Config())})
            result = run(config, args.source_root, args.session_id)
    except (AssertionError, ValueError, OSError, KeyError, TypeError, RuntimeError) as exc:
        result = {"schema_version": 1, "status": "failed", "error": type(exc).__name__, "message": str(exc)}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        print("Benchmark failed: " + str(exc), file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
