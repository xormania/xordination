"""Report-only authenticated HTTP benchmarks; correctness failures remain fatal.

Run the same driver against --source-root base/src and head/src on one runner.
API timings include client serialization, loopback HTTP, broker and SQLite work.
They do not measure isolated broker execution, native receipt, or model latency.
Broker resources, wait capacity and restart recovery use a separate
``xordination serve`` process so its CPU/RSS exclude the driver.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
import hashlib
import importlib
import inspect
import json
import math
import os
from pathlib import Path
import platform
import queue
import random
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace


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
    process_trials: int = 2
    idle_ms: int = 200
    wait_overflow: int = 2

    def validate(self):
        for name, low, high in (("trials", 1, 20), ("warmup", 0, 10),
                                ("messages", 1, 1000), ("workers", 1, 16),
                                ("payload_bytes", 0, 65536), ("seed", 0, 2**32 - 1),
                                ("process_trials", 1, 20), ("idle_ms", 0, 10000),
                                ("wait_overflow", 0, 16)):
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
    _package, store, server, client = modules
    return SimpleNamespace(Client=client.Client, BrokerError=client.BrokerError, Store=store.Store,
                           BrokerServer=server.BrokerServer, owned_broker=owned_broker)


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
                for client, _binding in pair:
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


# The broker child imports only the selected tree; -I drops PYTHONPATH and cwd.
BROKER_BOOT = """
import sys
from pathlib import Path
source = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(source))
import xordination.cli, xordination.server, xordination.store
for name, module in tuple(sys.modules.items()):
    if (name == "xordination" or name.startswith("xordination.")) \\
            and not Path(module.__file__).resolve().is_relative_to(source):
        sys.exit("Broker import escaped the selected source-root: " + name)
raise SystemExit(xordination.cli.main(["serve", "--state", sys.argv[2], "--port", sys.argv[3]]))
"""
WAIT_HOLD_S = 10
RETRYABLE = ("connection_error", "unavailable", "backpressure")


def elapsed_ms(started_ns, ended_ns=None):
    return ((time.monotonic_ns() if ended_ns is None else ended_ns) - started_ns) / 1_000_000


def proc_sample(pid):
    """Linux /proc CPU/RSS for one process; None where /proc is unavailable."""
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        status = Path(f"/proc/{pid}/status").read_text()
        ticks = os.sysconf("SC_CLK_TCK")
    except (OSError, ValueError, AttributeError):
        return None
    # The command name is parenthesized and may contain spaces.
    fields = stat[stat.rindex(")") + 2:].split()
    memory = {key: int(value.split()[0]) * 1024 for key, _, value in
              (line.partition(":") for line in status.splitlines()) if key in ("VmRSS", "VmHWM")}
    if len(memory) != 2:
        return None
    return {"at_ns": time.monotonic_ns(), "cpu_ms": (int(fields[11]) + int(fields[12])) * 1000 / ticks,
            "rss_bytes": memory["VmRSS"], "peak_rss_bytes": memory["VmHWM"]}


def resource_window(before, after):
    if before is None or after is None:
        return None
    window_ms = elapsed_ms(before["at_ns"], after["at_ns"])
    cpu_ms = after["cpu_ms"] - before["cpu_ms"]
    return {"window_ms": window_ms, "cpu_ms": cpu_ms,
            "cpu_percent": cpu_ms * 100 / window_ms if window_ms else 0,
            "rss_bytes": after["rss_bytes"], "peak_rss_bytes": after["peak_rss_bytes"]}


class BrokerProcess:
    """An owned ``xordination serve`` child on loopback; port 0 lets the kernel choose."""

    def __init__(self, source_root, state, port=0):
        self.stderr = tempfile.TemporaryFile()
        self.port = port
        self.started_ns = time.monotonic_ns()
        self.process = subprocess.Popen(
            [sys.executable, "-I", "-c", BROKER_BOOT, str(source_root), str(state), str(port)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=self.stderr,
            cwd=state.parent, text=True, encoding="utf-8")
        self.lines = queue.Queue()
        threading.Thread(target=lambda: self.lines.put((self.process.stdout.readline(), time.monotonic_ns())),
                         daemon=True).start()

    def announced(self):
        """Wait for the bound-address line; its read time is the announce time."""
        try:
            line, self.announced_ns = self.lines.get(timeout=20)
        except queue.Empty:
            line = ""
        try:
            port = json.loads(line)["port"] if line else None
        except (ValueError, KeyError, TypeError):
            port = None
        if type(port) is not int or self.port not in (0, port):
            self.stop()
            raise RuntimeError("Broker process did not announce readiness: " + self.diagnostic)
        self.port = port
        self.endpoint = "http://127.0.0.1:" + str(port)
        return self

    def stop(self):
        """Interrupt like an operator; return exit, duration and whole-life rusage."""
        if hasattr(self, "stopped"):
            return self.stopped
        started = time.monotonic_ns()
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGINT if os.name == "posix" else signal.SIGTERM)
        usage = None
        if hasattr(os, "wait4") and self.process.returncode is None:
            deadline = time.monotonic() + 10
            while True:
                pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
                if pid:
                    break
                if time.monotonic() > deadline:
                    self.process.kill()
                    pid, status, usage = os.wait4(self.process.pid, 0)
                    break
                time.sleep(.005)
            self.process.returncode = os.waitstatus_to_exitcode(status)
        else:
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        stop_ms = elapsed_ms(started)
        self.process.stdout.close()
        self.stderr.seek(0)
        self.diagnostic = self.stderr.read(2000).decode("utf-8", "replace")
        self.stderr.close()
        lifetime = None
        if usage is not None:
            # ru_maxrss is omitted: it can retain the forking driver's pre-exec size.
            lifetime = {"cpu_ms": (usage.ru_utime + usage.ru_stime) * 1000}
        self.stopped = {"exit_code": self.process.returncode, "stop_ms": stop_ms, "lifetime": lifetime}
        return self.stopped

    def stopped_cleanly(self, stopped):
        require(os.name != "posix" or stopped["exit_code"] == 0,
                "Broker process did not stop cleanly: " + self.diagnostic)


def broker_limits(BrokerServer):
    """The default limits used by ``xordination serve``, read from the product."""
    parameters = inspect.signature(BrokerServer).parameters
    limits = {name: parameters[name].default for name in ("max_requests", "max_waits")
              if name in parameters}
    require(len(limits) == 2 and all(type(v) is int and v > 0 for v in limits.values()),
            "Broker does not expose its default request/wait limits")
    return limits


def wait_capacity(config, clients, BrokerError, bindings, max_waits, body, hold_deadline_s=WAIT_HOLD_S):
    """Hold every documented wait slot, then check refusal, availability and wakeup.

    Clients are built beforehand: client construction is driver cost, not broker work.
    """
    waker, probe = clients["waker"], clients["probe"]
    waiters = [(clients[f"waiter-{n}"], bindings[f"waiter-{n}"]) for n in range(max_waits)]
    overflow = [clients[f"overflow-{n}"] for n in range(config.wait_overflow)]

    def hold(client):
        retries, deadline = 0, time.monotonic() + hold_deadline_s
        while True:
            started = time.monotonic_ns()
            try:
                result = client.call("wait", timeout=WAIT_HOLD_S, unacknowledged=True)
                return result, started, time.monotonic_ns(), retries
            except BrokerError as exc:
                # A waiter can lose a slot to the saturation probe; it retries.
                if exc.code != "backpressure" or time.monotonic() > deadline:
                    raise
                retries += 1
                time.sleep(.01)

    def refused(client):
        started = time.monotonic_ns()
        try:
            client.call("wait", timeout=1)
        except BrokerError as exc:
            require(exc.code == "backpressure", "Wait beyond the limit failed without backpressure")
            return elapsed_ms(started)
        raise AssertionError("A wait beyond the documented limit was accepted")

    started = time.monotonic_ns()
    with ThreadPoolExecutor(max_workers=max_waits + config.wait_overflow) as pool:
        held = [pool.submit(hold, client) for client, _binding in waiters]
        probes, deadline = 0, time.monotonic() + hold_deadline_s
        while True:
            probes += 1
            try:
                probe.call("wait", timeout=0)
            except BrokerError as exc:
                require(exc.code == "backpressure", "Saturation probe failed without backpressure")
                break
            require(time.monotonic() < deadline and not any(f.done() for f in held),
                    "Waiting clients did not saturate the documented wait limit")
            time.sleep(.01)
        saturated_ns = time.monotonic_ns()
        rejections = [future.result() for future in
                      [pool.submit(refused, client) for client in overflow]]
        # Waits must not exhaust the workers needed by other operations.
        available = []
        for _ in range(3):
            call_started = time.monotonic_ns()
            require(not waker.call("inbox")["messages"], "Unexpected waker message")
            available.append(elapsed_ms(call_started))
        sends, send_ms = [], []
        for number, (_client, binding) in enumerate(waiters):
            call_started = time.monotonic_ns()
            sent = waker.call("send", to=binding["participant"], body=body, key=f"wake-{number}")
            send_ms.append(elapsed_ms(call_started))
            sends.append((sent["id"], call_started))
        outcomes = [future.result() for future in held]
    wake_ms = []
    for (client, binding), (message_id, sent_ns), (result, accepted_ns, returned_ns, _) in zip(
            waiters, sends, outcomes, strict=True):
        require(accepted_ns < saturated_ns, "A waiting client was not held at the documented limit")
        require([m["id"] for m in result["messages"]] == [message_id],
                "A held wait did not return its own message")
        check_message(result["messages"][0], binding, bindings["waker"], body)
        wake_ms.append(elapsed_ms(sent_ns, returned_ns))
        acknowledge(Calls(), client, binding, message_id)
    # Released slots accept a new wait again.
    require(not probe.call("wait", timeout=0)["messages"], "Probe received an unexpected message")
    return {"max_waits": max_waits, "waiting_clients": max_waits,
            "overflow_clients": config.wait_overflow, "saturation_ms": elapsed_ms(started, saturated_ns),
            "saturation_probes": probes, "ramp_backpressure_retries": sum(o[3] for o in outcomes),
            "overflow_backpressure": len(rejections), "capacity_recovered": True,
            "samples_ms": {"overflow_rejection": rejections, "saturated_inbox": available,
                           "wake_send": send_ms, "wake": wake_ms}}


def first_success(client, BrokerError, broker):
    """Poll from spawn until the restarted broker answers an authenticated request."""
    attempts, deadline = 0, time.monotonic() + 20
    while True:
        attempts += 1
        try:
            return client.call("inbox", unacknowledged=True), attempts
        except BrokerError as exc:
            if exc.code not in RETRYABLE or time.monotonic() > deadline:
                raise
            if broker.process.poll() is not None:
                broker.stop()
                raise RuntimeError("Restarted broker exited: " + broker.diagnostic) from None
            time.sleep(.005)


def process_trial(config, source_root, product, limits):
    """Idle, loaded and waiting broker resources, then restart with retained state."""
    names = [f"{role}-{n}" for n in range(config.workers) for role in ("sender", "recipient")]
    names += [f"waiter-{n}" for n in range(limits["max_waits"])]
    names += [f"overflow-{n}" for n in range(config.wait_overflow)] + ["waker", "probe"]
    rng = random.Random(config.seed)
    body = "".join(rng.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=config.payload_bytes))
    with tempfile.TemporaryDirectory(prefix="xordination-benchmark-process-") as temporary:
        state = Path(temporary) / "state.sqlite3"
        store = product.Store(str(state))
        try:
            store.create_space("benchmark")
            bindings = {name: store.join("benchmark", name) for name in names}
        finally:
            store.close()
        broker = BrokerProcess(source_root, state)
        try:
            pid = broker.announced().process.pid
            client = {name: product.Client(broker.endpoint, bindings[name]["token"]) for name in names}
            require(client["sender-0"].call("identity")["generation"] == bindings["sender-0"]["generation"],
                    "Broker process did not authenticate its participant")
            idle_start = proc_sample(pid)
            time.sleep(config.idle_ms / 1000)
            idle_end = proc_sample(pid)
            pairs = [((client[f"sender-{n}"], bindings[f"sender-{n}"]),
                      (client[f"recipient-{n}"], bindings[f"recipient-{n}"])) for n in range(config.workers)]
            loaded_started = time.monotonic_ns()
            counts = concurrent_backlog(config, pairs, Calls(), body)
            loaded_ns = time.monotonic_ns() - loaded_started
            loaded_end = proc_sample(pid)
            waits = wait_capacity(config, client, product.BrokerError, bindings, limits["max_waits"], body)
            waiting_end = proc_sample(pid)
            # Leave one pending message and one receipt for the restarted broker.
            sender, recipient = client["sender-0"], client["recipient-0"]
            to = bindings["recipient-0"]["participant"]
            pending = sender.call("send", to=to, body=body, key="retained-pending")
            settled = sender.call("send", to=to, body=body, key="retained-settled")
            receipt = acknowledge(Calls(), recipient, bindings["recipient-0"], settled["id"])
        finally:
            stopped = broker.stop()
        broker.stopped_cleanly(stopped)
        # Restart on the same endpoint so existing clients reconnect unchanged.
        restarted = BrokerProcess(source_root, state, broker.port)
        try:
            page, attempts = first_success(recipient, product.BrokerError, restarted)
            recovered_ns = time.monotonic_ns()
            restarted.announced()
            require([m["id"] for m in page["messages"]] == [pending["id"]],
                    "Pending message did not survive broker restart")
            status = sender.call("status", message_id=settled["id"])
            require(status["acknowledged"] and status["receipts"] == [receipt],
                    "Receipt did not survive broker restart")
        finally:
            final = restarted.stop()
        restarted.stopped_cleanly(final)
    return {"correctness": "verified", "resource_source": "/proc" if idle_start else None,
            "idle": resource_window(idle_start, idle_end),
            "loaded": resource_window(idle_end, loaded_end),
            "waiting": resource_window(loaded_end, waiting_end),
            "lifetime": stopped["lifetime"],
            "loaded_work_units": counts["completed_work_units"],
            "loaded_work_units_per_second": counts["completed_work_units"] * 1e9 / loaded_ns,
            "wait_capacity": waits,
            "restart": {"stop_ms": stopped["stop_ms"],
                        "announce_ms": elapsed_ms(restarted.started_ns, restarted.announced_ns),
                        "first_success_ms": elapsed_ms(restarted.started_ns, recovered_ns),
                        "first_success_attempts": attempts,
                        "retained_pending_messages": 1, "retained_receipts": 1}}


def summarize_process(trials):
    """Report-only resource/capacity/recovery statistics across measured trials."""
    summary = {}
    for window in ("idle", "loaded", "waiting"):
        present = [t[window] for t in trials if t[window] is not None]
        if present:
            summary[window] = {field: stats([w[field] for w in present])
                               for field in ("cpu_percent", "rss_bytes", "peak_rss_bytes")}
    lifetimes = [t["lifetime"] for t in trials if t["lifetime"] is not None]
    if lifetimes:
        summary["lifetime"] = {field: stats([u[field] for u in lifetimes])
                               for field in ("cpu_ms",)}
    summary["loaded_work_units_per_second"] = stats([t["loaded_work_units_per_second"] for t in trials])
    summary["restart_ms"] = {field: stats([t["restart"][field + "_ms"] for t in trials])
                             for field in ("stop", "announce", "first_success")}
    summary["wait_saturation_ms"] = stats([t["wait_capacity"]["saturation_ms"] for t in trials])
    summary["wait_ms"] = {phase: stats([value for t in trials
                                        for value in t["wait_capacity"]["samples_ms"][phase]])
                          for phase in trials[0]["wait_capacity"]["samples_ms"]
                          if any(t["wait_capacity"]["samples_ms"][phase] for t in trials)}
    return summary


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
    runtime_product = runtime(source_root)
    limits = broker_limits(runtime_product.BrokerServer)
    product = fingerprint((source_root / "xordination").rglob("*.py"), source_root)
    manifest = {"config": asdict(config), "product": product,
                "measurement_session_id": session_id,
                "driver": fingerprint([Path(__file__).resolve(), ROOT / "tests" / "helpers.py"], ROOT),
                "environment": {"python": platform.python_version(), "implementation": platform.python_implementation(),
                                "platform": platform.platform(), "machine": platform.machine(),
                                "sqlite": sqlite3.sqlite_version},
                "scope": "in-process broker; authenticated loopback HTTP; fresh state per trial",
                "unmeasured": ["isolated broker execution", "native submission", "native receipt",
                               "model response", "remote network"],
                "percentiles": "nearest rank; pooled operation samples; throughput across trials",
                "broker_process_scope": ("separate xordination serve process; default limits; "
                                         "/proc CPU/RSS where available; fresh state per trial"),
                "broker_limits": limits}
    results = []
    for scenario in SCENARIOS:
        for number in range(config.warmup + config.trials):
            result = trial(config, scenario, runtime_product.Client, runtime_product.owned_broker)
            if number >= config.warmup:
                results.append({"trial": number - config.warmup, **result})
    # Each process trial starts a fresh interpreter; none is discarded as warmup.
    process_results = [{"trial": number, **process_trial(config, source_root, runtime_product, limits)}
                       for number in range(config.process_trials)]
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
            "trials": results, "summary": summarize(results),
            "broker_process": {"warmup_discarded": 0, "trials": process_results,
                               "summary": summarize_process(process_results)}}


def stat_leaves(value, prefix=""):
    """Flatten nested summaries into dotted paths of stats() results."""
    if isinstance(value, dict) and "p50" in value:
        return {prefix: value}
    leaves = {}
    if isinstance(value, dict):
        for key, item in value.items():
            leaves.update(stat_leaves(item, prefix + "." + key if prefix else key))
    return leaves


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
    # Older reports lack process measurements; only shared metrics are reported.
    process = {}
    if (base["manifest"].get("broker_process_scope") == head["manifest"].get("broker_process_scope")
            and base["manifest"].get("broker_limits") == head["manifest"].get("broker_limits")):
        before = stat_leaves(base.get("broker_process", {}).get("summary", {}))
        after = stat_leaves(head.get("broker_process", {}).get("summary", {}))
        for path in sorted(set(before) & set(after)):
            for percentile in ("p50", "p95"):
                old, new = before[path][percentile], after[path][percentile]
                stats([old, new])
                process[path + "." + percentile] = {
                    "base": old, "head": new, "change_percent": (new / old - 1) * 100 if old else None}
    return {"schema_version": 1, "kind": "xordination_benchmark_comparison",
            "performance_gate": "report_only", "base_product": base["manifest"]["product"],
            "head_product": head["manifest"]["product"], "same_runner_required": True,
            "measurement_session_id": base["manifest"]["measurement_session_id"],
            "same_runner_verification": "externally enforced by the orchestrator; no hardware attestation",
            "interpretation": "Observed variation is not evidence of a product improvement or regression.",
            "scenarios": deltas, "broker_process": process,
            "broker_process_comparison": "reported" if process else "unavailable"}


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
    except (AssertionError, ValueError, OSError, KeyError, TypeError, RuntimeError,
            subprocess.SubprocessError) as exc:
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
