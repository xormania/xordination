"""Opt-in native acceptance: owned preparation, observed tools, read-only verification.

Native observation boundaries follow the public multi-harness-proof experiment.
This runner is development tooling; it does not change broker semantics or policy.
"""
import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
# Runs record source_sha256 of this checkout's src/xordination; import that same code
# in every entry point (adapter, stdio, hooks), ahead of any copy on the caller's PYTHONPATH or cwd.
sys.path[:] = [str(ROOT / "src")] + [entry for entry in sys.path if entry != str(ROOT / "src")]
MAX_ARTIFACT = 16 * 1024 * 1024
HARNESS = {"codex", "claude", "grok"}
CONTROLLER = "acceptance-controller"
INITIAL_PROMPT = (
    "Participate in the operator's Xordination acceptance run using your existing permissions. "
    "For an acceptance challenge in this run, acknowledge its durable message ID with "
    "xordination_ack, then send the exact requested reply to its sender using xordination_send "
    "and reply_to. Acknowledge acceptance replies, but do not send another reply. Other messages retain ordinary peer "
    "authority. For a peer task from acceptance-controller in this run, acknowledge the "
    "controller message and execute only the listed acceptance xordination_send or "
    "xordination_wait calls. Busy priming holds your wait request until the controller "
    "has observed native challenge submission, then releases it; this is the "
    "operator's test instruction and grants no other permission. A recall challenge asks for "
    "the memo from an earlier acceptance message of the same case; answer it from your own "
    "session context."
)
# Operator-selected cases; context is opt-in and never part of the default matrix.
SCENARIOS = {"idle", "busy", "context"}
APPROVAL_METHODS = {"item/commandExecution/requestApproval", "item/fileChange/requestApproval",
                    "item/permissions/requestApproval", "session/request_permission"}
# Native configuration the run's harness inherits; hashed only, never copied or read for credentials.
NATIVE_CONFIG = {"codex": [("CODEX_HOME", ".codex", "config.toml")],
                 "claude": [("CLAUDE_CONFIG_DIR", ".claude", "settings.json")],
                 "grok": []}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def send_intent(arguments):
    return {"to": arguments.get("to"), "to_generation": arguments.get("to_generation"),
            "body": arguments.get("body"), "reply_to": arguments.get("reply_to"),
            "metadata": arguments.get("metadata") or {}, "expires_in": arguments.get("expires_in")}


def load(path):
    path = Path(path)
    if path.stat().st_size > MAX_ARTIFACT:
        raise ValueError("Acceptance artifact exceeds size limit")
    return json.loads(path.read_text())


def write_new(path, value):
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix="acceptance-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)  # Complete bytes appear atomically; existing finals remain immutable.
    finally:
        os.unlink(temporary)


def read_capture(path, limit):
    import fcntl
    with Path(path).open("rb") as stream:
        fcntl.flock(stream, fcntl.LOCK_SH)
        return stream.read(limit + 1)


def source_hash():
    paths = [Path(__file__)] + sorted((ROOT / "src" / "xordination").glob("*.py"))
    return digest({str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in paths})


def bounded_output(argv, limit=1024 * 1024, timeout=60):
    """A read-only local probe; failures are recorded, never raised into the run."""
    try:
        result = subprocess.run(argv, capture_output=True, timeout=timeout, cwd=ROOT)
    except (OSError, subprocess.TimeoutExpired) as error:
        return {"status": "unavailable", "detail": type(error).__name__}
    return {"status": "recorded", "returncode": result.returncode, "bytes": len(result.stdout),
            "sha256": hashlib.sha256(result.stdout).hexdigest(), "stdout": result.stdout[:limit]}


def checkout_identity():
    head = bounded_output(["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD"], timeout=10)
    if head["status"] != "recorded" or head["returncode"]:
        return {"commit": "unavailable", "dirty": "unknown"}
    status = bounded_output(["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=normal"], timeout=10)
    dirty = (bool(status["stdout"].strip()) if status["status"] == "recorded" and not status["returncode"]
             else "unknown")
    return {"commit": head["stdout"].decode(errors="replace").strip(), "dirty": dirty}


def harness_manager_status():
    """The image's harness manager records versions, provenance and integrity when present."""
    executable = shutil.which("harnesses")
    if not executable:
        return {"status": "unavailable", "detail": "harnesses command not on PATH"}
    result = bounded_output([executable, "status"])
    if result["status"] == "recorded":
        output = result.pop("stdout")
        try:
            result["output"] = json.loads(output) if result["bytes"] <= 1024 * 1024 else None
        except (ValueError, UnicodeError, RecursionError):
            result["output"] = None
        if result["output"] is None:
            result["text"] = output[:65536].decode(errors="replace")
    return {"executable": executable, **result}


def file_fingerprint(path):
    try:
        return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except FileNotFoundError:
        return "absent"
    except OSError as error:
        return "unreadable:" + type(error).__name__


def harness_fingerprint(participant):
    """Settings and inherited native configuration, by content hash; no credential file is read."""
    files = {}
    for variable, default, name in NATIVE_CONFIG[participant["harness"]]:
        path = (Path(os.environ[variable]) if os.environ.get(variable) else Path.home() / default) / name
        files[str(path)] = file_fingerprint(path)
    value = {"settings": {key: participant.get(key) for key in ("harness", "model", "reasoning")},
             "prompt_sha256": hashlib.sha256(INITIAL_PROMPT.encode()).hexdigest(),
             "native_config_files": files if NATIVE_CONFIG[participant["harness"]] else "unlisted"}
    return {**value, "sha256": digest(value)}


def environment_record(config, probes=None):
    """Run identity for reproduction; each value is an observation or explicitly absent."""
    image = os.environ.get("XORDINATION_IMAGE_DIGEST")
    if image is not None and not re.fullmatch(r"(?:[A-Za-z0-9._/:-]{1,400}@)?sha256:[0-9a-f]{64}", image):
        raise ValueError("XORDINATION_IMAGE_DIGEST must be sha256:<hex> or name@sha256:<hex>")
    return {"source": checkout_identity(),
            "python": {"implementation": platform.python_implementation(), "version": platform.python_version(),
                       "executable": sys.executable},
            "os": {"system": platform.system(), "release": platform.release(), "machine": platform.machine(),
                   "platform": platform.platform()},
            "image_digest": image or "unsupplied",
            "harness_manager": harness_manager_status(),
            "harness_fingerprints": {p["name"]: harness_fingerprint(p) for p in config["participants"]},
            "capability_probes": probes if probes is not None else "unrecorded"}


def case_messages(run_id, run_nonce, case):
    """The exact challenge/reply intent of one prepared case, rebuilt again before every use."""
    case_id = case["case_id"]
    reply = "Xordination acceptance reply " + ":".join((run_nonce, case_id, case["nonce"]))
    challenge = {"acceptance_run": run_id, "run_nonce": run_nonce, "case_id": case_id, "nonce": case["nonce"],
                 "task": "Acknowledge this message, then reply with the exact reply text.", "reply": reply}
    if case["scenario"] == "context":
        challenge.update(memo=case["memo"], retain="Keep this memo in your session context; a later "
                                                   "acceptance message of this case asks for it.")
    metadata = {"acceptance_run": run_id, "case_id": case_id, "run_nonce": run_nonce}
    result = {"body": json.dumps(challenge, sort_keys=True, separators=(",", ":")), "reply": reply,
              "key": "acceptance-" + case_id, "metadata": metadata}
    if case["scenario"] == "context":
        # The recall challenge omits the memo: only the earlier message in this session holds it.
        prefix = "Xordination acceptance recall " + ":".join((run_nonce, case_id, case["recall_nonce"])) + ":"
        recall = {"acceptance_run": run_id, "run_nonce": run_nonce, "case_id": case_id,
                  "nonce": case["recall_nonce"], "phase": "recall", "reply_prefix": prefix,
                  "task": "Acknowledge this message, then reply with reply_prefix followed immediately by "
                          "the memo of your earlier acceptance message in this case."}
        result["recall"] = {"body": json.dumps(recall, sort_keys=True, separators=(",", ":")),
                            "reply": prefix + case["memo"], "key": "acceptance-recall-" + case_id,
                            "metadata": {**metadata, "phase": "recall"}}
    return result


def validate_config(config):
    if set(config) - {"participants", "pairs", "scenarios"}:
        raise ValueError("Unknown acceptance configuration input")
    participants = config.get("participants", [])
    if not 2 <= len(participants) <= 8:
        raise ValueError("Choose two to eight independent native participants")
    names = []
    for participant in participants:
        if set(participant) - {"name", "harness", "version", "model", "reasoning"}:
            raise ValueError("Unknown participant configuration input")
        name = participant.get("name", "")
        if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", name)
                or name == CONTROLLER or name in names or participant.get("harness") not in HARNESS):
            raise ValueError("Participants need unique safe names and a supported native harness")
        for key in ("version", "model", "reasoning"):
            if key in participant and (not isinstance(participant[key], str) or not participant[key]
                                       or len(participant[key]) > 500):
                raise ValueError("Participant observations/settings must be bounded strings")
        names.append(name)
    pairs = config.get("pairs", [[a, b] for a in names for b in names if a != b])
    if (not isinstance(pairs, list) or not pairs or len(pairs) > 56 or
            any(not isinstance(pair, list) or len(pair) != 2 or pair[0] == pair[1] or
                pair[0] not in names or pair[1] not in names for pair in pairs) or
            len({tuple(pair) for pair in pairs}) != len(pairs)):
        raise ValueError("Required directed pairs must be explicit, distinct and known")
    scenarios = config.get("scenarios", ["idle"])
    if (not isinstance(scenarios, list) or not scenarios or
            len(set(scenarios)) != len(scenarios) or set(scenarios) - SCENARIOS):
        raise ValueError("Choose distinct idle/busy/context scenarios")
    return {"participants": participants, "pairs": pairs, "scenarios": scenarios}


def probe_harnesses(config, doctor):
    """The installed doctor result per required harness: a capability observation, no model call."""
    required = {name for pair in config["pairs"] for name in pair}
    probes = {}
    for participant in config["participants"]:
        if participant["name"] in required and participant["harness"] not in probes:
            probes[participant["harness"]] = doctor(participant["harness"])
    return probes


def prepare(config, output, probes=None):
    from xordination.store import Store
    config = validate_config(config)
    environment = environment_record(config, probes)  # Before any run state; a bad digest refuses.
    output = Path(output)
    output.mkdir(mode=0o700)  # Exclusive ownership; never reuse a run directory.
    (output / "native").mkdir(mode=0o700)
    (output / "gates").mkdir(mode=0o700)
    run_id, run_nonce = uuid.uuid4().hex, uuid.uuid4().hex
    draft = {"schema_version": 1, "evidence_mode": "operator_native", "run_id": run_id,
             "run_nonce": run_nonce, "config": config, "config_sha256": digest(config),
             "source_sha256": source_hash(), "created_at": time.time(), "generations": {},
             "space": "acceptance-" + run_id, "cases": [], "environment": environment}
    store = Store(output / "broker.sqlite3")
    try:
        store.create_space(draft["space"])
        controller = store.join(draft["space"], CONTROLLER)
        write_new(output / "controller.connection.json", controller)
        for participant in config["participants"]:
            binding = store.join(draft["space"], participant["name"], participant["harness"])
            draft["generations"][participant["name"]] = binding["generation"]
            write_new(output / (participant["name"] + ".connection.json"), binding)
    finally:
        store.close()
    for scenario in config["scenarios"]:
        for sender, recipient in config["pairs"]:
            case = {"case_id": uuid.uuid4().hex, "scenario": scenario, "sender": sender,
                    "recipient": recipient, "nonce": uuid.uuid4().hex}
            if scenario == "context":
                case.update(memo=uuid.uuid4().hex, recall_nonce=uuid.uuid4().hex)
            draft["cases"].append({**case, **case_messages(run_id, run_nonce, case)})
    write_new(output / "prepared.json", draft)
    return {"run_id": run_id, "output": str(output.resolve()), "required_cases": len(draft["cases"]),
            "next": "Start the broker and operator-selected native participants, then capture bindings."}


@contextmanager
def readonly(path):
    """One WAL-aware snapshot; never instantiate Store or reconcile live delivery."""
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        if connection.execute("PRAGMA user_version").fetchone()[0] != 1:
            raise ValueError("Unsupported acceptance store schema")
        yield connection
    finally:
        connection.close()


def bindings(connection, draft):
    result = {}
    required = {case[role] for case in draft["cases"] for role in ("sender", "recipient")}
    for participant in draft["config"]["participants"]:
        name = participant["name"]
        if name not in required:
            continue
        row = connection.execute("""SELECT b.* FROM bindings b JOIN participants p
            ON b.generation=p.generation WHERE b.space=? AND b.participant=?""",
                                 (draft["space"], name)).fetchone()
        if (not row or not row["active"] or row["generation"] != draft["generations"][name]
                or not row["native_id"] or row["harness"] != participant["harness"]
                or row["native_identity_source"] != "adapter_reported"):
            raise ValueError("Missing current observed native binding for " + name)
        result[name] = {key: row[key] for key in ("generation", "native_id", "harness")}
    identities = [(row["harness"], row["native_id"]) for row in result.values()]
    if len(set(identities)) != len(identities):
        raise ValueError("Participants must have independent native sessions")
    return result


def checked_draft(directory):
    draft = load(directory / "prepared.json")
    if (draft["config_sha256"] != digest(draft["config"]) or
            draft["source_sha256"] != source_hash()):
        raise ValueError("Prepared configuration/source changed; prepare a fresh run")
    config = validate_config(draft["config"])
    if config != draft["config"] or draft.get("evidence_mode") != "operator_native" or draft.get("schema_version") != 1:
        raise ValueError("Prepared run has an invalid configuration/schema")
    if any(not isinstance(draft.get(key), str) or not re.fullmatch(r"[0-9a-f]{32}", draft[key])
           for key in ("run_id", "run_nonce")):
        raise ValueError("Prepared run identity is invalid")
    if (draft.get("space") != "acceptance-" + draft["run_id"] or
            set(draft["generations"]) != {p["name"] for p in config["participants"]}):
        raise ValueError("Prepared run participant/scope matrix changed")
    required = {(scenario, sender, recipient) for scenario in config["scenarios"] for sender, recipient in config["pairs"]}
    seen, identifiers, nonces = set(), set(), set()
    for case in draft["cases"]:
        signature = (case["scenario"], case["sender"], case["recipient"])
        if signature not in required or signature in seen:
            raise ValueError("Prepared run has missing/duplicate required cases")
        fresh = [case["nonce"]] + ([case.get("memo"), case.get("recall_nonce")] if case["scenario"] == "context" else [])
        if (not re.fullmatch(r"[0-9a-f]{32}", case["case_id"]) or case["case_id"] in identifiers or
                any(not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value) or value in nonces
                    for value in fresh) or len(set(fresh)) != len(fresh)):
            raise ValueError("Prepared case IDs/nonces must be unique and fresh")
        expected = case_messages(draft["run_id"], draft["run_nonce"], case)
        if any(case.get(key) != value for key, value in expected.items()) or ("recall" in case) != ("recall" in expected):
            raise ValueError("Prepared case challenge/reply intent changed")
        seen.add(signature)
        identifiers.add(case["case_id"])
        nonces.update(fresh)
    if seen != required:
        raise ValueError("Prepared run has missing/duplicate required cases")
    return draft


def send_task_arguments(frozen, case, intent):
    return {"to": case["recipient"], "to_generation": frozen["bindings"][case["recipient"]]["generation"],
            "body": intent["body"], "key": intent["key"], "metadata": intent["metadata"]}


def capture(directory):
    directory = Path(directory)
    draft = checked_draft(directory)
    with readonly(directory / "broker.sqlite3") as connection:
        frozen = {**draft, "bindings": bindings(connection, draft), "captured_at": time.time()}
        # Fresh cases must not already exist before the frozen native binding snapshot.
        if connection.execute("SELECT COUNT(*) FROM messages WHERE space=?", (draft["space"],)).fetchone()[0]:
            raise ValueError("Run already has messages; capture bindings before exercising cases")
    write_new(directory / "frozen.json", frozen)
    write_new(directory / "frozen.sha256.json", {"sha256": digest(frozen)})
    tasks = {}
    for case in frozen["cases"]:
        for intent in [case] + ([case["recall"]] if "recall" in case else []):
            tasks.setdefault(case["sender"], []).append({"scenario": case["scenario"], "tool": "xordination_send",
                                                        "arguments": send_task_arguments(frozen, case, intent)})
    write_new(directory / "tasks.json", tasks)
    return {"run_id": frozen["run_id"], "required_cases": len(frozen["cases"]),
            "next": "Send the operator's tasks to source native sessions; retain receipt/reply capture."}


@contextmanager
def gate_lock(path):
    import fcntl
    descriptor = os.open(path / "lock", os.O_WRONLY | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def gate_event(arm, **extra):
    return {"run_id": arm["run_id"], "case_id": arm["case_id"], "participant": arm["participant"],
            "generation": arm["generation"], "native_id": arm["native_id"],
            "monotonic_ns": time.monotonic_ns(), **extra}


class Cancelled(Exception):
    """The operator stopped dispatch; this is never recorded as a case result."""


def wait_until(condition, timeout, cancelled=None):
    """Bounded polling waits on explicit state; elapsed time never establishes readiness."""
    deadline = time.monotonic() + timeout
    while True:
        if cancelled is not None and cancelled():
            raise Cancelled("Operator requested stop")
        result = condition()
        if result:
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Controlled busy phase did not reach its required state")
        threading.Event().wait(min(.02, remaining))


def active_gate(directory, name, arguments):
    draft = checked_draft(Path(directory))
    found = []
    for case in draft["cases"]:
        if case["scenario"] != "busy" or case["recipient"] != name:
            continue
        path = Path(directory) / "gates" / case["case_id"]
        if (path / "armed.json").exists() and not (path / "closed.json").exists():
            arm = load(path / "armed.json")
            if arm["wait_arguments"] == arguments:
                found.append((path, arm))
    if len(found) > 1:
        raise ValueError("More than one busy gate matches a native wait request")
    return found[0] if found else None


def hold_wait(directory, name, native_id, call_id, arguments, origin, cancelled=None):
    """Hold an actual native tool request in test code; always release normal tool flow."""
    selected = active_gate(directory, name, arguments)
    if not selected:
        return
    path, arm = selected
    if native_id != arm["native_id"]:
        raise ValueError("Busy hold does not belong to the frozen native session")
    with gate_lock(path):
        write_new(path / "ready.json", gate_event(arm, native_call_id=call_id, origin=origin))
    reason = "released"
    try:
        while True:
            with gate_lock(path):
                if (path / "released.json").exists():
                    break
                if cancelled is not None and cancelled.is_set():
                    reason = "cancelled"
                    break
                if time.monotonic_ns() >= arm["deadline_monotonic_ns"]:
                    reason = "timed_out"
                    break
            threading.Event().wait(.02)
    finally:
        with gate_lock(path):
            write_new(path / "closed.json", gate_event(arm, reason=reason))


def release_gate(path, arm, reason):
    with gate_lock(path):
        if not (path / "released.json").exists():
            write_new(path / "released.json", gate_event(arm, reason=reason))


def arm_busy_gate(directory, frozen, case, endpoint, phase_timeout):
    from xordination.client import Client
    name = case["recipient"]
    binding = frozen["bindings"][name]
    descriptor = load(directory / (name + ".connection.json"))
    recipient = Client(endpoint, descriptor["token"])
    wait_arguments = {"cursor": recipient.call("inbox")["cursor"], "timeout": 30}
    path = directory / "gates" / case["case_id"]
    path.mkdir(mode=0o700)
    arm = {"run_id": frozen["run_id"], "case_id": case["case_id"], "participant": name,
           **binding, "wait_arguments": wait_arguments, "phase_timeout": phase_timeout,
           "armed_monotonic_ns": time.monotonic_ns(),
           "deadline_monotonic_ns": time.monotonic_ns() + int(2 * phase_timeout * 1e9)}
    write_new(path / "armed.json", arm)
    return path, arm


def busy_case(directory, frozen, case, client, send_task, phase_timeout, cancelled=None):
    path, arm = arm_busy_gate(directory, frozen, case, client.endpoint, phase_timeout)
    name = case["recipient"]
    binding = frozen["bindings"][name]
    wait_arguments = arm["wait_arguments"]
    reason = "failed"
    stage = "readiness"
    try:
        client.call("send", to=name, to_generation=binding["generation"],
            key="acceptance-prime-" + case["case_id"],
            body=json.dumps({"acceptance_run": frozen["run_id"], "run_nonce": frozen["run_nonce"],
                             "phase": "busy_prime", "case_id": case["case_id"],
                             "instruction": "Acknowledge this controller message, then execute the exact wait task.",
                             "tasks": [{"tool": "xordination_wait", "arguments": wait_arguments}]}, sort_keys=True),
            metadata={"acceptance_run": frozen["run_id"], "phase": "busy_prime", "case_id": case["case_id"]})
        def ready():
            if not (path / "ready.json").exists() or (path / "closed.json").exists():
                return None
            events, _, errors = native_events(directory, frozen)
            if any(error.endswith(":" + name) for error in errors):
                return None
            capture = load(path / "ready.json")
            matches = [event for event in events if event["participant"] == name and
                       event["native_id"] == binding["native_id"] and event["tool"] == "xordination_wait" and
                       event["arguments"] == wait_arguments and event["monotonic_ns"] >= arm["armed_monotonic_ns"] and
                       (capture["native_call_id"] is None or capture["native_call_id"] == event["call_id"])]
            if len({event["call_id"] for event in matches}) > 1:
                raise ValueError("Ambiguous native readiness for one held wait request")
            return matches[0] if matches else None
        observation = wait_until(ready, phase_timeout, cancelled)
        write_new(path / "native-ready.json", gate_event(arm, native_call_id=observation["call_id"],
                                                        native_sequence=observation["sequence"]))
        stage = "submission"
        send_task(case)  # The source receives its challenge only after held native readiness.
        def submitted():
            with gate_lock(path):
                if (path / "closed.json").exists() or (path / "released.json").exists():
                    raise TimeoutError("Native busy hold ended before challenge submission")
                with readonly(directory / "broker.sqlite3") as connection:
                    row = connection.execute("""SELECT m.id AS message_id,a.id AS attempt_id FROM messages m
                        JOIN attempts a ON a.message_id=m.id WHERE m.space=? AND m.sender=? AND m.idempotency_key=?
                        AND m.sender_generation=? AND m.recipient=? AND m.body=? AND m.metadata=?
                        AND m.original_generation=? AND m.delivery_generation=? AND a.generation=?
                        AND a.native_id=? AND a.state='submitted'""", (frozen["space"], case["sender"], case["key"],
                        frozen["bindings"][case["sender"]]["generation"], name, case["body"],
                        encoded(case["metadata"]).decode(), binding["generation"], binding["generation"],
                        binding["generation"], binding["native_id"])).fetchone()
                if not row:
                    return None
                value = gate_event(arm, message_id=row["message_id"], attempt_id=row["attempt_id"],
                                   native_call_id=observation["call_id"])
                write_new(path / "submitted.json", value)
                return value
        result = wait_until(submitted, phase_timeout, cancelled)
        reason = "submitted"
        return result
    except Cancelled:
        write_new(path / "failed.json", gate_event(arm, stage=stage, reason="operator_stopped"))
        raise  # The finally block still releases the held native request.
    except Exception as error:
        failure = gate_event(arm, stage=stage,
                             reason="native_" + stage + "_timeout" if isinstance(error, TimeoutError) else "busy_phase_failed")
        write_new(path / "failed.json", failure)
        return {**failure, "status": "fail"}
    finally:
        release_gate(path, arm, reason)
        if (path / "ready.json").exists():
            try:
                wait_until(lambda: (path / "closed.json").exists(), phase_timeout)
            except TimeoutError:
                if not (path / "failed.json").exists():
                    write_new(path / "failed.json", gate_event(arm, stage="release", reason="native_release_timeout"))
                raise


def send_tasks(directory, phase_timeout=30, cancelled=None, idle_timeout=None):
    """Trigger source-native work; these controller messages are never pair evidence."""
    from xordination.client import Client
    directory = Path(directory)
    frozen = load(directory / "frozen.json")
    if digest(frozen) != load(directory / "frozen.sha256.json")["sha256"]:
        raise ValueError("Frozen manifest was altered")
    checked_draft(directory)
    if not isinstance(phase_timeout, (int, float)) or not math.isfinite(phase_timeout) or not 0 < phase_timeout <= 300:
        raise ValueError("Busy phase timeout must be finite, positive and at most 300 seconds")
    if idle_timeout is not None and (isinstance(idle_timeout, bool) or not isinstance(idle_timeout, (int, float))
                                     or not math.isfinite(idle_timeout) or idle_timeout <= 0):
        raise ValueError("Idle timeout must be finite and positive")
    connection = load(directory / "controller.connection.json")
    client = Client(os.environ.get("XORDINATION_ENDPOINT", "http://127.0.0.1:8765"), connection["token"])
    messages = []
    def dispatch(scenarios, key, recall=False):
        # Reconstruct required tasks from the frozen manifest rather than trusting edited tasks.json.
        tasks = {}
        for case in frozen["cases"]:
            if case["scenario"] in scenarios:
                tasks.setdefault(case["sender"], []).append({"scenario": case["scenario"], "tool": "xordination_send",
                    "arguments": send_task_arguments(frozen, case, case["recall"] if recall else case)})
        for name, work in tasks.items():
            message = client.call("send", to=name, to_generation=frozen["bindings"][name]["generation"],
                key=key + "-" + frozen["run_id"] + "-" + name,
                body=json.dumps({"acceptance_run": frozen["run_id"], "run_nonce": frozen["run_nonce"],
                                 "instruction": "Execute these exact acceptance tasks in your native session.",
                                 "tasks": work}, sort_keys=True),
                metadata={"acceptance_run": frozen["run_id"], "phase": "operator_tasks"})
            messages.append({"participant": name, "message_id": message["id"]})
    def settle(stage, scenarios, complete):
        # Earlier round trips (model turns and native approvals included) must finish before
        # dependent cases start; that is a whole-matrix budget, not one held phase.
        case_ids = sorted(case["case_id"] for case in frozen["cases"] if case["scenario"] in scenarios)
        try:
            wait_until(lambda: all(complete(case) for case in verify(directory, retain=False)["cases"]
                                   if case["case_id"] in case_ids), idle_timeout or phase_timeout, cancelled)
        except TimeoutError:
            failure = {"run_id": frozen["run_id"], "stage": stage, "reason": stage.replace("-", "_") + "_completion_timeout",
                       "case_ids": case_ids, "monotonic_ns": time.monotonic_ns()}
            write_new(directory / (stage + "-failed.json"), failure)
            return failure
        write_new(directory / (stage + "-complete.json"), {"run_id": frozen["run_id"], "case_ids": case_ids,
                                                          "monotonic_ns": time.monotonic_ns()})
    present = {case["scenario"] for case in frozen["cases"]}
    dispatch({"idle", "context"}, "acceptance-tasks")
    failure = None
    if "context" in present:
        # The recall challenge goes out only after the earlier exchange completed in the same sessions.
        failure = settle("context-earlier", {"context"}, earlier_complete)
        if not failure:
            dispatch({"context"}, "acceptance-recall-tasks", recall=True)
    if not failure and "busy" in present and present & {"idle", "context"}:
        failure = settle("idle", {"idle", "context"}, lambda case: case["status"] == "pass")
    if failure:
        return {"run_id": frozen["run_id"], "status": "fail", "tasks_dispatched": messages, "failure": failure,
                "busy_submissions": [], "next": "Required earlier exchanges did not finish; dependent cases remain unexercised."}
    def send_task(case):
        name = case["sender"]
        message = client.call("send", to=name, to_generation=frozen["bindings"][name]["generation"],
            key="acceptance-task-" + case["case_id"],
            body=json.dumps({"acceptance_run": frozen["run_id"], "run_nonce": frozen["run_nonce"],
                             "phase": "challenge", "case_id": case["case_id"],
                             "tasks": [{"scenario": "busy", "tool": "xordination_send", "arguments": {
                                 "to": case["recipient"], "to_generation": frozen["bindings"][case["recipient"]]["generation"],
                                 "body": case["body"], "key": case["key"], "metadata": case["metadata"]}}]}, sort_keys=True),
            metadata={"acceptance_run": frozen["run_id"], "phase": "operator_tasks", "case_id": case["case_id"]})
        messages.append({"participant": name, "message_id": message["id"]})
    busy_results = []
    for case in frozen["cases"]:
        if case["scenario"] == "busy":
            if cancelled is not None and cancelled():
                raise Cancelled("Operator requested stop")
            try:
                busy_results.append(busy_case(directory, frozen, case, client, send_task, phase_timeout, cancelled))
            except (OSError, ValueError, TimeoutError) as error:
                busy_results.append({"case_id": case["case_id"], "status": "fail", "reason": type(error).__name__})
    return {"run_id": frozen["run_id"], "tasks_dispatched": messages,
            "busy_submissions": busy_results,
            "status": "fail" if any(result.get("status") == "fail" for result in busy_results) else "pass",
            "next": "Allow real native sessions to acknowledge/reply, then verify; dispatch is not a pass."}


def tool_name(name):
    if not isinstance(name, str):
        return None
    for prefix in ("mcp__xordination__", "xordination__", ""):
        candidate = name.removeprefix(prefix)
        if candidate in {"xordination_send", "xordination_ack", "xordination_wait"}:
            return candidate
    return None


def grok_observation(params):
    update = params.get("update", {})
    if update.get("sessionUpdate") != "tool_call":
        return None
    meta = update.get("_meta", {})
    if not isinstance(meta, dict):
        raise ValueError("Malformed Grok native tool metadata")
    wire = update.get("title")
    if "x.ai/tool" in meta:
        identity = meta["x.ai/tool"]
        if (not isinstance(identity, dict) or type(identity.get("version")) is not int
                or identity["version"] != 1):
            raise ValueError("Unsupported Grok native tool identity version")
        wire = identity.get("name")
        namespace = identity.get("namespace")
        if not ((tool_name(wire) and wire == "xordination__" + tool_name(wire) and namespace == "mcp") or
                (wire == "use_tool" and namespace == "grok_build")):
            return None  # Catalog discovery and unrelated tools do not establish handling.
        title = update.get("title")
        if (tool_name(title) or title == "use_tool") and title != wire and tool_name(title) != tool_name(wire):
            raise ValueError("Conflicting Grok wire/display tool identities")
    arguments = update.get("rawInput")  # Never use display text or canonical UI projections.
    if wire == "use_tool":
        if not isinstance(arguments, dict) or set(arguments) != {"tool_name", "tool_input"}:
            raise ValueError("Grok dispatch omitted the actual target/input")
        target = arguments["tool_name"]
        if not tool_name(target) or target != "xordination__" + tool_name(target):
            return None
        wire, arguments = target, arguments["tool_input"]
    name = tool_name(wire)
    if not name:
        return None
    return (params.get("sessionId"), update.get("toolCallId"), name, arguments)


def earlier_complete(case):
    """Every phase of a case except its later recall exchange."""
    return all(value for phase, value in case["phases"].items()
               if not phase.startswith("recall_") and phase != "context_order")


def append_capture(path, event):
    # A single owned adapter writes Codex/Grok; separate Claude hooks need an OS lock.
    import fcntl
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "ab") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        event["sequence"] = os.fstat(stream.fileno()).st_size
        stream.write(encoded(event) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def bounded_label(value):
    return value if isinstance(value, str) and value and len(value) <= 500 else None


def record_approval(directory, participant, native_id, kind, origin, request_id=None, outcome=None):
    """A native approval wait is evidence: its request identity and timing, never its content."""
    directory = Path(directory)
    draft = checked_draft(directory)
    if participant not in {p["name"] for p in draft["config"]["participants"]} or kind not in {"wait", "resolved"}:
        raise ValueError("Approval observation omitted participant/kind")
    event = {"run_id": draft["run_id"], "run_nonce": draft["run_nonce"], "participant": participant,
             "native_id": bounded_label(native_id), "kind": kind, "origin": origin,
             "request_id": bounded_label(request_id), "outcome": bounded_label(outcome),
             "time": time.time(), "monotonic_ns": time.monotonic_ns()}
    append_capture(directory / "native" / (participant + ".approvals.jsonl"), event)


def approval_outcome(result):
    if not isinstance(result, dict):
        return "unknown"
    if isinstance(result.get("decision"), str):
        return result["decision"]
    if "permissions" in result:
        return "granted" if result["permissions"] else "declined"
    outcome = result.get("outcome")
    if isinstance(outcome, dict) and isinstance(outcome.get("outcome"), str):
        return outcome["outcome"]
    return "unknown"


def record_session(directory, participant, native_id, source, model, reasoning):
    """Effective settings only as the native side reported them; absent values stay unobserved."""
    directory = Path(directory)
    draft = checked_draft(directory)
    if participant not in {p["name"] for p in draft["config"]["participants"]} or not bounded_label(native_id):
        raise ValueError("Native session observation omitted participant/session")
    path = directory / "native" / (participant + ".session.json")
    if path.exists():
        return  # One owned adapter starts one native session; the first report is retained.
    write_new(path, {"run_id": draft["run_id"], "run_nonce": draft["run_nonce"], "participant": participant,
                     "native_id": native_id, "source": source, "time": time.time(),
                     "observed_model": bounded_label(model) or "unobserved",
                     "observed_reasoning": bounded_label(reasoning) or "unobserved"})


def session_settings(result, harness):
    """(native_id, source, model, reasoning) exposed by the native session-start response."""
    if not isinstance(result, dict):
        return None, None, None, None
    if harness == "codex":
        thread = result.get("thread")
        return (thread.get("id") if isinstance(thread, dict) else None, "codex/thread/start.response",
                result.get("model"), result.get("reasoningEffort"))
    models = result.get("models")
    return (result.get("sessionId"), "grok/session/new.response",
            models.get("currentModelId") if isinstance(models, dict) else None, None)


def record_native(directory, participant, native_id, call_id, tool, arguments, origin, turn_id=None):
    """Record only this run's communication inputs, not native transcripts/credentials."""
    directory = Path(directory)
    draft = checked_draft(directory)
    known = {p["name"]: p for p in draft["config"]["participants"]}
    if (participant not in known or not isinstance(native_id, str) or not native_id or
            not isinstance(call_id, str) or not call_id or not isinstance(arguments, dict)):
        raise ValueError("Native observation omitted participant/session/call/input")
    if tool == "xordination_send":
        intents = draft["cases"] + [case["recall"] for case in draft["cases"] if "recall" in case]
        if arguments.get("body") not in {intent["reply"] for intent in intents} | {intent["body"] for intent in intents}:
            return  # Do not retain unrelated project content.
    elif tool == "xordination_wait":
        if not active_gate(directory, participant, arguments):
            return  # Observe only the run's explicitly armed wait, not normal project waits.
    elif tool != "xordination_ack":
        return
    allowed = ({"to", "to_generation", "body", "key", "reply_to", "metadata", "expires_in"}
               if tool == "xordination_send" else {"cursor", "timeout"} if tool == "xordination_wait" else {"message_id"})
    if set(arguments) - allowed or len(encoded(arguments)) > 70000:
        raise ValueError("Native communication observation has unexpected/oversized inputs")
    event = {"run_id": draft["run_id"], "run_nonce": draft["run_nonce"],
             "config_sha256": draft["config_sha256"], "observer_sha256": draft["source_sha256"],
             "participant": participant, "native_id": native_id, "call_id": call_id,
             "tool": tool, "arguments": arguments, "origin": origin,
             "time": time.time(), "monotonic_ns": time.monotonic_ns()}
    if bounded_label(turn_id):
        event["turn_id"] = turn_id  # Native turn identity, when the wire exposes it.
    append_capture(directory / "native" / (participant + ".jsonl"), event)


def native_events(directory, frozen):
    events, artifacts, errors = [], [], []
    origins = {"codex": "codex/item/tool/call", "grok": "grok/session/update", "claude": "claude/PreToolUse"}
    required = {case[role] for case in frozen["cases"] for role in ("sender", "recipient")}
    for name, binding in frozen["bindings"].items():
        if name not in required:
            continue  # Configured availability is distinct from expressly requested coverage.
        path = directory / "native" / (name + ".jsonl")
        if not path.exists():
            errors.append("native_capture_missing:" + name)
            continue
        if path.stat().st_size > MAX_ARTIFACT:
            errors.append("native_capture_limit:" + name)
            continue
        try:
            raw = read_capture(path, MAX_ARTIFACT)
        except OSError:
            errors.append("native_capture_invalid:" + name)
            continue
        if len(raw) > MAX_ARTIFACT:
            errors.append("native_capture_limit:" + name)
            continue
        artifacts.append({"path": "native/" + name + ".jsonl", "sha256": hashlib.sha256(raw).hexdigest(),
                          "bytes": len(raw)})
        offset = 0
        calls = {}
        participant_events = []
        participant_error = False
        for line in raw.splitlines(keepends=True):
            try:
                event = json.loads(line)
            except (ValueError, UnicodeError, RecursionError):
                errors.append("native_capture_invalid:" + name)
                participant_error = True
                continue
            if not isinstance(event, dict):
                errors.append("native_capture_invalid:" + name)
                participant_error = True
                continue
            if (not isinstance(event.get("call_id"), str) or not event["call_id"] or
                    not isinstance(event.get("native_id"), str) or not event["native_id"] or
                    not isinstance(event.get("tool"), str) or
                    event.get("tool") not in {"xordination_ack", "xordination_send", "xordination_wait"}):
                errors.append("native_capture_invalid:" + name)
                participant_error = True
                continue
            try:
                encoded(event)  # Reject NaN/Infinity and invalid/deep capture structures.
            except (ValueError, TypeError, RecursionError):
                errors.append("native_capture_invalid:" + name)
                participant_error = True
                continue
            if (event.get("participant") != name or event.get("native_id") != binding["native_id"] or
                    event.get("run_id") != frozen["run_id"] or event.get("run_nonce") != frozen["run_nonce"] or
                    event.get("config_sha256") != frozen["config_sha256"] or
                    event.get("observer_sha256") != frozen["source_sha256"] or
                    event.get("origin") != origins[binding["harness"]] or event.get("sequence") != offset or
                    not event.get("call_id") or not isinstance(event.get("arguments"), dict)):
                errors.append("native_capture_identity:" + name)
                participant_error = True
            stamp, monotonic = event.get("time"), event.get("monotonic_ns")
            if (type(stamp) not in (int, float) or not frozen["captured_at"] <= stamp <= time.time()
                    or type(monotonic) is not int or monotonic <= 0):
                errors.append("native_capture_time:" + name)
                participant_error = True
            signature = digest({key: event.get(key) for key in ("native_id", "tool", "arguments", "origin")})
            if event.get("call_id") in calls and calls[event["call_id"]] != signature:
                errors.append("native_call_id_conflict:" + name)
                participant_error = True
            calls[event.get("call_id")] = signature
            participant_events.append(event)
            offset += len(line)
        if not participant_error:
            events.extend(participant_events)
    diagnostic = directory / "observation-errors.jsonl"
    if diagnostic.exists():
        try:
            raw = read_capture(diagnostic, 32768)
            if len(raw) > 32768:
                raise ValueError("Observer diagnostic size limit")
            artifacts.append({"path": diagnostic.name, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
            compromised = set()
            for line in raw.splitlines():
                entry = json.loads(line)
                name = entry["participant"]
                if name in required or name == "unknown":
                    compromised.add(name)
                    errors.append("native_observer_error:" + name)
            events = [event for event in events if event["participant"] not in compromised]
        except (OSError, ValueError, TypeError, KeyError):
            errors.append("native_observer_diagnostics_invalid")
            events = []
    return events, artifacts, errors


def controlled_busy(directory, frozen, case, message, attempts, events):
    """True when evidence establishes held submission, False when it contradicts or failed, None when absent."""
    path = directory / "gates" / case["case_id"]
    if (path / "failed.json").exists():
        return False
    try:
        arm, ready, native_ready, submitted, released, closed = [load(path / (name + ".json")) for name in
                                                   ("armed", "ready", "native-ready", "submitted", "released", "closed")]
    except FileNotFoundError:
        return None  # Missing timing evidence remains unknown; it still cannot pass.
    except (OSError, ValueError):
        return False
    try:
        binding = frozen["bindings"][case["recipient"]]
        expected = {"run_id": frozen["run_id"], "case_id": case["case_id"], "participant": case["recipient"],
                    "generation": binding["generation"], "native_id": binding["native_id"]}
        if any(any(value.get(key) != item for key, item in expected.items()) for value in
               (arm, ready, native_ready, submitted, released, closed)):
            return False
        if (ready["origin"] != {"codex": "codex/tool-handler", "grok": "grok/mcp-wait", "claude": "claude/native-hook"}[binding["harness"]]
                or released["reason"] != "submitted" or closed["reason"] != "released"):
            return False
        if not (arm["armed_monotonic_ns"] <= ready["monotonic_ns"] <= native_ready["monotonic_ns"] <=
                submitted["monotonic_ns"] <= released["monotonic_ns"] <= closed["monotonic_ns"]
                and submitted["monotonic_ns"] < arm["deadline_monotonic_ns"]):
            return False
        if (submitted["message_id"] != message["id"] or not any(attempt["id"] == submitted["attempt_id"] and
                attempt["state"] == "submitted" and attempt["generation"] == binding["generation"] and
                attempt["native_id"] == binding["native_id"] for attempt in attempts)):
            return False
        if ready["native_call_id"] is not None and ready["native_call_id"] != submitted["native_call_id"]:
            return False
        if native_ready["native_call_id"] != submitted["native_call_id"]:
            return False
        # One actual invocation cannot qualify two gates, even if an artifact is replayed.
        for other in frozen["cases"]:
            other_path = directory / "gates" / other["case_id"] / "submitted.json"
            if other["case_id"] != case["case_id"] and other["recipient"] == case["recipient"] and other_path.exists():
                if load(other_path)["native_call_id"] == submitted["native_call_id"]:
                    return False
        return any(event["participant"] == case["recipient"] and event["native_id"] == binding["native_id"] and
                   event["call_id"] == submitted["native_call_id"] and event["tool"] == "xordination_wait" and
                   event["sequence"] == native_ready["native_sequence"] and event["monotonic_ns"] >= arm["armed_monotonic_ns"] and
                   event["monotonic_ns"] <= native_ready["monotonic_ns"] and
                   event["arguments"] == arm["wait_arguments"] for event in events)
    except (OSError, ValueError, TypeError, KeyError):
        return False


def busy_timing(directory, case, events, held, ack):
    """Submission during held work, later handling and in-turn handling are separate claims.

    Timing that was not observed is "unknown", never a negative result; the pass verdict
    still requires the controlled_busy phase.
    """
    path = directory / "gates" / case["case_id"]
    def artifact(name):
        try:
            value = load(path / (name + ".json"))
        except (OSError, ValueError):
            return None
        return value if isinstance(value, dict) else None
    submitted, released, native_ready = artifact("submitted"), artifact("released"), artifact("native-ready")
    wait = next((event for event in events if native_ready and event["participant"] == case["recipient"] and
                 event["tool"] == "xordination_wait" and event["call_id"] == native_ready.get("native_call_id")), None)
    held_until = released.get("monotonic_ns") if held is True and released else None
    return {"submission_during_work": {True: "observed", False: "failed", None: "unknown"}[held],
            "submitted_monotonic_ns": submitted.get("monotonic_ns", "unknown") if submitted else "unknown",
            "handling": "observed" if ack else "unknown",
            "handled_monotonic_ns": ack["monotonic_ns"] if ack else "unknown",
            "handled_while_held": ack["monotonic_ns"] < held_until if ack and held_until else "unknown",
            # Same native turn as the held work, only when the wire exposed both turn identities.
            "in_turn": ("yes" if wait["turn_id"] == ack["turn_id"] else "no")
                       if ack and wait and wait.get("turn_id") and ack.get("turn_id") else "unknown"}


def capture_lines(directory, name, suffix, artifacts, errors, kind):
    path = directory / "native" / (name + suffix)
    if not path.exists():
        return None
    try:
        raw = read_capture(path, MAX_ARTIFACT)
    except OSError:
        errors.append(kind + "_invalid:" + name)
        return None
    if len(raw) > MAX_ARTIFACT:
        errors.append(kind + "_limit:" + name)
        return None
    artifacts.append({"path": "native/" + name + suffix, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)})
    return raw


def required_bindings(frozen):
    required = {case[role] for case in frozen["cases"] for role in ("sender", "recipient")}
    return {name: binding for name, binding in frozen["bindings"].items() if name in required}


def approval_events(directory, frozen):
    """Approval waits observed in the native sessions; their absence is not evidence of none."""
    events, artifacts, errors = [], [], []
    for name, binding in required_bindings(frozen).items():
        raw = capture_lines(directory, name, ".approvals.jsonl", artifacts, errors, "approval_capture")
        offset = 0
        for line in (raw or b"").splitlines(keepends=True):
            try:
                event = json.loads(line)
            except (ValueError, UnicodeError, RecursionError):
                event = None
            if (not isinstance(event, dict) or event.get("participant") != name or
                    event.get("run_id") != frozen["run_id"] or event.get("run_nonce") != frozen["run_nonce"] or
                    event.get("native_id") not in (None, binding["native_id"]) or
                    event.get("kind") not in {"wait", "resolved"} or event.get("sequence") != offset):
                errors.append("approval_capture_invalid:" + name)
                break
            events.append({key: event.get(key) for key in ("participant", "native_id", "kind", "origin",
                                                            "request_id", "outcome", "time", "monotonic_ns")})
            offset += len(line)
    return events, artifacts, errors


def observed_settings(directory, frozen):
    """Requested settings beside what the native session reported; never inferred from a request."""
    settings, artifacts, errors = {}, [], []
    participants = {p["name"]: p for p in frozen["config"]["participants"]}
    for name, binding in required_bindings(frozen).items():
        entry = {"requested_model": participants[name].get("model") or "native_default",
                 "requested_reasoning": participants[name].get("reasoning") or "native_default",
                 "observed_model": "unobserved", "observed_reasoning": "unobserved", "source": None}
        raw = capture_lines(directory, name, ".session.json", artifacts, errors, "native_session")
        if raw is not None:
            try:
                observation = json.loads(raw)
                if (observation.get("participant") != name or observation.get("run_id") != frozen["run_id"] or
                        observation.get("run_nonce") != frozen["run_nonce"] or
                        observation.get("native_id") != binding["native_id"]):
                    errors.append("native_session_identity:" + name)
                else:
                    entry.update({key: bounded_label(observation.get(key)) or "unobserved"
                                  for key in ("observed_model", "observed_reasoning")},
                                 source=bounded_label(observation.get("source")))
            except (ValueError, UnicodeError, RecursionError, AttributeError):
                errors.append("native_session_invalid:" + name)
        settings[name] = entry
    return settings, artifacts, errors


def verify(directory, retain=True):
    directory = Path(directory)
    frozen = load(directory / "frozen.json")
    if digest(frozen) != load(directory / "frozen.sha256.json")["sha256"]:
        raise ValueError("Frozen manifest was altered")
    draft = checked_draft(directory)
    if any(frozen.get(key) != draft.get(key) for key in draft):
        raise ValueError("Frozen manifest differs from prepared configuration")
    events, artifacts, errors = native_events(directory, frozen)
    approvals, approval_artifacts, approval_errors = approval_events(directory, frozen)
    settings, setting_artifacts, setting_errors = observed_settings(directory, frozen)
    artifacts, errors = artifacts + approval_artifacts + setting_artifacts, errors + approval_errors + setting_errors
    cases = []
    def matching(name, tool, arguments):
        return [event for event in events if event["participant"] == name and event["tool"] == tool and
                event["arguments"] == arguments]
    def exchange(connection, case, intent):
        """Both legs of one challenge; no message unless it was accepted exactly once."""
        sender, recipient = case["sender"], case["recipient"]
        source, target = frozen["bindings"][sender], frozen["bindings"][recipient]
        rows = connection.execute("SELECT * FROM messages WHERE space=? AND sender=? AND idempotency_key=?",
                                  (frozen["space"], sender, intent["key"])).fetchall()
        phases = {key: False for key in ("accepted", "native_submitted", "broker_receipt", "reply",
                                         "native_send", "native_ack", "native_reply", "reply_native_submitted",
                                         "reply_broker_receipt", "reply_native_ack")}
        marks = {"ack": None, "reply": None}  # The recipient's first correlated native handling events.
        if len(rows) != 1:
            return phases, None, [], marks
        message = rows[0]
        phases["accepted"] = (message["sender_generation"] == source["generation"] and
            message["recipient"] == recipient and message["original_generation"] == target["generation"] and
            message["delivery_generation"] == target["generation"] and message["body"] == intent["body"] and
            json.loads(message["metadata"]) == intent["metadata"] and message["reply_to"] is None)
        send_args = {"to": recipient, "to_generation": target["generation"], "body": intent["body"],
                     "key": intent["key"], "metadata": intent["metadata"]}
        phases["native_send"] = bool(matching(sender, "xordination_send", send_args))
        attempts = connection.execute("SELECT * FROM attempts WHERE message_id=?", (message["id"],)).fetchall()
        phases["native_submitted"] = any(attempt["generation"] == target["generation"] and
            attempt["native_id"] == target["native_id"] and attempt["state"] == "submitted"
            for attempt in attempts)
        phases["broker_receipt"] = connection.execute("""SELECT 1 FROM receipts WHERE message_id=?
            AND generation=? AND observer=? AND native_id=? AND kind='client_ack'""",
            (message["id"], target["generation"], recipient, target["native_id"])).fetchone() is not None
        acks = matching(recipient, "xordination_ack", {"message_id": message["id"]})
        phases["native_ack"] = bool(acks)
        marks["ack"] = min(acks, key=lambda event: event["sequence"], default=None)
        replies = connection.execute("SELECT * FROM messages WHERE reply_to=?", (message["id"],)).fetchall()
        for reply in replies:
            if (reply["sender"] == recipient and reply["sender_generation"] == target["generation"] and
                    reply["recipient"] == sender and reply["original_generation"] == source["generation"] and
                    reply["delivery_generation"] == source["generation"] and reply["body"] == intent["reply"]):
                phases["reply"] = True
                sent = [event for event in events if event["participant"] == recipient and
                        event["tool"] == "xordination_send" and
                        send_intent(event["arguments"]) == json.loads(reply["intent"]) and
                        event["arguments"].get("key") == reply["idempotency_key"]]
                phases["native_reply"] = bool(sent)
                marks["reply"] = min(sent, key=lambda event: event["sequence"], default=None)
                phases["reply_native_submitted"] = connection.execute("""SELECT 1 FROM attempts WHERE
                    message_id=? AND generation=? AND native_id=? AND state='submitted'""",
                    (reply["id"], source["generation"], source["native_id"])).fetchone() is not None
                phases["reply_broker_receipt"] = connection.execute("""SELECT 1 FROM receipts WHERE
                    message_id=? AND generation=? AND observer=? AND native_id=? AND kind='client_ack'""",
                    (reply["id"], source["generation"], sender, source["native_id"])).fetchone() is not None
                phases["reply_native_ack"] = bool(matching(sender, "xordination_ack", {"message_id": reply["id"]}))
        return phases, message, attempts, marks
    with readonly(directory / "broker.sqlite3") as connection:
        try:
            if bindings(connection, draft) != frozen["bindings"]:
                errors.append("native_binding_changed")
        except ValueError:
            errors.append("native_binding_changed")
        for case in frozen["cases"]:
            phases, message, attempts, marks = exchange(connection, case, case)
            result = {"case_id": case["case_id"], "sender": case["sender"], "recipient": case["recipient"],
                      "scenario": case["scenario"]}
            if case["scenario"] == "busy":
                # A recorded gate failure is explicit even when it stopped the challenge being sent.
                held = (controlled_busy(directory, frozen, case, message, attempts, events) if message is not None
                        else False if (directory / "gates" / case["case_id"] / "failed.json").exists() else None)
                if message is not None:
                    phases["controlled_busy"] = held is True
                result["busy"] = busy_timing(directory, case, events, held, marks["ack"] if phases["broker_receipt"] else None)
            recall = None
            if case["scenario"] == "context":
                recall_phases, recall, _, recall_marks = exchange(connection, case, case["recall"])
                phases.update(("recall_" + key, value) for key, value in recall_phases.items())
                # The earlier exchange was handled, and answered, in this session before the recall.
                phases["context_order"] = bool(
                    marks["ack"] and marks["reply"] and recall_marks["ack"] and
                    marks["ack"]["sequence"] < recall_marks["ack"]["sequence"] and
                    marks["reply"]["sequence"] < recall_marks["ack"]["sequence"])
            if message is None:
                failures = ["required_case_unexercised"]
            else:
                failures = ["missing_" + phase for phase, value in phases.items() if not value and
                            (recall is not None or not phase.startswith("recall_") and phase != "context_order")]
                if case["scenario"] == "context" and recall is None:
                    failures.append("recall_unexercised")
            cases.append({**result, "status": "fail" if failures else "pass", "phases": phases, "failures": failures})
    report = {"schema_version": 1, "run_id": frozen["run_id"], "evidence_mode": "operator_native",
              "config_sha256": frozen["config_sha256"], "source_sha256": frozen["source_sha256"],
              "status": "pass" if cases and not errors and all(case["status"] == "pass" for case in cases) else "fail",
              "required_cases": len(cases), "passed_cases": sum(case["status"] == "pass" for case in cases),
              "cases": cases, "errors": sorted(set(errors)), "native_artifacts": artifacts,
              "configured_participants": frozen["config"]["participants"],
              "environment": frozen.get("environment", "unrecorded"), "native_settings": settings,
              "approval_waits": approvals,
              "evidence_boundary": "Run-local observer capture, not authenticated against hostile local file editing. "
                  "Busy means exact native submission observed while a run-local native wait request was held; "
                  "handling and in-turn handling are reported separately and remain unknown without evidence. "
                  "No token-generation interruption or simultaneous model computation claim. "
                  "Retained context means a recall reply carried a memo sent only in an earlier message to the "
                  "same native session; broker tools could re-read it, so it is not a private-memory proof. "
                  "Effective model/reasoning are native session-start reports, otherwise unobserved. "
                  "Configured versions/settings are operator observations, not independently certified."}
    # Each verification is retained separately; failed evidence cannot be overwritten by a rerun.
    if retain:
        write_new(directory / ("report-" + uuid.uuid4().hex + ".json"), report)
    return report


def observation_error(directory, participant, error):
    """Capture failure is diagnostic, never a native permission decision."""
    try:
        print("Xordination acceptance observation failed; normal native tool flow continues.", file=sys.stderr)
    except Exception:
        pass
    try:
        # Fixed owned filename and bounded fields; never export arbitrary exception text.
        if not isinstance(participant, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,63}", participant):
            participant = "unknown"
        descriptor = os.open(Path(directory) / "observation-errors.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with os.fdopen(descriptor, "ab") as stream:
            import fcntl
            fcntl.flock(stream, fcntl.LOCK_EX)
            if os.fstat(stream.fileno()).st_size >= 32700:
                return
            stream.write(encoded({"participant": participant, "error_type": type(error).__name__, "time": time.time()}) + b"\n")
            stream.flush()
    except Exception:
        pass  # Disk/capture failure must not deny a vendor tool or rewrite native policy.


def passive_observe(directory, participant, action):
    try:
        return action()
    except Exception as error:
        observation_error(directory, participant, error)
        return None


def observed_adapter(directory, name, client, binary, project):
    """Add test-only observation/holds while preserving the inherited native adapter."""
    from xordination.adapters import CodexAdapter, GrokAdapter
    directory = Path(directory)
    draft = checked_draft(directory)
    participant = next((row for row in draft["config"]["participants"] if row["name"] == name), None)
    if not participant or participant["harness"] == "claude":
        raise ValueError("Choose a configured Codex/Grok participant; Claude uses its native PreToolUse hook")
    def recorder(native, call, tool, args, origin, turn=None):
        return record_native(directory, name, native, call, tool, args, origin, turn)
    async def approval(method, params, request, prefix):
        """Record the wait and its end around the inherited operator prompt; never answer it."""
        tool = params.get("toolCall")
        call = params.get("itemId") or (tool.get("toolCallId") if isinstance(tool, dict) else None)
        native = params.get("threadId", params.get("sessionId"))
        passive_observe(directory, name, lambda: record_approval(directory, name, native, "wait",
                                                                 prefix + method, call))
        outcome = "error"
        try:
            result = await request(method, params)
            outcome = approval_outcome(result)
            return result
        finally:
            passive_observe(directory, name, lambda: record_approval(directory, name, native, "resolved",
                                                                     prefix + method, call, outcome))
    def observe_session(rpc, method):
        original = rpc.request
        async def request(called, params, **options):
            result = await original(called, params, **options)
            if called == method:
                passive_observe(directory, name, lambda: record_session(
                    directory, name, *session_settings(result, participant["harness"])))
            return result
        rpc.request = request  # This owned RPC only; concurrent adapters keep their own settings.
    class ObservedCodex(CodexAdapter):
        async def spawn(self, *args, **kwargs):
            await super().spawn(*args, **kwargs)
            observe_session(self.rpc, "thread/start")
        async def request(self, method, params):
            if method in APPROVAL_METHODS:
                return await approval(method, params, super().request, "codex/")
            if method == "item/tool/call" and tool_name(params.get("tool")):
                arguments = params.get("arguments", {})
                if isinstance(arguments, str):
                    arguments = json.loads(arguments)
                passive_observe(directory, name, lambda: recorder(params.get("threadId"), params.get("callId"),
                                tool_name(params["tool"]), arguments, "codex/item/tool/call", params.get("turnId")))
                if tool_name(params["tool"]) == "xordination_wait":
                    cancelled = threading.Event()
                    try:
                        await asyncio.to_thread(passive_observe, directory, name, lambda: hold_wait(
                            directory, name, params.get("threadId"), params.get("callId"), arguments,
                            "codex/tool-handler", cancelled))
                    finally:
                        cancelled.set()
            return await super().request(method, params)
    class ObservedGrok(GrokAdapter):
        async def notification(self, method, params):
            if method == "session/update":
                def observe():
                    observation = grok_observation(params)
                    if observation:
                        recorder(*observation, "grok/session/update")
                passive_observe(directory, name, observe)
            return await super().notification(method, params)
        async def request(self, method, params):
            if method in APPROVAL_METHODS:
                return await approval(method, params, super().request, "grok/")
            return await super().request(method, params)
        async def spawn(self, *args, **kwargs):
            await super().spawn(*args, **kwargs)
            observe_session(self.rpc, "session/new")
            original_request = self.rpc.request
            async def request(method, params, **options):
                if method == "session/new":
                    params = dict(params)
                    params["mcpServers"] = [{**server, "args": [str(Path(__file__).resolve()), "stdio",
                        "--run", str(directory.resolve()), "--participant", name, "--endpoint", client.endpoint]}
                        if server.get("name") == "xordination" else server for server in params.get("mcpServers", [])]
                return await original_request(method, params, **options)
            self.rpc.request = request  # This owned RPC only; concurrent adapters keep their own settings.
    harness = participant["harness"]
    return (ObservedCodex if harness == "codex" else ObservedGrok)(
        client, binary, project,
        model=participant.get("model"), reasoning=participant.get("reasoning"), prompt=INITIAL_PROMPT)


def launch_adapter(directory, name, project, binary=None):
    """Explicit operator-only launch; never invoked by ordinary CI."""
    from xordination.adapters import _binary
    from xordination.client import Client
    directory = Path(directory)
    participant = next(row for row in checked_draft(directory)["config"]["participants"] if row["name"] == name)
    connection = load(directory / (name + ".connection.json"))
    endpoint = os.environ.get("XORDINATION_ENDPOINT", "http://127.0.0.1:8765")
    adapter = observed_adapter(directory, name, Client(endpoint, connection["token"]),
                               _binary(participant["harness"], binary), project)
    asyncio.run(adapter.run())


def run_test_stdio(directory, name, endpoint, token_env):
    from unittest.mock import patch
    from xordination import mcp
    class HeldServer(mcp._Server):
        def _wait(self, arguments, cancelled):
            def hold():
                identity = self.client.call("identity")
                hold_wait(directory, name, identity["native_id"], None, arguments, "grok/mcp-wait", cancelled)
            passive_observe(directory, name, hold)
            return super()._wait(arguments, cancelled)
    with patch("xordination.mcp._Server", HeldServer):
        return mcp.run_stdio(endpoint, os.environ[token_env])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--config", required=True)
    p.add_argument("--output", required=True)
    for command in ("capture", "send", "verify", "adapter", "observe-claude", "stdio"):
        p = sub.add_parser(command)
        p.add_argument("--run", required=True)
        if command in {"adapter", "observe-claude", "stdio"}:
            p.add_argument("--participant", required=True)
        if command == "adapter":
            p.add_argument("--project", required=True)
            p.add_argument("--binary")
        if command == "send":
            p.add_argument("--phase-timeout", type=float, default=30)
            p.add_argument("--idle-timeout", type=float,
                           help="Budget for all idle round trips before busy priming (default: --phase-timeout)")
        if command == "stdio":
            p.add_argument("--endpoint", required=True)
            p.add_argument("--token-env", default="XORDINATION_TOKEN")
    args = parser.parse_args(argv)
    if args.command == "observe-claude":
        def observe():
            packet = json.load(sys.stdin)
            event = packet.get("hook_event_name")
            if event == "PermissionRequest" or (event == "Notification" and
                                                packet.get("notification_type") == "permission_prompt"):
                # Claude shows the prompt in its own terminal; record that the session waits.
                record_approval(args.run, args.participant, packet.get("session_id"), "wait",
                                "claude/" + event, packet.get("tool_use_id"))
            if event == "PreToolUse" and tool_name(packet.get("tool_name")):
                tool, arguments = tool_name(packet["tool_name"]), packet.get("tool_input")
                record_native(args.run, args.participant, packet.get("session_id"), packet.get("tool_use_id"),
                              tool, arguments, "claude/PreToolUse")
                if tool == "xordination_wait":
                    hold_wait(args.run, args.participant, packet.get("session_id"), packet.get("tool_use_id"),
                              arguments, "claude/native-hook")
        passive_observe(args.run, args.participant, observe)
        return 0  # Empty output/zero exit preserves Claude's original permission/tool flow.
    try:
        if args.command == "prepare":
            from xordination.adapters import doctor
            config = validate_config(load(args.config))
            result = prepare(config, args.output, probe_harnesses(config, doctor))
        elif args.command == "capture":
            result = capture(args.run)
        elif args.command == "verify":
            result = verify(args.run)
        elif args.command == "send":
            result = send_tasks(args.run, args.phase_timeout, idle_timeout=args.idle_timeout)
        elif args.command == "adapter":
            launch_adapter(args.run, args.participant, args.project, args.binary)
            return 0
        else:
            return run_test_stdio(args.run, args.participant, args.endpoint, args.token_env)
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0 if result.get("status", "pass") == "pass" else 1
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        print("Acceptance failed: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
