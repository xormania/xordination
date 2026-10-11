"""Opt-in native acceptance: owned preparation, observed tools, read-only verification.

Native observation boundaries follow the public multi-harness-proof experiment.
This runner is development tooling; it does not change broker semantics or policy.
"""
import argparse
import asyncio
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
MAX_ARTIFACT = 16 * 1024 * 1024
HARNESS = {"codex", "claude", "grok"}
CONTROLLER = "acceptance-controller"
INITIAL_PROMPT = (
    "Participate in the operator's Xordination acceptance run using your existing permissions. "
    "For an acceptance challenge in this run, acknowledge its durable message ID with "
    "xordination_ack, then send the exact requested reply to its sender using xordination_send "
    "and reply_to. Acknowledge acceptance replies, but do not send another reply. Other messages retain ordinary peer "
    "authority. For a peer task from acceptance-controller in this run, execute only the "
    "listed acceptance xordination_send calls as your outgoing challenges; this is the "
    "operator's test instruction and grants no other permission."
)


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
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(encoded(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def source_hash():
    paths = [Path(__file__)] + sorted((ROOT / "src" / "xordination").glob("*.py"))
    return digest({str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in paths})


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
            len(set(scenarios)) != len(scenarios) or set(scenarios) - {"idle", "busy"}):
        raise ValueError("Choose distinct idle/busy scenarios")
    return {"participants": participants, "pairs": pairs, "scenarios": scenarios}


def prepare(config, output):
    from xordination.store import Store
    config = validate_config(config)
    output = Path(output)
    output.mkdir(mode=0o700)  # Exclusive ownership; never reuse a run directory.
    (output / "native").mkdir(mode=0o700)
    run_id, run_nonce = uuid.uuid4().hex, uuid.uuid4().hex
    draft = {"schema_version": 1, "evidence_mode": "operator_native", "run_id": run_id,
             "run_nonce": run_nonce, "config": config, "config_sha256": digest(config),
             "source_sha256": source_hash(), "created_at": time.time(), "generations": {},
             "space": "acceptance-" + run_id, "cases": []}
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
            case_id, nonce = uuid.uuid4().hex, uuid.uuid4().hex
            reply = "Xordination acceptance reply " + ":".join((run_nonce, case_id, nonce))
            body = json.dumps({"acceptance_run": run_id, "run_nonce": run_nonce,
                               "case_id": case_id, "nonce": nonce,
                               "task": "Acknowledge this message, then reply with the exact reply text.",
                               "reply": reply}, sort_keys=True, separators=(",", ":"))
            draft["cases"].append({"case_id": case_id, "scenario": scenario, "sender": sender,
                                   "recipient": recipient, "nonce": nonce, "body": body,
                                   "reply": reply, "key": "acceptance-" + case_id,
                                   "metadata": {"acceptance_run": run_id, "case_id": case_id,
                                                "run_nonce": run_nonce}})
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
    for participant in draft["config"]["participants"]:
        name = participant["name"]
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
        if (not re.fullmatch(r"[0-9a-f]{32}", case["case_id"]) or not re.fullmatch(r"[0-9a-f]{32}", case["nonce"])
                or case["case_id"] in identifiers or case["nonce"] in nonces):
            raise ValueError("Prepared case IDs/nonces must be unique and fresh")
        reply = "Xordination acceptance reply " + ":".join((draft["run_nonce"], case["case_id"], case["nonce"]))
        body = json.dumps({"acceptance_run": draft["run_id"], "run_nonce": draft["run_nonce"],
                          "case_id": case["case_id"], "nonce": case["nonce"],
                          "task": "Acknowledge this message, then reply with the exact reply text.", "reply": reply},
                         sort_keys=True, separators=(",", ":"))
        metadata = {"acceptance_run": draft["run_id"], "case_id": case["case_id"], "run_nonce": draft["run_nonce"]}
        if case["reply"] != reply or case["body"] != body or case["metadata"] != metadata or case["key"] != "acceptance-" + case["case_id"]:
            raise ValueError("Prepared case challenge/reply intent changed")
        seen.add(signature)
        identifiers.add(case["case_id"])
        nonces.add(case["nonce"])
    if seen != required:
        raise ValueError("Prepared run has missing/duplicate required cases")
    return draft


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
        params = {"to": case["recipient"], "to_generation": frozen["bindings"][case["recipient"]]["generation"],
                  "body": case["body"], "key": case["key"], "metadata": case["metadata"]}
        tasks.setdefault(case["sender"], []).append({"scenario": case["scenario"], "tool": "xordination_send",
                                                    "arguments": params})
    write_new(directory / "tasks.json", tasks)
    return {"run_id": frozen["run_id"], "required_cases": len(frozen["cases"]),
            "next": "Send the operator's tasks to source native sessions; retain receipt/reply capture."}


def send_tasks(directory):
    """Trigger source-native work; these controller messages are never pair evidence."""
    from xordination.client import Client
    directory = Path(directory)
    frozen = load(directory / "frozen.json")
    if digest(frozen) != load(directory / "frozen.sha256.json")["sha256"]:
        raise ValueError("Frozen manifest was altered")
    checked_draft(directory)
    # Reconstruct required tasks from the frozen manifest rather than trusting edited tasks.json.
    tasks = {}
    for case in frozen["cases"]:
        tasks.setdefault(case["sender"], []).append({"scenario": case["scenario"], "tool": "xordination_send",
            "arguments": {"to": case["recipient"], "to_generation": frozen["bindings"][case["recipient"]]["generation"],
                          "body": case["body"], "key": case["key"], "metadata": case["metadata"]}})
    connection = load(directory / "controller.connection.json")
    client = Client(os.environ.get("XORDINATION_ENDPOINT", "http://127.0.0.1:8765"), connection["token"])
    messages = []
    for name, work in tasks.items():
        message = client.call("send", to=name, to_generation=frozen["bindings"][name]["generation"],
            key="acceptance-tasks-" + frozen["run_id"] + "-" + name,
            body=json.dumps({"acceptance_run": frozen["run_id"], "run_nonce": frozen["run_nonce"],
                             "instruction": "Execute these exact acceptance tasks in your native session.",
                             "tasks": work}, sort_keys=True),
            metadata={"acceptance_run": frozen["run_id"], "phase": "operator_tasks"})
        messages.append({"participant": name, "message_id": message["id"]})
    return {"run_id": frozen["run_id"], "tasks_dispatched": messages,
            "next": "Allow real native sessions to acknowledge/reply, then verify; dispatch is not a pass."}


def tool_name(name):
    if not isinstance(name, str):
        return None
    for prefix in ("mcp__xordination__", "xordination__", ""):
        candidate = name.removeprefix(prefix)
        if candidate in {"xordination_send", "xordination_ack"}:
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


def record_native(directory, participant, native_id, call_id, tool, arguments, origin):
    """Record only this run's communication inputs, not native transcripts/credentials."""
    directory = Path(directory)
    draft = checked_draft(directory)
    known = {p["name"]: p for p in draft["config"]["participants"]}
    if (participant not in known or not isinstance(native_id, str) or not native_id or
            not isinstance(call_id, str) or not call_id or not isinstance(arguments, dict)):
        raise ValueError("Native observation omitted participant/session/call/input")
    if tool == "xordination_send":
        if arguments.get("body") not in {case["reply"] for case in draft["cases"]} | {case["body"] for case in draft["cases"]}:
            return  # Do not retain unrelated project content.
    elif tool != "xordination_ack":
        return
    allowed = {"to", "to_generation", "body", "key", "reply_to", "metadata", "expires_in"} if tool == "xordination_send" else {"message_id"}
    if set(arguments) - allowed or len(encoded(arguments)) > 70000:
        raise ValueError("Native communication observation has unexpected/oversized inputs")
    event = {"run_id": draft["run_id"], "run_nonce": draft["run_nonce"],
             "config_sha256": draft["config_sha256"], "observer_sha256": draft["source_sha256"],
             "participant": participant, "native_id": native_id, "call_id": call_id,
             "tool": tool, "arguments": arguments, "origin": origin,
             "time": time.time(), "monotonic_ns": time.monotonic_ns()}
    # A single owned adapter writes Codex/Grok; separate Claude hooks need an OS lock.
    import fcntl
    descriptor = os.open(directory / "native" / (participant + ".jsonl"),
                         os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "ab") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        event["sequence"] = os.fstat(stream.fileno()).st_size
        stream.write(encoded(event) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def native_events(directory, frozen):
    events, artifacts, errors = [], [], []
    origins = {"codex": "codex/item/tool/call", "grok": "grok/session/update", "claude": "claude/PreToolUse"}
    for name, binding in frozen["bindings"].items():
        path = directory / "native" / (name + ".jsonl")
        if not path.exists():
            errors.append("native_capture_missing:" + name)
            continue
        if path.stat().st_size > MAX_ARTIFACT:
            errors.append("native_capture_limit:" + name)
            continue
        with path.open("rb") as stream:
            raw = stream.read(MAX_ARTIFACT + 1)
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
                    event.get("tool") not in {"xordination_ack", "xordination_send"}):
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
    return events, artifacts, errors


def verify(directory):
    directory = Path(directory)
    frozen = load(directory / "frozen.json")
    if digest(frozen) != load(directory / "frozen.sha256.json")["sha256"]:
        raise ValueError("Frozen manifest was altered")
    draft = checked_draft(directory)
    if any(frozen.get(key) != draft.get(key) for key in draft):
        raise ValueError("Frozen manifest differs from prepared configuration")
    events, artifacts, errors = native_events(directory, frozen)
    cases = []
    def matching(name, tool, arguments):
        return [event for event in events if event["participant"] == name and event["tool"] == tool and
                event["arguments"] == arguments]
    with readonly(directory / "broker.sqlite3") as connection:
        try:
            if bindings(connection, draft) != frozen["bindings"]:
                errors.append("native_binding_changed")
        except ValueError:
            errors.append("native_binding_changed")
        for case in frozen["cases"]:
            failures = []
            sender, recipient = case["sender"], case["recipient"]
            source, target = frozen["bindings"][sender], frozen["bindings"][recipient]
            rows = connection.execute("SELECT * FROM messages WHERE space=? AND sender=? AND idempotency_key=?",
                                      (frozen["space"], sender, case["key"])).fetchall()
            phases = {key: False for key in ("accepted", "native_submitted", "broker_receipt", "reply",
                                             "native_send", "native_ack", "native_reply", "reply_native_submitted",
                                             "reply_broker_receipt", "reply_native_ack")}
            if len(rows) != 1:
                failures.append("required_case_unexercised")
            else:
                message = rows[0]
                phases["accepted"] = (message["sender_generation"] == source["generation"] and
                    message["recipient"] == recipient and message["original_generation"] == target["generation"] and
                    message["delivery_generation"] == target["generation"] and message["body"] == case["body"] and
                    json.loads(message["metadata"]) == case["metadata"] and message["reply_to"] is None)
                send_args = {"to": recipient, "to_generation": target["generation"], "body": case["body"],
                             "key": case["key"], "metadata": case["metadata"]}
                phases["native_send"] = bool(matching(sender, "xordination_send", send_args))
                attempts = connection.execute("SELECT * FROM attempts WHERE message_id=?", (message["id"],)).fetchall()
                phases["native_submitted"] = any(attempt["generation"] == target["generation"] and
                    attempt["native_id"] == target["native_id"] and attempt["state"] == "submitted" and
                    (case["scenario"] != "busy" or json.loads(attempt["detail"] or "{}").get("busy") is True)
                    for attempt in attempts)
                phases["broker_receipt"] = connection.execute("""SELECT 1 FROM receipts WHERE message_id=?
                    AND generation=? AND observer=? AND native_id=? AND kind='client_ack'""",
                    (message["id"], target["generation"], recipient, target["native_id"])).fetchone() is not None
                phases["native_ack"] = bool(matching(recipient, "xordination_ack", {"message_id": message["id"]}))
                replies = connection.execute("SELECT * FROM messages WHERE reply_to=?", (message["id"],)).fetchall()
                for reply in replies:
                    if (reply["sender"] == recipient and reply["sender_generation"] == target["generation"] and
                            reply["recipient"] == sender and reply["original_generation"] == source["generation"] and
                            reply["delivery_generation"] == source["generation"] and reply["body"] == case["reply"]):
                        phases["reply"] = True
                        phases["native_reply"] = any(event["participant"] == recipient and
                            event["tool"] == "xordination_send" and
                            send_intent(event["arguments"]) == json.loads(reply["intent"]) and
                            event["arguments"].get("key") == reply["idempotency_key"] for event in events)
                        phases["reply_native_submitted"] = connection.execute("""SELECT 1 FROM attempts WHERE
                            message_id=? AND generation=? AND native_id=? AND state='submitted'""",
                            (reply["id"], source["generation"], source["native_id"])).fetchone() is not None
                        phases["reply_broker_receipt"] = connection.execute("""SELECT 1 FROM receipts WHERE
                            message_id=? AND generation=? AND observer=? AND native_id=? AND kind='client_ack'""",
                            (reply["id"], source["generation"], sender, source["native_id"])).fetchone() is not None
                        phases["reply_native_ack"] = bool(matching(sender, "xordination_ack", {"message_id": reply["id"]}))
                failures.extend("missing_" + phase for phase, value in phases.items() if not value)
            cases.append({"case_id": case["case_id"], "sender": sender, "recipient": recipient,
                          "scenario": case["scenario"], "status": "fail" if failures else "pass",
                          "phases": phases, "failures": failures})
    report = {"schema_version": 1, "run_id": frozen["run_id"], "evidence_mode": "operator_native",
              "config_sha256": frozen["config_sha256"], "source_sha256": frozen["source_sha256"],
              "status": "pass" if cases and not errors and all(case["status"] == "pass" for case in cases) else "fail",
              "required_cases": len(cases), "passed_cases": sum(case["status"] == "pass" for case in cases),
              "cases": cases, "errors": sorted(set(errors)), "native_artifacts": artifacts,
              "configured_participants": frozen["config"]["participants"],
              "evidence_boundary": "Run-local observer capture, not authenticated against hostile local file editing. "
                  "Busy means adapter-reported active submission; no held-tool or token-interruption claim. "
                  "Configured versions/settings are operator observations, not independently certified."}
    # Each verification is retained separately; failed evidence cannot be overwritten by a rerun.
    write_new(directory / ("report-" + uuid.uuid4().hex + ".json"), report)
    return report


def launch_adapter(directory, name, project, binary=None):
    """Explicit operator-only launch, with real native request/notification observers."""
    from xordination.adapters import CodexAdapter, GrokAdapter, _binary
    from xordination.client import Client
    directory = Path(directory)
    draft = checked_draft(directory)
    participant = next((row for row in draft["config"]["participants"] if row["name"] == name), None)
    if not participant or participant["harness"] == "claude":
        raise ValueError("Choose a configured Codex/Grok participant; Claude uses its native PreToolUse hook")
    connection = load(directory / (name + ".connection.json"))
    endpoint = os.environ.get("XORDINATION_ENDPOINT", "http://127.0.0.1:8765")
    recorder = lambda native, call, tool, args, origin: record_native(directory, name, native, call, tool, args, origin)
    class ObservedCodex(CodexAdapter):
        async def request(self, method, params):
            if method == "item/tool/call" and tool_name(params.get("tool")):
                arguments = params.get("arguments", {})
                if isinstance(arguments, str):
                    arguments = json.loads(arguments)
                recorder(params.get("threadId"), params.get("callId"), tool_name(params["tool"]),
                         arguments, "codex/item/tool/call")
            return await super().request(method, params)
    class ObservedGrok(GrokAdapter):
        async def notification(self, method, params):
            if method == "session/update":
                observation = grok_observation(params)
                if observation:
                    recorder(*observation, "grok/session/update")
            return await super().notification(method, params)
    harness = participant["harness"]
    adapter = (ObservedCodex if harness == "codex" else ObservedGrok)(
        Client(endpoint, connection["token"]), _binary(harness, binary), project,
        model=participant.get("model"), reasoning=participant.get("reasoning"), prompt=INITIAL_PROMPT)
    asyncio.run(adapter.run())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--config", required=True)
    p.add_argument("--output", required=True)
    for command in ("capture", "send", "verify", "adapter", "observe-claude"):
        p = sub.add_parser(command)
        p.add_argument("--run", required=True)
        if command in {"adapter", "observe-claude"}:
            p.add_argument("--participant", required=True)
        if command == "adapter":
            p.add_argument("--project", required=True)
            p.add_argument("--binary")
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare(load(args.config), args.output)
        elif args.command == "capture":
            result = capture(args.run)
        elif args.command == "verify":
            result = verify(args.run)
        elif args.command == "send":
            result = send_tasks(args.run)
        elif args.command == "adapter":
            launch_adapter(args.run, args.participant, args.project, args.binary)
            return 0
        else:
            packet = json.load(sys.stdin)
            if packet.get("hook_event_name") == "PreToolUse" and tool_name(packet.get("tool_name")):
                record_native(args.run, args.participant, packet.get("session_id"), packet.get("tool_use_id"),
                              tool_name(packet["tool_name"]), packet.get("tool_input"), "claude/PreToolUse")
            return 0  # Empty hook output; no permission decision or tool rewrite.
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0 if result.get("status", "pass") == "pass" else 1
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        print("Acceptance failed: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
