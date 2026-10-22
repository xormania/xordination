"""Whole communication journeys and a separate, identity-bound evidence oracle.

These are deterministic protocol fixtures, never certification of live model
behavior. Each case owns fresh mutable state and preserves its partial evidence.
"""

import argparse
import asyncio
from dataclasses import asdict, dataclass
import hashlib
import itertools
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import uuid

from helpers import MCPProcess, isolated_process_env, owned_broker
from native_fixtures import MOCK_CODEX
from xordination.adapters import CodexAdapter
from xordination.client import BrokerError, Client
from xordination.rpc import RPC


@dataclass(frozen=True)
class Scenario:
    name: str
    kind: str
    sender: str
    recipient: str


@dataclass(frozen=True)
class Binding:
    participant: str
    generation: str
    harness: str
    native_id: str = None


@dataclass(frozen=True)
class RunManifest:
    run_nonce: str
    source_sha256: str
    participants: tuple
    scenarios: tuple
    evidence_mode: str = "protocol_fixture"


@dataclass(frozen=True)
class CaseManifest:
    run_nonce: str
    source_sha256: str
    scenario: Scenario
    bindings: tuple
    evidence_mode: str = "protocol_fixture"

    @property
    def sha256(self):
        return digest(asdict(self))


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def json_value(value):
    """Use one representation before and after a report is serialized."""
    return json.loads(json.dumps(value, allow_nan=False))


def source_digest():
    root = Path(__file__).resolve().parents[1]
    paths = sorted((root / "src" / "xordination").glob("*.py"))
    paths += [Path(__file__).resolve(), root / "tests" / "native_fixtures.py", root / "tests" / "helpers.py"]
    return digest([(str(path.relative_to(root)), hashlib.sha256(path.read_bytes()).hexdigest())
                   for path in paths])


def core_scenarios(participants=("alpha", "beta", "gamma")):
    """Participant labels are configurable; harness names are not peer IDs."""
    if (len(participants) < 2 or len(set(participants)) != len(participants)
            or any(not isinstance(name, str) or not name for name in participants)):
        raise ValueError("At least two distinct participant names are required")
    pairs = [Scenario(f"exchange:{a}->{b}", "exchange", a, b)
             for a, b in itertools.permutations(participants, 2)]
    a, b = participants[:2]
    return tuple(pairs + [Scenario(kind, kind, a, b) for kind in
                         ("duplicate", "replacement", "restart_retry", "busy_native")])


def _public_bindings(bindings):
    return tuple(Binding(b["participant"], b["generation"], b["harness"], b["native_id"])
                 for b in bindings.values())


def _error(call, operation, **arguments):
    try:
        call(operation, **arguments)
    except BrokerError as error:
        return error.code
    return "not_rejected"


def _send(events, manifest, sender):
    scenario = manifest.scenario
    # Fresh challenges also bind replies to this run/case rather than a transcript.
    body = f"{manifest.run_nonce}/{scenario.name}: reply with this exact challenge"
    events["accepted"] = sender("send", to=scenario.recipient, body=body, key="challenge")
    return events["accepted"]


def _complete_exchange(events, manifest, sender, recipient):
    scenario = manifest.scenario
    sent = events["accepted"]
    events["inbox_before_ack"] = recipient("inbox")
    events["pre_receipt_status"] = sender("status", message_id=sent["id"])
    events["receipt"] = recipient("ack", message_id=sent["id"])["receipt"]
    events["receipt_replay"] = recipient("ack", message_id=sent["id"])["receipt"]
    events["reply"] = recipient("send", to=scenario.sender, body="reply:" + sent["body"],
                                key="reply", reply_to=sent["id"])
    events["sender_inbox"] = sender("inbox")
    events["status"] = sender("status", message_id=sent["id"])


async def _busy_native(events, manifest, sender, receiver, directory):
    """Real adapter/RPC/native process; only the held fixture workload is scripted."""
    entered, release = threading.Event(), threading.Event()

    class HeldClient:
        def call(self, operation, **arguments):
            if operation == "wait":
                entered.set()
                if not release.wait(10):
                    raise TimeoutError("Owned native fixture hold was not released")
                return {"messages": []}
            return receiver.call(operation, **arguments)

    adapter = CodexAdapter(HeldClient(), sys.executable, directory)
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-u", "-c", MOCK_CODEX, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            env=isolated_process_env(directory))
        adapter.rpc = RPC(process, adapter.request, adapter.notification)
        adapter.session = "thread-one"
        await adapter.start_prompt("Continue unfinished fixture work")
        if not await asyncio.to_thread(entered.wait, 10):
            raise TimeoutError("Native fixture never entered the held tool")
        events["hold_entered_ns"] = time.monotonic_ns()
        _send(events, manifest, sender.call)
        claimed = events["claim"] = receiver.call("claim")
        message = {**claimed["message"], "attempt_id": claimed["attempt_id"]}
        adapter.pending_attempts[message["attempt_id"]] = message
        await asyncio.wait_for(adapter.deliver(message), 5)
        events["submitted_while_held"] = not release.is_set()
        events["submitted_ns"] = time.monotonic_ns()
        events["native_status"] = sender.call("status", message_id=message["id"])
        release.set()
        events["hold_released_ns"] = time.monotonic_ns()
        _complete_exchange(events, manifest, sender.call, receiver.call)
    finally:
        release.set()
        await adapter.close()


def run_suite(*, participants=("alpha", "beta", "gamma"), scenarios=None):
    """Return the frozen expected manifests separately from untrusted evidence."""
    participants = tuple(participants)
    scenarios = tuple(scenarios if scenarios is not None else core_scenarios(participants))
    if not scenarios or len({s.name for s in scenarios}) != len(scenarios):
        raise ValueError("A nonempty, uniquely named scenario set is required")
    if any(s.sender not in participants or s.recipient not in participants or s.sender == s.recipient
           or s.kind not in {"exchange", "duplicate", "replacement", "restart_retry", "busy_native"}
           for s in scenarios):
        raise ValueError("Scenario participant or kind is invalid")
    plan = RunManifest(uuid.uuid4().hex, source_digest(), participants, scenarios)
    expected, results = [], []
    with tempfile.TemporaryDirectory() as directory:
        for index, scenario in enumerate(scenarios):
            state = Path(directory) / str(index) / "broker.sqlite3"
            events, failure = {}, None
            manifest, bindings = None, {}
            try:
                with owned_broker(state) as (store, _, endpoint):
                    store.create_space("behavior")
                    bindings = {name: store.join("behavior", name,
                        harness="codex" if scenario.kind == "busy_native" and name == scenario.recipient else "external")
                        for name in participants}
                    if scenario.kind == "busy_native":
                        store.bind(store.authenticate(bindings[scenario.recipient]["token"]), "thread-one")
                        bindings[scenario.recipient] = {**bindings[scenario.recipient], "native_id": "thread-one"}
                    manifest = CaseManifest(plan.run_nonce, plan.source_sha256, scenario,
                                            _public_bindings(bindings))
                    sender = Client(endpoint, bindings[scenario.sender]["token"])
                    recipient = Client(endpoint, bindings[scenario.recipient]["token"])
                    try:
                        if scenario.kind == "busy_native":
                            asyncio.run(_busy_native(events, manifest, sender, recipient, directory))
                        elif scenario.kind == "exchange":
                            # Whole journeys cross the real stdio -> HTTP -> durable store boundary.
                            with MCPProcess(endpoint, sender.token, directory) as left, MCPProcess(endpoint, recipient.token, directory) as right:
                                _send(events, manifest, left.tool)
                                _complete_exchange(events, manifest, left.tool, right.tool)
                        else:
                            sent = _send(events, manifest, sender.call)
                            if scenario.kind == "duplicate":
                                events["duplicate"] = sender.call("send", to=scenario.recipient,
                                                                  body=sent["body"], key="challenge")
                                events["conflict"] = _error(sender.call, "send", to=scenario.recipient,
                                                           body="different intent", key="challenge")
                            elif scenario.kind == "replacement":
                                old = recipient
                                events["old_claim"] = recipient.call("claim")
                                replacement = store.join("behavior", scenario.recipient, replace=True)
                                events["replacement"] = {k: replacement[k] for k in ("participant", "generation", "native_id")}
                                recipient = Client(endpoint, replacement["token"])
                                events["replacement_inbox"] = recipient.call("inbox")
                                events["old_send"] = _error(old.call, "send", to=scenario.sender, body="stale", key="stale")
                                events["recovery"] = recipient.call("recover", message_id=sent["id"])
                                events["recovery_claim"] = recipient.call("claim")
                                sender.call("retry", message_id=sent["id"])
                                events["new_claim"] = recipient.call("claim")
                                events["stale_finish"] = _error(old.call, "finish", message_id=sent["id"],
                                    attempt_id=events["old_claim"]["attempt_id"], outcome="submitted")
                                events["after_stale_status"] = sender.call("status", message_id=sent["id"])
                                recipient.call("finish", message_id=sent["id"], attempt_id=events["new_claim"]["attempt_id"], outcome="submitted")
                            elif scenario.kind == "restart_retry":
                                events["old_claim"] = recipient.call("claim")
                                # End this broker ownership epoch, reopen the actual persistent store,
                                # then continue the same journey with the original scoped credentials.
                            if scenario.kind != "restart_retry":
                                _complete_exchange(events, manifest, sender.call, recipient.call)
                    except Exception as error:
                        failure = {"type": type(error).__name__, "code": getattr(error, "code", None)}
            except Exception as error:
                failure = {"type": type(error).__name__, "code": getattr(error, "code", None)}
            if manifest is None:
                manifest = CaseManifest(plan.run_nonce, plan.source_sha256, scenario, _public_bindings(bindings))
            expected.append(manifest)
            if scenario.kind == "restart_retry" and failure is None:
                try:
                    with owned_broker(state) as (_, _, endpoint):
                        sender = Client(endpoint, bindings[scenario.sender]["token"])
                        recipient = Client(endpoint, bindings[scenario.recipient]["token"])
                        sent = events["accepted"]
                        events["restart_status"] = sender.call("status", message_id=sent["id"])
                        events["automatic_claim"] = recipient.call("claim")
                        events["recipient_retry"] = _error(recipient.call, "retry", message_id=sent["id"])
                        events["sender_retry"] = _error(sender.call, "retry", message_id=sent["id"])
                        events["new_claim"] = events["automatic_claim"] if events["automatic_claim"]["message"] else recipient.call("claim")
                        events["stale_finish"] = _error(recipient.call, "finish", message_id=sent["id"],
                            attempt_id=events["old_claim"]["attempt_id"], outcome="submitted")
                        events["after_stale_status"] = sender.call("status", message_id=sent["id"])
                        recipient.call("finish", message_id=sent["id"], attempt_id=events["new_claim"]["attempt_id"], outcome="submitted")
                        _complete_exchange(events, manifest, sender.call, recipient.call)
                except Exception as error:
                    failure = {"type": type(error).__name__, "code": getattr(error, "code", None)}
            results.append({"manifest": json_value(asdict(manifest)), "manifest_sha256": manifest.sha256,
                            "events": events, "execution_error": failure})
    return plan, tuple(expected), {"schema_version": 1, "manifest": json_value(asdict(plan)), "cases": results,
                                  "final_source_sha256": source_digest()}


def _verify_case(expected, evidence, *, require_live=False):
    """Inspect observations without trusting producer pass flags or summaries."""
    failures = []
    def check(condition, code):
        if not condition:
            failures.append(code)
    check(expected.evidence_mode == "protocol_fixture" and not require_live, "evidence_mode")
    check(evidence.get("manifest") == json_value(asdict(expected)) and evidence.get("manifest_sha256") == expected.sha256,
          "manifest_identity")
    if evidence.get("execution_error") is not None:
        failures.append("execution_error")
        return tuple(failures)
    events = evidence.get("events", {})
    bindings = {b.participant: b for b in expected.bindings}
    scenario = expected.scenario
    sender, recipient = bindings[scenario.sender], bindings[scenario.recipient]
    replacement = events.get("replacement", {})
    if scenario.kind == "replacement":
        check(replacement.get("participant") == recipient.participant
              and replacement.get("generation") not in (None, recipient.generation), "replacement_identity")
        recipient_generation = replacement.get("generation")
    else:
        recipient_generation = recipient.generation
    sent = events.get("accepted", {})
    check(bool(sent.get("id")) and sent.get("accepted") is True and sent.get("sender") == sender.participant
          and sent.get("from_generation") == sender.generation and sent.get("to") == recipient.participant
          and sent.get("to_generation") == recipient.generation
          and sent.get("body") == f"{expected.run_nonce}/{scenario.name}: reply with this exact challenge",
          "message_identity")
    inbox = events.get("inbox_before_ack", {}).get("messages", [])
    observed = [m for m in inbox if m.get("id") == sent.get("id")]
    check(len(observed) == 1 and observed[0].get("body") == sent.get("body")
          and observed[0].get("from_generation") == sender.generation
          and observed[0].get("delivery_generation") == recipient_generation, "inbox_identity")
    pre = events.get("pre_receipt_status", {})
    check(pre.get("acknowledged") is False and pre.get("receipts") == [], "read_is_not_receipt")
    receipt = events.get("receipt", {})
    check(receipt.get("message_id") == sent.get("id") and bool(receipt.get("id"))
          and receipt.get("generation") == recipient_generation and receipt.get("observer") == recipient.participant
          and receipt.get("kind") == "client_ack"
          and receipt.get("native_id") == (replacement.get("native_id") if replacement else recipient.native_id),
          "receipt_identity")
    check(receipt == events.get("receipt_replay"), "receipt_idempotency")
    status = events.get("status", {})
    check(status.get("id") == sent.get("id") and status.get("acknowledged") is True
          and status.get("receipts") == [receipt], "receipt_persisted")
    reply = events.get("reply", {})
    check(reply.get("reply_to") == sent.get("id") and bool(reply.get("id"))
          and reply.get("sender") == recipient.participant and reply.get("from_generation") == recipient_generation
          and reply.get("to") == sender.participant and reply.get("to_generation") == sender.generation
          and reply.get("body") == "reply:" + sent.get("body", ""), "reply_link")
    stable_fields = ("id", "sender", "from_generation", "to", "to_generation", "reply_to", "body")
    check(any(all(m.get(key) == reply.get(key) for key in stable_fields) for m in status.get("replies", []))
          and any(m.get("id") == reply.get("id")
          and m.get("reply_to") == sent.get("id") for m in events.get("sender_inbox", {}).get("messages", [])), "reply_persisted")
    if scenario.kind == "duplicate":
        check(events.get("duplicate", {}).get("id") == sent.get("id")
              and events.get("duplicate", {}).get("idempotent_replay") is True, "duplicate_message")
        check(events.get("conflict") == "idempotency_conflict", "intent_conflict")
        check(len(inbox) == 1, "duplicate_inbox")
    if scenario.kind in ("replacement", "restart_retry"):
        old = events.get("old_claim", {})
        new = events.get("new_claim", {})
        check(old.get("message", {}).get("id") == sent.get("id") and new.get("message", {}).get("id") == sent.get("id")
              and bool(new.get("attempt_id")) and new.get("attempt_id") != old.get("attempt_id"), "attempt_identity")
        check(events.get("stale_finish") == "stale_attempt"
              and events.get("after_stale_status", {}).get("state") == "claimed", "stale_callback_fenced")
        attempts = status.get("attempts", [])
        check(len(attempts) == 2 and attempts[0].get("id") == old.get("attempt_id")
              and attempts[0].get("state") == "uncertain" and attempts[0].get("generation") == recipient.generation
              and attempts[0].get("message_id") == sent.get("id")
              and attempts[1].get("id") == new.get("attempt_id") and attempts[1].get("message_id") == sent.get("id")
              and attempts[1].get("state") == "submitted" and attempts[1].get("generation") == recipient_generation,
              "attempt_history")
    if scenario.kind == "replacement":
        check(events.get("old_send") == "stale_generation" and events.get("replacement_inbox", {}).get("messages") == [],
              "replacement_fenced")
        check(events.get("recovery", {}).get("retry_required") is True
              and events.get("recovery_claim", {}).get("message") is None, "recovery_requires_retry")
    if scenario.kind == "restart_retry":
        check(events.get("restart_status", {}).get("state") == "uncertain"
              and events.get("automatic_claim", {}).get("message") is None, "automatic_replay_blocked")
        check(events.get("recipient_retry") == "forbidden" and events.get("sender_retry") == "not_rejected", "retry_authority")
    if scenario.kind == "busy_native":
        native = events.get("native_status", {})
        attempts = native.get("attempts", [])
        check(events.get("submitted_while_held") is True
              and 0 < events.get("hold_entered_ns", 0) <= events.get("submitted_ns", 0)
              <= events.get("hold_released_ns", 0), "busy_overlap")
        check(native.get("state") == "submitted" and native.get("acknowledged") is False
              and len(attempts) == 1 and attempts[0].get("id") == events.get("claim", {}).get("attempt_id")
              and attempts[0].get("native_id") == recipient.native_id
              and attempts[0].get("generation") == recipient.generation
              and attempts[0].get("detail", {}).get("native_id") == recipient.native_id
              and attempts[0].get("detail", {}).get("busy") is True, "native_submission_identity")
    return tuple(failures)


def verify_case(expected, evidence, *, require_live=False):
    """Malformed evidence fails its own case without hiding neighboring cases."""
    try:
        return _verify_case(expected, evidence, require_live=require_live)
    except (AttributeError, KeyError, TypeError, ValueError):
        return ("malformed_evidence",)


def verify_run(expected, case_manifests, report, *, require_live=False):
    """Required/unexercised cases remain failures while neighboring evidence survives."""
    failures = {}
    if not isinstance(report, dict) or not isinstance(report.get("cases"), list):
        return {"run": ("malformed_evidence",)}
    if report.get("schema_version") != 1 or report.get("manifest") != json_value(asdict(expected)):
        failures["run"] = ("manifest_identity",)
    if report.get("final_source_sha256") != expected.source_sha256:
        failures["run"] = failures.get("run", ()) + ("source_changed",)
    required = {s.name for s in expected.scenarios}
    manifests = {c.scenario.name: c for c in case_manifests}
    if len(manifests) != len(case_manifests) or set(manifests) != required or any(c.run_nonce != expected.run_nonce or c.source_sha256 != expected.source_sha256
                                       for c in case_manifests):
        failures["run"] = failures.get("run", ()) + ("case_manifest_identity",)
    seen = set()
    for evidence in report.get("cases", []):
        try:
            name = evidence.get("manifest", {}).get("scenario", {}).get("name")
            if not isinstance(name, str):
                raise ValueError("Malformed case identity")
        except (AttributeError, TypeError, ValueError):
            failures["run"] = failures.get("run", ()) + ("malformed_evidence",)
            continue
        if name not in required or name in seen or name not in manifests:
            failures["run"] = failures.get("run", ()) + ("unexpected_or_duplicate_case",)
            continue
        seen.add(name)
        errors = verify_case(manifests[name], evidence, require_live=require_live)
        if errors:
            failures[name] = errors
    for missing in required - seen:
        failures[missing] = ("unexercised",)
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    expected, cases, report = run_suite()
    report["verification"] = verify_run(expected, cases, report)
    text = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if arguments.output:
        arguments.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 1 if report["verification"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
