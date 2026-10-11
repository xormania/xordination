"""Adversarial verifier tests use synthetic native events, never vendor sessions."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from helpers import owned_broker
from xordination.store import Store


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "testing" / "live.py"
spec = importlib.util.spec_from_file_location("live_acceptance", SCRIPT)
live = importlib.util.module_from_spec(spec)
spec.loader.exec_module(live)


class LiveAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name) / "run"
        live.prepare({"participants": [{"name": "alice", "harness": "codex", "version": "fixture"},
                                        {"name": "bob", "harness": "grok", "version": "fixture"}]}, self.directory)
        self.store = Store(self.directory / "broker.sqlite3")
        self.addCleanup(self.store.close)
        self.contexts = {}
        for name in ("alice", "bob"):
            token = live.load(self.directory / (name + ".connection.json"))["token"]
            context = self.store.authenticate(token)
            self.store.bind(context, "native-" + name, capabilities={"transport": "fixture"})
            self.contexts[name] = self.store.authenticate(token)
        live.capture(self.directory)
        self.frozen = live.load(self.directory / "frozen.json")

    def emit(self, name, tool, arguments, *, call_id=None, native_id=None):
        binding = self.frozen["bindings"][name]
        origin = {"codex": "codex/item/tool/call", "grok": "grok/session/update"}[binding["harness"]]
        live.record_native(self.directory, name, native_id or binding["native_id"],
                           call_id or os.urandom(8).hex(), tool, arguments, origin)

    def exercise(self, case, *, native=True, body=None, reply_before_ack=False):
        sender, recipient = case["sender"], case["recipient"]
        args = {"to": recipient, "to_generation": self.frozen["bindings"][recipient]["generation"],
                "body": body or case["body"], "key": case["key"], "metadata": case["metadata"]}
        if native:
            self.emit(sender, "xordination_send", args)
        message = self.store.send(self.contexts[sender], **args)
        claim = self.store.claim(self.contexts[recipient])
        self.store.finish(self.contexts[recipient], message["id"], claim["attempt_id"], "submitted",
                          detail={"native_id": "native-" + recipient, "busy": False})
        ack = {"message_id": message["id"]}
        if not reply_before_ack:
            if native:
                self.emit(recipient, "xordination_ack", ack)
            self.store.ack(self.contexts[recipient], **ack)
        reply_args = {"to": sender, "body": case["reply"], "reply_to": message["id"],
                      "key": "reply-" + case["case_id"]}
        if native:
            self.emit(recipient, "xordination_send", reply_args)
        reply = self.store.send(self.contexts[recipient], **reply_args)
        if reply_before_ack:
            if native:
                self.emit(recipient, "xordination_ack", ack)
            self.store.ack(self.contexts[recipient], **ack)
        claim = self.store.claim(self.contexts[sender])
        self.store.finish(self.contexts[sender], reply["id"], claim["attempt_id"], "submitted", detail={"busy": False})
        if native:
            self.emit(sender, "xordination_ack", {"message_id": reply["id"]})
        self.store.ack(self.contexts[sender], reply["id"])
        return message

    def test_complete_correlated_synthetic_capture_and_readonly_wal_snapshot(self):
        for case in self.frozen["cases"]:
            self.exercise(case)
        paths = [self.directory / "broker.sqlite3", self.directory / "broker.sqlite3-wal"]
        before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["required_cases"], 2)
        self.assertEqual(report["passed_cases"], 2)
        self.assertEqual(before, {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})
        with live.readonly(self.directory / "broker.sqlite3") as connection:
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("UPDATE messages SET body='mutation'")
        token = live.load(self.directory / "alice.connection.json")["token"]
        self.assertNotIn(token, json.dumps(report))

    def test_broker_ack_and_reply_without_native_observation_cannot_pass(self):
        for case in self.frozen["cases"]:
            self.exercise(case, native=False)
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["passed_cases"], 0)
        self.assertTrue(all(case["phases"]["broker_receipt"] for case in report["cases"]))
        self.assertTrue(all(not case["phases"]["native_ack"] for case in report["cases"]))

    def test_missing_directed_pair_preserves_other_verified_pair(self):
        self.exercise(self.frozen["cases"][0])
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["passed_cases"], 1)
        self.assertIn("required_case_unexercised", report["cases"][1]["failures"])

    def test_wrong_native_session_invalidates_partial_phase_counts(self):
        for case in self.frozen["cases"]:
            self.exercise(case)
        self.emit("bob", "xordination_ack", {"message_id": "wrong"}, native_id="former-native-bob")
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["passed_cases"], 0)
        self.assertIn("native_capture_identity:bob", report["errors"])

    def test_wrong_nonce_cannot_be_promoted_by_native_prose(self):
        case = self.frozen["cases"][0]
        self.exercise(case, body=case["body"].replace(case["nonce"], "stale-nonce"))
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "fail")
        self.assertIn("missing_accepted", report["cases"][0]["failures"])

    def test_conflicting_native_call_id_invalidates_original_and_later_events(self):
        for case in self.frozen["cases"]:
            self.exercise(case)
        path = self.directory / "native" / "alice.jsonl"
        original = json.loads(path.read_text().splitlines()[0])
        self.emit("alice", "xordination_ack", {"message_id": "other"}, call_id=original["call_id"])
        report = live.verify(self.directory)
        self.assertEqual(report["passed_cases"], 0)
        self.assertIn("native_call_id_conflict:alice", report["errors"])

    def test_frozen_manifest_mutation_and_generation_replacement_are_rejected(self):
        frozen = live.load(self.directory / "frozen.json")
        frozen["cases"].pop()
        (self.directory / "frozen.json").write_bytes(live.encoded(frozen))
        with self.assertRaisesRegex(ValueError, "manifest was altered"):
            live.verify(self.directory)
        (self.directory / "frozen.json").write_bytes(live.encoded(self.frozen))
        self.store.join(self.frozen["space"], "bob", "grok", replace=True)
        report = live.verify(self.directory)
        self.assertIn("native_binding_changed", report["errors"])

    def test_required_matrix_cannot_be_weakened_before_freezing(self):
        prepared = live.load(self.directory / "prepared.json")
        original = live.encoded(prepared)
        for cases in (prepared["cases"][:1], [prepared["cases"][0], prepared["cases"][0]]):
            prepared["cases"] = cases
            (self.directory / "prepared.json").write_bytes(live.encoded(prepared))
            with self.assertRaisesRegex(ValueError, "required cases"):
                live.checked_draft(self.directory)
        (self.directory / "prepared.json").write_bytes(original)

    def test_out_of_run_native_time_is_rejected(self):
        for case in self.frozen["cases"]:
            self.exercise(case)
        path = self.directory / "native" / "alice.jsonl"
        events = [json.loads(line) for line in path.read_text().splitlines()]
        events[0]["time"] = self.frozen["created_at"] - 1
        offset, lines = 0, []
        for event in events:
            event["sequence"] = offset
            line = live.encoded(event) + b"\n"
            lines.append(line)
            offset += len(line)
        path.write_bytes(b"".join(lines))
        report = live.verify(self.directory)
        self.assertIn("native_capture_time:alice", report["errors"])
        self.assertEqual(report["passed_cases"], 0)

    def test_reply_before_ack_and_late_native_source_observation_remain_valid(self):
        for case in self.frozen["cases"]:
            self.exercise(case, reply_before_ack=True)
        # ACP observation arrival can lag native dispatch. Move the send observation
        # after its recipient acknowledgement/reply without changing causal message links.
        path = self.directory / "native" / "alice.jsonl"
        events = [json.loads(line) for line in path.read_text().splitlines()]
        sends = [event for event in events if event["tool"] == "xordination_send"]
        self.assertTrue(sends)
        sends[0]["monotonic_ns"] = max(event["monotonic_ns"] for event in events) + 100
        offset, lines = 0, []
        for event in events:
            event["sequence"] = offset
            line = live.encoded(event) + b"\n"
            lines.append(line)
            offset += len(line)
        path.write_bytes(b"".join(lines))
        # Receipt and reply have durable exact links; global observer timestamp
        # order is not a native execution-order oracle.
        self.assertEqual(live.verify(self.directory)["status"], "pass")

    def test_queued_return_reply_without_source_handling_cannot_pass(self):
        for case in self.frozen["cases"]:
            message = self.exercise(case)
            # A deliberately incomplete return leg must not be confused with a roundtrip.
            reply = self.store._db.execute("SELECT id FROM messages WHERE reply_to=?", (message["id"],)).fetchone()
            self.store._db.execute("DELETE FROM receipts WHERE message_id=?", (reply["id"],))
        report = live.verify(self.directory)
        self.assertEqual(report["passed_cases"], 0)
        self.assertTrue(all("missing_reply_broker_receipt" in case["failures"] for case in report["cases"]))

    def test_malformed_native_capture_reports_failure_and_retains_evidence(self):
        for case in self.frozen["cases"]:
            self.exercise(case)
        path = self.directory / "native" / "alice.jsonl"
        original = path.read_bytes()
        for invalid in (b"{malformed-json\n", live.encoded({"call_id": ["invalid"], "tool": "xordination_ack"}),
                        live.encoded({"call_id": "valid", "native_id": "native-alice"}),
                        live.encoded({"call_id": "valid", "native_id": "native-alice", "tool": ["invalid"]}),
                        json.dumps({"call_id": "valid", "native_id": "native-alice", "tool": "xordination_send",
                                    "arguments": {"metadata": {"invalid": float("nan")}}}).encode()):
            with self.subTest(invalid=invalid):
                path.write_bytes(original + invalid + b"\n")
                report = live.verify(self.directory)
                self.assertIn("native_capture_invalid:alice", report["errors"])
                self.assertEqual(report["passed_cases"], 0)

    def test_failed_verifications_are_retained_and_fresh_directory_required(self):
        live.verify(self.directory)
        live.verify(self.directory)
        self.assertEqual(len(list(self.directory.glob("report-*.json"))), 2)
        with self.assertRaises(FileExistsError):
            live.prepare(self.frozen["config"], self.directory)

    def test_control_dispatch_uses_real_http_and_cannot_exercise_pairs(self):
        with owned_broker(self.directory / "broker.sqlite3") as (_, _, endpoint):
            with patch.dict(os.environ, {"XORDINATION_ENDPOINT": endpoint}):
                report = live.send_tasks(self.directory)
        self.assertEqual(len(report["tasks_dispatched"]), 2)
        result = live.verify(self.directory)
        self.assertEqual(result["passed_cases"], 0)
        self.assertTrue(all("required_case_unexercised" in case["failures"] for case in result["cases"]))

    def test_grok_raw_dispatch_identity_and_discovery_boundaries(self):
        packet = {"sessionId": "native-bob", "update": {"sessionUpdate": "tool_call", "toolCallId": "native-call",
            "title": "use_tool", "rawInput": {"tool_name": "xordination__xordination_ack",
                                                 "tool_input": {"message_id": "a"}},
            "_meta": {"x.ai/tool": {"version": 1, "name": "use_tool", "namespace": "grok_build"}}}}
        self.assertEqual(live.grok_observation(packet),
                         ("native-bob", "native-call", "xordination_ack", {"message_id": "a"}))
        packet["update"]["rawInput"]["tool_name"] = "other__xordination_ack"
        self.assertIsNone(live.grok_observation(packet))
        packet["update"]["_meta"]["x.ai/tool"] = {"version": 1, "name": "search_tool", "namespace": "grok_build"}
        self.assertIsNone(live.grok_observation(packet))
        packet["update"]["_meta"]["x.ai/tool"]["version"] = 2
        with self.assertRaisesRegex(ValueError, "identity version"):
            live.grok_observation(packet)


if __name__ == "__main__":
    unittest.main()
