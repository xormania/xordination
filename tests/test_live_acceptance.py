"""Adversarial verifier tests use synthetic native events, never vendor sessions."""
import hashlib
import importlib.util
import json
import os
import queue
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import tempfile
import unittest
from unittest.mock import patch

from helpers import owned_broker
from xordination.client import Client
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
        origin = {"codex": "codex/item/tool/call", "grok": "grok/session/update",
                  "claude": "claude/PreToolUse"}[binding["harness"]]
        live.record_native(self.directory, name, native_id or binding["native_id"],
                           call_id or os.urandom(8).hex(), tool, arguments, origin)

    def exercise(self, case, *, native=True, body=None, reply_before_ack=False, intent=None, reply=None):
        sender, recipient = case["sender"], case["recipient"]
        intent = intent or case
        args = {"to": recipient, "to_generation": self.frozen["bindings"][recipient]["generation"],
                "body": body or intent["body"], "key": intent["key"], "metadata": intent["metadata"]}
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
        reply_args = {"to": sender, "body": reply or intent["reply"], "reply_to": message["id"],
                      "key": "reply-" + intent["key"]}
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

    def assert_entry_point_imports_the_hashed_checkout(self, environment):
        probe = ("import importlib.util, sys\n"
                 "spec = importlib.util.spec_from_file_location('live_entry', sys.argv[1])\n"
                 "module = importlib.util.module_from_spec(spec)\n"
                 "spec.loader.exec_module(module)\n"
                 "import xordination\n"
                 "print(xordination.__file__)\n")
        result = subprocess.run([sys.executable, "-c", probe, str(SCRIPT)], capture_output=True, text=True,
                                cwd=tempfile.gettempdir(), env=environment, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(Path(result.stdout.strip()).resolve(),
                         (SCRIPT.parents[2] / "src" / "xordination" / "__init__.py").resolve())

    def test_entry_points_import_the_hashed_checkout_without_pythonpath(self):
        # source_sha256 covers this checkout's src; a subprocess entry point started from another
        # cwd, or with a relative or missing PYTHONPATH, must still import that same code.
        self.assert_entry_point_imports_the_hashed_checkout(
            {key: value for key, value in os.environ.items() if key != "PYTHONPATH"})

    def test_entry_points_import_the_hashed_checkout_ahead_of_earlier_pythonpath_copies(self):
        # Another xordination listed on PYTHONPATH before this checkout's src must not shadow it.
        with tempfile.TemporaryDirectory() as other:
            (Path(other) / "xordination").mkdir()
            (Path(other) / "xordination" / "__init__.py").write_text("")
            self.assert_entry_point_imports_the_hashed_checkout(
                dict(os.environ, PYTHONPATH=os.pathsep.join([other, str(SCRIPT.parents[2] / "src")])))

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

    def test_selected_pair_does_not_require_capture_from_unused_configured_participant(self):
        self.directory = Path(self.temporary.name) / "selected-run"
        config = {"participants": [{"name": "alice", "harness": "codex"},
                                   {"name": "bob", "harness": "grok"},
                                   {"name": "unused", "harness": "claude"}], "pairs": [["alice", "bob"]]}
        live.prepare(config, self.directory)
        self.store = Store(self.directory / "broker.sqlite3")
        self.addCleanup(self.store.close)
        for name in ("alice", "bob"):
            token = live.load(self.directory / (name + ".connection.json"))["token"]
            context = self.store.authenticate(token)
            self.store.bind(context, "native-" + name)
            self.contexts[name] = self.store.authenticate(token)
        live.capture(self.directory)
        self.frozen = live.load(self.directory / "frozen.json")
        self.exercise(self.frozen["cases"][0])
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["required_cases"], 1)
        self.assertEqual(report["passed_cases"], 1)
        self.assertFalse((self.directory / "native" / "unused.jsonl").exists())

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

    def test_claude_observer_cli_failures_preserve_native_tool_flow(self):
        self.directory = Path(self.temporary.name) / "claude-observer-run"
        live.prepare({"participants": [{"name": "alice", "harness": "codex"},
                                        {"name": "bob", "harness": "claude"}],
                      "pairs": [["alice", "bob"]]}, self.directory)
        self.store = Store(self.directory / "broker.sqlite3")
        self.addCleanup(self.store.close)
        for name in ("alice", "bob"):
            token = live.load(self.directory / (name + ".connection.json"))["token"]
            self.store.bind(self.store.authenticate(token), "native-" + name)
            self.contexts[name] = self.store.authenticate(token)
        live.capture(self.directory)
        self.frozen = live.load(self.directory / "frozen.json")
        message = self.exercise(self.frozen["cases"][0])
        self.assertEqual(live.verify(self.directory)["status"], "pass")
        packet = {"hook_event_name": "PreToolUse", "session_id": "native-bob", "tool_use_id": "native-call",
                  "tool_name": "mcp__xordination__xordination_ack", "tool_input": {"message_id": message["id"]}}
        original = (self.directory / "prepared.json").read_bytes()
        native = self.directory / "native" / "bob.jsonl"
        original_capture = native.read_bytes()
        diagnostic = self.directory / "observation-errors.jsonl"
        command = [sys.executable, str(SCRIPT), "observe-claude", "--run", str(self.directory), "--participant", "bob"]
        for failure in ("malformed", "source_changed", "stale_session", "capture_io", "diagnostic_io"):
            with self.subTest(failure=failure):
                self.assertEqual(live.verify(self.directory)["status"], "pass")
                value = dict(packet)
                text = json.dumps(value)
                if failure == "malformed":
                    text = "{malformed-native-hook"
                elif failure == "source_changed":
                    prepared = live.load(self.directory / "prepared.json")
                    prepared["source_sha256"] = "changed"
                    (self.directory / "prepared.json").write_bytes(live.encoded(prepared))
                elif failure == "stale_session":
                    value["session_id"] = "retired-session"
                    text = json.dumps(value)
                elif failure == "capture_io":
                    native.unlink()
                    native.mkdir()
                else:
                    diagnostic.mkdir()
                    text = "{malformed-native-hook"
                result = subprocess.run(command, input=text, text=True, capture_output=True,
                                        timeout=5, cwd=SCRIPT.parents[2])
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "")
                token = live.load(self.directory / "bob.connection.json")["token"]
                self.assertNotIn(token, result.stderr)
                (self.directory / "prepared.json").write_bytes(original)
                report = live.verify(self.directory)
                self.assertEqual(report["status"], "fail")
                self.assertEqual(report["passed_cases"], 0)
                reason = {"diagnostic_io": "native_observer_diagnostics_invalid",
                          "stale_session": "native_capture_identity:bob"}.get(failure, "native_observer_error:bob")
                self.assertIn(reason, report["errors"])
                if failure == "capture_io":
                    self.assertIn("native_capture_invalid:bob", report["errors"])
                for path in (native, diagnostic):
                    if path.is_dir():
                        path.rmdir()
                    elif path.exists():
                        path.unlink()
                native.write_bytes(original_capture)
                self.assertEqual(live.verify(self.directory)["status"], "pass")

    def test_observation_logging_failure_does_not_escape_passive_observer(self):
        def fail():
            raise ValueError("Injected observation failure")
        with patch.object(live.sys.stderr, "write", side_effect=OSError("Broken diagnostic stream")):
            self.assertIsNone(live.passive_observe(self.directory, "bob", fail))
        report = live.verify(self.directory)
        self.assertIn("native_observer_error:bob", report["errors"])

    def test_actual_grok_stdio_and_claude_hook_hold_requests_until_controlled_release(self):
        for harness in ("grok", "claude"):
            with self.subTest(harness=harness):
                directory = Path(self.temporary.name) / ("held-" + harness)
                config = {"participants": [{"name": "alice", "harness": "codex"}, {"name": "bob", "harness": harness}],
                          "pairs": [["alice", "bob"]], "scenarios": ["busy"]}
                live.prepare(config, directory)
                with owned_broker(directory / "broker.sqlite3") as (store, _, endpoint):
                    tokens = {name: live.load(directory / (name + ".connection.json"))["token"] for name in ("alice", "bob")}
                    for name, token in tokens.items():
                        store.bind(store.authenticate(token), "native-" + name)
                    live.capture(directory)
                    frozen = live.load(directory / "frozen.json")
                    case = frozen["cases"][0]
                    path, arm = live.arm_busy_gate(directory, frozen, case, endpoint, 2)
                    command = [sys.executable, str(SCRIPT), "stdio" if harness == "grok" else "observe-claude",
                               "--run", str(directory), "--participant", "bob"]
                    if harness == "grok":
                        command += ["--endpoint", endpoint]
                    process = subprocess.Popen(command, text=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE, cwd=SCRIPT.parents[2], env={**os.environ, "XORDINATION_TOKEN": tokens["bob"]})
                    frames = queue.Queue()
                    def reader(stream, frames):
                        for line in stream:
                            frames.put(line)
                    thread = threading.Thread(target=reader, args=(process.stdout, frames), daemon=True)
                    thread.start()
                    try:
                        if harness == "grok":
                            live.record_native(directory, "bob", "native-bob", "held-acp-call", "xordination_wait",
                                               arm["wait_arguments"], "grok/session/update")
                            self.assertFalse((path / "ready.json").exists(),
                                             "ACP announcement alone is not a held MCP request")
                            initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                                "protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "fixture", "version": "1"}}}
                            process.stdin.write(json.dumps(initialize) + "\n")
                            process.stdin.flush()
                            self.assertEqual(json.loads(frames.get(timeout=3))["id"], 1)
                            packet = {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {
                                "name": "xordination_wait", "arguments": arm["wait_arguments"]}}
                        else:
                            packet = {"hook_event_name": "PreToolUse", "session_id": "native-bob", "tool_use_id": "held-native-call",
                                      "tool_name": "mcp__xordination__xordination_wait", "tool_input": arm["wait_arguments"]}
                        process.stdin.write(json.dumps(packet) + "\n")
                        process.stdin.flush()
                        if harness == "claude":
                            process.stdin.close()
                        live.wait_until(lambda: (path / "ready.json").exists(), 3)  # noqa: B023
                        self.assertIsNone(process.poll())
                        self.assertTrue(frames.empty(), "Held native request must not complete before release")
                        ready = live.load(path / "ready.json")
                        self.assertEqual(ready["native_id"], "native-bob")
                        self.assertEqual(ready["generation"], frozen["bindings"]["bob"]["generation"])
                        if harness == "grok":
                            message = Client(endpoint, tokens["alice"]).call("send", to="bob", body=case["body"],
                                key=case["key"], metadata=case["metadata"],
                                to_generation=frozen["bindings"]["bob"]["generation"])
                        live.release_gate(path, arm, "test_release")
                        if harness == "grok":
                            result = json.loads(frames.get(timeout=3))
                            self.assertEqual(result["id"], 2)
                            self.assertEqual(result["result"]["structuredContent"]["messages"][0]["id"], message["id"])
                            process.stdin.close()
                        self.assertEqual(process.wait(timeout=3), 0)
                        thread.join(timeout=3)
                        self.assertFalse(thread.is_alive())
                        if harness == "claude":
                            self.assertTrue(frames.empty(), "Observation hook must not emit a permission decision")
                            events, _, errors = live.native_events(directory, frozen)
                            waits = [event for event in events if event["tool"] == "xordination_wait"]
                            self.assertEqual(len(waits), 1)
                            self.assertEqual(waits[0]["call_id"], "held-native-call")
                            self.assertNotIn("native_capture_identity:bob", errors)
                        self.assertEqual(live.load(path / "closed.json")["reason"], "released")
                    finally:
                        live.release_gate(path, arm, "cleanup")
                        if process.poll() is None:
                            process.terminate()
                            try:
                                process.wait(timeout=2)
                            except subprocess.TimeoutExpired:
                                process.kill()
                                process.wait(timeout=2)
                        for stream in (process.stdin, process.stdout, process.stderr):
                            if stream and not stream.closed:
                                stream.close()
                        thread.join(timeout=2)

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

    def fresh_run(self, name, config):
        self.directory = Path(self.temporary.name) / name
        live.prepare(config, self.directory)
        self.store = Store(self.directory / "broker.sqlite3")
        self.addCleanup(self.store.close)
        for participant in config["participants"]:
            token = live.load(self.directory / (participant["name"] + ".connection.json"))["token"]
            self.store.bind(self.store.authenticate(token), "native-" + participant["name"])
            self.contexts[participant["name"]] = self.store.authenticate(token)
        live.capture(self.directory)
        self.frozen = live.load(self.directory / "frozen.json")

    def test_run_records_checkout_runtime_image_and_harness_identity(self):
        folder = Path(self.temporary.name)
        tools, codex_home = folder / "bin", folder / "codex-home"
        tools.mkdir()
        codex_home.mkdir()
        (codex_home / "config.toml").write_text('model = "configured"\n')
        (codex_home / "auth.json").write_text('{"token": "native-credential-secret"}')
        manager = tools / "harnesses"
        manager.write_text("#!/bin/sh\nprintf '%s\\n' '{\"harnesses\": {\"codex\": {\"version\": \"9.9.9\"}}}'\n")
        manager.chmod(0o700)
        image = "sha256:" + "ab" * 32
        probes = {"codex": {"harness": "codex", "status": "supported"}}
        config = {"participants": [{"name": "alice", "harness": "codex", "model": "requested-model"},
                                   {"name": "bob", "harness": "claude"}]}
        environment = {"PATH": str(tools) + os.pathsep + os.environ.get("PATH", ""), "CODEX_HOME": str(codex_home),
                       "XORDINATION_IMAGE_DIGEST": image}
        with patch.dict(os.environ, environment):
            live.prepare(config, folder / "identity-run", probes)
            with self.assertRaisesRegex(ValueError, "IMAGE_DIGEST"):
                with patch.dict(os.environ, {"XORDINATION_IMAGE_DIGEST": "latest"}):
                    live.prepare(config, folder / "bad-digest-run")
        self.assertFalse((folder / "bad-digest-run").exists())  # Refused before any run state.
        prepared = (folder / "identity-run" / "prepared.json").read_text()
        self.assertNotIn("native-credential-secret", prepared)
        recorded = json.loads(prepared)["environment"]
        self.assertRegex(recorded["source"]["commit"], r"\A(?:[0-9a-f]{40}|[0-9a-f]{64}|unavailable)\Z")
        self.assertIn(recorded["source"]["dirty"], (True, False, "unknown"))
        self.assertEqual(recorded["python"]["version"], live.platform.python_version())
        self.assertEqual(recorded["os"]["system"], live.platform.system())
        self.assertEqual(recorded["image_digest"], image)
        self.assertEqual(recorded["capability_probes"], probes)
        self.assertEqual((recorded["harness_manager"]["returncode"], recorded["harness_manager"]["output"]),
                         (0, {"harnesses": {"codex": {"version": "9.9.9"}}}))
        alice = recorded["harness_fingerprints"]["alice"]
        self.assertEqual(alice["native_config_files"],
                         {str(codex_home / "config.toml"): hashlib.sha256(b'model = "configured"\n').hexdigest()})
        self.assertEqual(alice["settings"]["model"], "requested-model")
        # The fingerprint changes with inherited native configuration, not only with run arguments.
        (codex_home / "config.toml").write_text('model = "edited"\n')
        with patch.dict(os.environ, {"CODEX_HOME": str(codex_home)}):
            self.assertNotEqual(live.harness_fingerprint(config["participants"][0])["sha256"], alice["sha256"])
        # Without the optional inputs every value stays explicitly absent rather than guessed.
        with patch.dict(os.environ, {"PATH": str(folder / "empty")}):
            self.assertEqual(live.harness_manager_status()["status"], "unavailable")
        report = live.verify(self.directory)
        self.assertEqual(report["environment"], self.frozen["environment"])
        self.assertEqual((report["environment"]["image_digest"], report["environment"]["capability_probes"]),
                         ("unsupplied", "unrecorded"))

    def observed(self, name):
        from xordination.client import Client
        token = live.load(self.directory / (name + ".connection.json"))["token"]
        adapter = live.observed_adapter(self.directory, name, Client("http://127.0.0.1:9", token),
                                        sys.executable, self.temporary.name)
        adapter.session = "native-" + name
        return adapter

    def test_effective_settings_are_native_reports_or_explicitly_unobserved(self):
        import asyncio
        from xordination import adapters
        responses = {"thread/start": {"thread": {"id": "native-alice"}, "model": "observed-codex-model",
                                      "reasoningEffort": "high"},
                     "session/new": {"sessionId": "native-bob"}}  # Grok exposes no current model here.
        class Wire:
            async def request(self, method, params, **options):
                return responses[method]
        async def spawn(self, argv, **options):
            self.rpc = Wire()
        async def start(adapter, method):
            await adapter.spawn([sys.executable])
            await adapter.rpc.request(method, {"mcpServers": []})
        with patch.object(adapters.NativeAdapter, "spawn", spawn):
            asyncio.run(start(self.observed("alice"), "thread/start"))
            asyncio.run(start(self.observed("bob"), "session/new"))
        for case in self.frozen["cases"]:
            self.exercise(case)
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["native_settings"], {
            "alice": {"requested_model": "native_default", "requested_reasoning": "native_default",
                      "observed_model": "observed-codex-model", "observed_reasoning": "high",
                      "source": "codex/thread/start.response"},
            "bob": {"requested_model": "native_default", "requested_reasoning": "native_default",
                    "observed_model": "unobserved", "observed_reasoning": "unobserved",
                    "source": "grok/session/new.response"}})
        # A report from another native session is not this binding's effective setting.
        path = self.directory / "native" / "alice.session.json"
        observation = live.load(path)
        path.write_bytes(live.encoded({**observation, "native_id": "other-session"}))
        report = live.verify(self.directory)
        self.assertIn("native_session_identity:alice", report["errors"])
        self.assertEqual(report["native_settings"]["alice"]["observed_model"], "unobserved")

    def test_native_approval_waits_are_recorded_evidence_not_answers(self):
        import asyncio
        from xordination import adapters
        answers = iter(["y", ""])
        async def operator(prompt):
            return next(answers)
        codex, grok = self.observed("alice"), self.observed("bob")
        command = {"threadId": "native-alice", "itemId": "item-7", "command": "private project command"}
        permission = {"sessionId": "native-bob", "toolCall": {"toolCallId": "tool-3", "title": "private"},
                      "options": [{"kind": "allow_once", "optionId": "once"}]}
        with patch.object(adapters, "_terminal_line", operator):
            self.assertEqual(asyncio.run(codex.request("item/commandExecution/requestApproval", command)),
                             {"decision": "accept"})
            self.assertEqual(asyncio.run(grok.request("session/request_permission", permission)),
                             {"outcome": {"outcome": "cancelled"}})
        for case in self.frozen["cases"]:
            self.exercise(case)
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "pass")  # Approval evidence does not alter the verdict.
        self.assertEqual([(event["participant"], event["kind"], event["origin"], event["request_id"], event["outcome"])
                          for event in report["approval_waits"]],
                         [("alice", "wait", "codex/item/commandExecution/requestApproval", "item-7", None),
                          ("alice", "resolved", "codex/item/commandExecution/requestApproval", "item-7", "accept"),
                          ("bob", "wait", "grok/session/request_permission", "tool-3", None),
                          ("bob", "resolved", "grok/session/request_permission", "tool-3", "cancelled")])
        self.assertNotIn("private", json.dumps(report["approval_waits"]))
        # A Claude permission prompt reaches the passive hook, which records it and answers nothing.
        self.fresh_run("claude-approval-run", {"participants": [{"name": "alice", "harness": "codex"},
                                                                {"name": "carol", "harness": "claude"}],
                                               "pairs": [["alice", "carol"]]})
        command = [sys.executable, str(SCRIPT), "observe-claude", "--run", str(self.directory), "--participant", "carol"]
        for packet in ({"hook_event_name": "Notification", "notification_type": "permission_prompt",
                        "session_id": "native-carol", "message": "Claude needs your permission"},
                       {"hook_event_name": "Notification", "notification_type": "idle_prompt",
                        "session_id": "native-carol", "message": "waiting"}):
            result = subprocess.run(command, input=json.dumps(packet), text=True, capture_output=True,
                                    timeout=10, cwd=SCRIPT.parents[2])
            self.assertEqual((result.returncode, result.stdout), (0, ""), result.stderr)
        report = live.verify(self.directory)
        self.assertEqual([(event["kind"], event["origin"], event["native_id"]) for event in report["approval_waits"]],
                         [("wait", "claude/Notification", "native-carol")])
        self.assertFalse(any(error.startswith("approval") for error in report["errors"]))
        path = self.directory / "native" / "carol.approvals.jsonl"
        path.write_text(path.read_text().replace("native-carol", "other-session"))
        self.assertIn("approval_capture_invalid:carol", live.verify(self.directory)["errors"])

    def test_retained_context_requires_the_earlier_memo_from_the_same_session(self):
        self.fresh_run("context-run", {"participants": [{"name": "alice", "harness": "codex"},
                                                        {"name": "bob", "harness": "grok"}],
                                       "pairs": [["alice", "bob"]], "scenarios": ["context"]})
        case = self.frozen["cases"][0]
        self.assertIn(case["memo"], case["body"])
        self.assertNotIn(case["memo"], case["recall"]["body"])
        self.exercise(case)
        report = live.verify(self.directory)
        self.assertEqual(report["cases"][0]["failures"], ["recall_unexercised"])
        self.assertTrue(live.earlier_complete(report["cases"][0]))  # Dispatch may now send the recall.
        # A recall answered without the earlier memo is not retained context.
        self.exercise(case, intent=case["recall"], reply=case["recall"]["reply"][:-32] + "0" * 32)
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "fail")
        self.assertIn("missing_recall_reply", report["cases"][0]["failures"])
        self.assertTrue(report["cases"][0]["phases"]["recall_native_ack"])  # Handled, but without the memo.

    def test_retained_context_passes_only_in_order_and_resists_tampering(self):
        config = {"participants": [{"name": "alice", "harness": "codex"}, {"name": "bob", "harness": "grok"}],
                  "pairs": [["alice", "bob"]], "scenarios": ["context"]}
        self.fresh_run("ordered-context-run", config)
        case = self.frozen["cases"][0]
        self.exercise(case)
        self.exercise(case, intent=case["recall"])
        report = live.verify(self.directory)
        self.assertEqual(report["status"], "pass", report["cases"][0]["failures"])
        self.assertTrue(report["cases"][0]["phases"]["context_order"])
        # The recall handled before the earlier exchange cannot establish retained earlier context.
        self.fresh_run("reversed-context-run", config)
        case = self.frozen["cases"][0]
        self.exercise(case, intent=case["recall"])
        self.exercise(case)
        report = live.verify(self.directory)
        self.assertEqual(report["cases"][0]["failures"], ["missing_context_order"])
        prepared = live.load(self.directory / "prepared.json")
        prepared["cases"][0]["recall"]["reply"] = "Xordination acceptance recall forged"
        (self.directory / "prepared.json").write_bytes(live.encoded(prepared))
        with self.assertRaisesRegex(ValueError, "intent changed"):
            live.checked_draft(self.directory)

    def test_busy_timing_without_gate_evidence_is_unknown_and_still_fails(self):
        self.fresh_run("unknown-busy-run", {"participants": [{"name": "alice", "harness": "codex"},
                                                             {"name": "bob", "harness": "grok"}],
                                            "pairs": [["alice", "bob"]], "scenarios": ["busy"]})
        self.exercise(self.frozen["cases"][0])
        case = live.verify(self.directory)["cases"][0]
        self.assertEqual(case["failures"], ["missing_controlled_busy"])
        self.assertEqual((case["busy"]["submission_during_work"], case["busy"]["submitted_monotonic_ns"],
                          case["busy"]["handling"], case["busy"]["handled_while_held"], case["busy"]["in_turn"]),
                         ("unknown", "unknown", "observed", "unknown", "unknown"))
        # Turn identities, when the wire exposes both, distinguish in-turn handling from later handling.
        gate = self.directory / "gates" / self.frozen["cases"][0]["case_id"]
        gate.mkdir()
        (gate / "native-ready.json").write_text(json.dumps({"native_call_id": "wait-call"}))
        (gate / "released.json").write_text(json.dumps({"monotonic_ns": 200}))
        wait = {"participant": "bob", "tool": "xordination_wait", "call_id": "wait-call", "turn_id": "turn-1"}
        for turn, at, expected in (("turn-1", 100, ("yes", True)), ("turn-2", 300, ("no", False))):
            timing = live.busy_timing(self.directory, self.frozen["cases"][0], [wait], True,
                                      {"monotonic_ns": at, "turn_id": turn})
            self.assertEqual((timing["in_turn"], timing["handled_while_held"]), expected)
        unobserved = live.busy_timing(self.directory, self.frozen["cases"][0], [wait], True, None)
        self.assertEqual((unobserved["handling"], unobserved["in_turn"]), ("unknown", "unknown"))


if __name__ == "__main__":
    unittest.main()
