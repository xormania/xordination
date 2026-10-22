import contextlib
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from helpers import isolated_process_env, observe_fsync, owned_broker
import xordination
from xordination.adapters import _mcp_args
from xordination.cli import main
from xordination.store import Store
from xordination.trace import Trace, bundle


class BootstrapSafety(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "POSIX directory durability")
    def test_directory_sync_failure_rolls_back_replacement(self):
        with tempfile.TemporaryDirectory() as folder:
            state = str(Path(folder) / "state.sqlite")
            descriptor = Path(folder) / "credentials" / "private.json"
            store = Store(state)
            store.create_space("project")
            original = store.join("project", "one")
            try:
                with observe_fsync([descriptor.parent], fail=descriptor.parent) as reached_directory:
                    with contextlib.redirect_stderr(io.StringIO()):
                        result = main(["join", "--state", state, "--space", "project",
                                       "--participant", "one", "--replace", "--output", str(descriptor)])
                self.assertEqual(result, 2)
                self.assertTrue(reached_directory)
                self.assertFalse(descriptor.exists())
                self.assertEqual(store.authenticate(original["token"])["generation"],
                                 original["generation"])
            finally:
                store.close()

    @unittest.skipUnless(os.name == "posix", "POSIX directory durability")
    def test_nested_descriptor_creation_syncs_filename_and_new_ancestors(self):
        with tempfile.TemporaryDirectory() as folder:
            state = str(Path(folder) / "state.sqlite")
            descriptor = Path(folder) / "private" / "nested" / "credential.json"
            store = Store(state)
            store.create_space("project")
            store.join("project", "one")
            directories = (descriptor.parent, descriptor.parent.parent, Path(folder))
            try:
                with observe_fsync([descriptor, *directories]) as synced:
                    result = main(["join", "--state", state, "--space", "project",
                                   "--participant", "one", "--replace", "--output", str(descriptor)])
                self.assertEqual(result, 0)
                self.assertIn(descriptor, synced)
                for directory in directories:
                    self.assertIn(directory, synced[synced.index(descriptor) + 1:])
                issued = json.loads(descriptor.read_text())
                self.assertEqual(store.authenticate(issued["token"])["generation"], issued["generation"])
            finally:
                store.close()

    def test_closed_stdout_pipe_never_retires_current_binding(self):
        with tempfile.TemporaryDirectory() as folder:
            state = str(Path(folder) / "state.sqlite")
            store = Store(state)
            store.create_space("project")
            original = store.join("project", "one")
            reader, writer = os.pipe()
            os.close(reader)
            try:
                result = subprocess.run(
                    [sys.executable, "-m", "xordination", "join", "--state", state,
                     "--space", "project", "--participant", "one", "--replace"],
                    stdout=writer, stderr=subprocess.PIPE, timeout=10,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(b"pipe", result.stderr.lower())
                self.assertEqual(store.authenticate(original["token"])["generation"],
                                 original["generation"])
            finally:
                os.close(writer)
                store.close()

    def test_failed_descriptor_output_never_retires_current_binding(self):
        with tempfile.TemporaryDirectory() as folder:
            state = str(Path(folder) / "state.sqlite")
            descriptor = Path(folder) / "private.json"
            store = Store(state)
            store.create_space("project")
            original = store.join("project", "one")
            descriptor.write_text(json.dumps(original))
            args = ["join", "--state", state, "--space", "project", "--participant", "one",
                    "--replace", "--output", str(descriptor)]
            try:
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(main(args), 2)
                self.assertEqual(store.authenticate(original["token"])["generation"], original["generation"])
                self.assertEqual(json.loads(descriptor.read_text())["token"], original["token"])
                descriptor.unlink()
                with observe_fsync([descriptor], fail=descriptor) as failed_descriptor:
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(main(args), 2)
                self.assertTrue(failed_descriptor)
                self.assertFalse(descriptor.exists())
                self.assertEqual(store.authenticate(original["token"])["generation"], original["generation"])
            finally:
                store.close()

    def test_claude_hook_entry_point_refuses_retired_generation_without_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            with owned_broker(Path(folder) / "state.sqlite") as (store, _, endpoint):
                store.create_space("project")
                sender = store.authenticate(store.join("project", "sender")["token"])
                retired = store.join("project", "claude", harness="claude", native_id="old-session")
                old_message = store.send(sender, "claude", "For the old generation", "old")
                current = store.join("project", "claude", harness="claude", native_id="new-session", replace=True)
                context = store.authenticate(current["token"])
                store.bind(context, "new-session", {"channel_registered": True})
                before = (store.identity(context), store.authenticate(retired["token"], allow_retired=True),
                          store.status(sender, old_message["id"]))
                packet = {"session_id": "cleared-session", "hook_event_name": "SessionStart", "source": "clear"}
                result = subprocess.run(
                    [sys.executable, "-m", "xordination", "claude-hook", "--endpoint", endpoint,
                     "--token-env", "XORDINATION_TEST_TOKEN"],
                    input=json.dumps(packet), capture_output=True, text=True, timeout=30, cwd=folder,
                    env=isolated_process_env(folder, token=retired["token"]))
                self.assertNotEqual(0, result.returncode)
                self.assertEqual("", result.stdout)  # No native hook decision or continuation override.
                self.assertIn("superseded", result.stderr)
                self.assertNotIn(retired["token"], result.stderr)
                after = (store.identity(context), store.authenticate(retired["token"], allow_retired=True),
                         store.status(sender, old_message["id"]))
                self.assertEqual(before, after)


class DiagnosticsBundle(unittest.TestCase):
    def test_bundle_retains_correlation_and_never_credentials_or_bodies(self):
        token, body = "bundle-token-c81e", "confidential bundle body 4a7d"
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            trace_path = folder / "mcp.trace"
            trace = Trace(trace_path, "mcp-stdio", secrets=(token,))
            trace.record("in", {"jsonrpc": "2.0", "id": 7, "method": "tools/call",
                                "params": {"name": "xordination_send", "arguments": {"body": body}}})
            trace.record("out", {"jsonrpc": "2.0", "method": "notifications/claude/channel",
                                 "params": {"content": body, "meta": {"message_id": "message-9"}}})
            trace.close()
            with trace_path.open("a") as stream:
                # Unredacted or foreign lines must be re-redacted or dropped, never copied.
                stream.write(json.dumps({"format": "xordination-trace/1", "trace": "x", "seq": 4,
                                         "stream": "forged-" + token, "direction": "out", "event": "frame",
                                         "monotonic_ns": 1, "time": 1.0,
                                         "frame": {"params": {"env": [{"name": "XORDINATION_TOKEN",
                                                                       "value": token}], "body": body}}}) + "\n")
                stream.write("Authorization: Bearer " + token + "\n")
            observed = {"status": "unverified", "binary": "/opt/" + token + "/codex"}
            output = folder / "support.zip"
            args = ["diagnostics", "bundle", "--output", str(output), "--trace", str(trace_path),
                    "--harness", "codex"]
            with owned_broker(folder / "state.sqlite") as (_, _, endpoint), \
                    patch("xordination.adapters.doctor", return_value=observed), \
                    patch.dict(os.environ, {"XORDINATION_TOKEN": token}), \
                    contextlib.redirect_stdout(io.StringIO()) as printed:
                self.assertEqual(0, main(args + ["--endpoint", endpoint]))
            self.assertEqual(0o600, os.stat(output).st_mode & 0o777)
            with zipfile.ZipFile(output) as archive:
                members = {name: archive.read(name).decode() for name in archive.namelist()}
            self.assertEqual({"manifest.json", "doctor.json", "health.json", "traces/00-mcp.trace"}, set(members))
            for name, text in members.items():
                self.assertNotIn(token, text, name)
                self.assertNotIn(body, text, name)
            self.assertNotIn(token, printed.getvalue())
            manifest = json.loads(members["manifest.json"])
            self.assertEqual("excluded", manifest["redaction"]["message_bodies"])
            self.assertEqual(1, manifest["traces"][0]["dropped_unrecognized_lines"])
            self.assertFalse(manifest["live_communication_verified"])
            records = [json.loads(line) for line in members["traces/00-mcp.trace"].splitlines()]
            self.assertEqual([1, 2, 3, 4], [record["seq"] for record in records])
            self.assertEqual(7, records[1]["frame"]["id"])
            self.assertEqual("message-9", records[2]["frame"]["params"]["meta"]["message_id"])
            # params.env is no protocol path, so even its "name" key is redacted.
            self.assertEqual("<redacted 17 chars>", records[3]["frame"]["params"]["env"][0]["name"])
            health = json.loads(members["health.json"])
            self.assertEqual(("available", True), (health["status"], health["health"]["ok"]))
            self.assertEqual("unverified", json.loads(members["doctor.json"])["harnesses"]["codex"]["status"])

            # An existing archive is never overwritten; an unreachable broker stays explicit.
            before = output.read_bytes()
            with patch("xordination.adapters.doctor", return_value=observed), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(2, main(args))
            self.assertEqual(before, output.read_bytes())
            other = folder / "unreachable.zip"
            with patch("xordination.adapters.doctor", return_value=observed), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(0, main(["diagnostics", "bundle", "--output", str(other), "--harness", "codex",
                                          "--endpoint", "http://127.0.0.1:1"]))
            with zipfile.ZipFile(other) as archive:
                health = json.loads(archive.read("health.json"))
            self.assertEqual(("unavailable", "connection_error"), (health["status"], health["error"]))

    def test_correlation_keys_are_kept_only_at_protocol_paths(self):
        secrets = ["sk-live-" + "%02d" % index + "d93f" for index in range(8)]
        frames = [
            # MCP send: the tool name and reply_to are protocol fields; metadata is caller content.
            {"jsonrpc": "2.0", "id": "call-1", "method": "tools/call", "params": {
                "name": "xordination_send", "arguments": {"to": "peer", "body": "b", "key": "k", "reply_to": "m-0",
                                                          "metadata": {"id": secrets[0], "name": secrets[1],
                                                                       "status": secrets[2]}}}},
            # A broker result echoes the message, including its metadata.
            {"jsonrpc": "2.0", "id": "call-1", "result": {"content": [{"type": "text", "text": "{}"}],
                "structuredContent": {"id": "m-1", "message_id": "m-1", "reply_to": "m-0",
                                      "metadata": {"id": secrets[3], "attempt_id": secrets[4]}}}},
            # Native frames keep thread/turn/session identities, not nested payload ids.
            {"jsonrpc": "2.0", "method": "item/completed", "params": {
                "threadId": "thread-1", "turnId": "turn-1",
                "item": {"id": "item-1", "type": "agentMessage", "arguments": {"id": secrets[5]}}}},
            {"jsonrpc": "2.0", "id": 3, "result": {"thread": {"id": "thread-1", "name": secrets[6]},
                                                  "sessionId": "session-1", "_meta": {"callId": secrets[7]}}},
        ]
        expected = [
            ("id",), ("method",), ("params", "name"), ("params", "arguments", "reply_to"),
            ("result", "structuredContent", "id"), ("result", "structuredContent", "message_id"),
            ("result", "content", 0, "type"), ("params", "threadId"), ("params", "turnId"),
            ("params", "item", "id"), ("params", "item", "type"), ("result", "thread", "id"),
            ("result", "sessionId"),
        ]
        def lookup(frames, path):
            values = []
            for frame in frames:
                value = frame
                for part in path:
                    try:
                        value = value[part]
                    except (KeyError, IndexError, TypeError):
                        break
                else:
                    values.append(value)
            return values
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            path = folder / "paths.trace"
            trace = Trace(path, "mcp-stdio")
            for frame in frames:
                trace.record("in", frame)
            trace.close()
            # A trace written before path matching held raw values; bundling must re-redact them.
            with path.open("a") as stream:
                for index, frame in enumerate(frames):
                    stream.write(json.dumps({"format": "xordination-trace/1", "trace": "legacy", "seq": index + 1,
                                             "stream": "mcp-stdio", "direction": "in", "event": "frame",
                                             "monotonic_ns": 1, "time": 1.0, "frame": frame}) + "\n")
            traced = path.read_text()
            output = folder / "support.zip"
            bundle(output, traces=[path])
            with zipfile.ZipFile(output) as archive:
                bundled = archive.read("traces/00-paths.trace").decode()
        written = [json.loads(line)["frame"] for line in traced.splitlines() if '"legacy"' not in line
                   and "frame" in json.loads(line)]
        rebundled = [json.loads(line)["frame"] for line in bundled.splitlines() if "frame" in json.loads(line)]
        for secret in secrets:
            self.assertNotIn(secret, bundled)
            self.assertNotIn(secret, json.dumps(written))
        self.assertEqual(written + written, rebundled)
        for key in expected:
            self.assertEqual(lookup(frames, key), lookup(written, key), key)
            self.assertTrue(lookup(written, key), key)
        self.assertEqual("<redacted 14 chars>", written[0]["params"]["arguments"]["metadata"]["id"])
        self.assertEqual("<redacted 14 chars>", written[1]["result"]["structuredContent"]["metadata"]["id"])

    def test_native_identity_conflict_fields_stay_redacted(self):
        observed, native = "observed-session-5b2e", "native-session-81c0"
        source, withheld = "hook-reported-77aa", "native_identity_conflict"
        frames = [
            # An identity tool result now carries the fenced conflict record.
            {"jsonrpc": "2.0", "id": "call-identity", "result": {
                "content": [{"type": "text", "text": "{}"}], "isError": False,
                "structuredContent": {"id": "participant-1", "native_id": native,
                                      "native_identity_source": "conflict",
                                      "native_identity_conflict": {"observed_native_id": observed,
                                                                   "identity_source": source, "observed": 1.5}}}},
            # A withheld message keeps correlation identifiers only.
            {"jsonrpc": "2.0", "id": "call-status", "result": {
                "content": [{"type": "text", "text": "{}"}], "isError": False,
                "structuredContent": {"id": "m-1", "message_id": "m-1", "body": None, "metadata": None,
                                      "content_withheld": withheld,
                                      "messages": [{"id": "m-2", "content_withheld": withheld,
                                                    "native_id": native}]}}},
            # Fenced tool errors surface only as text content.
            {"jsonrpc": "2.0", "id": "call-send", "result": {"isError": True, "content": [{"type": "text", "text":
                json.dumps({"error": {"code": "replacement_required", "message": "fenced " + observed}})}]}},
        ]
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            path = folder / "conflict.trace"
            trace = Trace(path, "mcp-stdio")
            for frame in frames:
                trace.record("out", frame)
            trace.close()
            with path.open("a") as stream:
                for index, frame in enumerate(frames):
                    stream.write(json.dumps({"format": "xordination-trace/1", "trace": "legacy", "seq": index + 1,
                                             "stream": "mcp-stdio", "direction": "out", "event": "frame",
                                             "monotonic_ns": 1, "time": 1.0, "frame": frame}) + "\n")
            traced = path.read_text()
            output = folder / "support.zip"
            bundle(output, traces=[path])
            with zipfile.ZipFile(output) as archive:
                bundled = archive.read("traces/00-conflict.trace").decode()
        written = [json.loads(line)["frame"] for line in traced.splitlines()
                   if '"legacy"' not in line and "frame" in json.loads(line)]
        def leaves(value):
            if isinstance(value, dict):
                return [leaf for item in value.values() for leaf in leaves(item)]
            if isinstance(value, list):
                return [leaf for item in value for leaf in leaves(item)]
            return [value] if isinstance(value, str) else []
        rebundled = [json.loads(line)["frame"] for line in bundled.splitlines() if "frame" in json.loads(line)]
        self.assertEqual(written + written, rebundled)
        kept = leaves(rebundled)
        # Key names stay structural; no new conflict value is kept anywhere.
        for value in (observed, native, source, "replacement_required"):
            self.assertNotIn(value, bundled)
            self.assertFalse(any(value in leaf for leaf in kept), value)
        for value in (withheld, "conflict"):
            self.assertNotIn(value, kept)
        identity = written[0]["result"]["structuredContent"]
        self.assertEqual("participant-1", identity["id"])
        self.assertEqual("<redacted 21 chars>", identity["native_identity_conflict"]["observed_native_id"])
        self.assertEqual(1.5, identity["native_identity_conflict"]["observed"])
        status = written[1]["result"]["structuredContent"]
        self.assertEqual(("m-1", "m-1", "m-2"), (status["id"], status["message_id"], status["messages"][0]["id"]))
        self.assertEqual("<redacted 24 chars>", status["content_withheld"])
        self.assertIsNone(status["body"])
        self.assertTrue(written[2]["result"]["content"][0]["text"].startswith("<redacted "))

    def test_cancel_and_receipt_fields_keep_only_correlation_identifiers(self):
        reason, sender, kind = "private cancel reason 3c9d", "sender-alias-6e2f", "native_insertion"
        frames = [
            {"jsonrpc": "2.0", "id": "call-cancel", "method": "tools/call", "params": {
                "name": "xordination_cancel", "arguments": {"message_id": "m-7", "reason": reason}}},
            {"jsonrpc": "2.0", "id": "call-cancel", "result": {
                "content": [{"type": "text", "text": "{}"}], "isError": False,
                "structuredContent": {"id": "m-7", "message_id": "m-7", "state": "cancelled", "idempotent_replay": False,
                                      "cancellation": {"participant": sender, "generation": 3,
                                                       "previous_state": "queued", "reason": reason,
                                                       "cancelled_at": 2.5, "may_have_been_submitted": False}}}},
            {"jsonrpc": "2.0", "id": "call-status", "result": {
                "content": [{"type": "text", "text": "{}"}], "isError": False,
                "structuredContent": {"id": "m-7", "receipt_kinds": [kind, "agent_ack"],
                                      "receipts": [{"id": "r-1", "message_id": "m-7", "kind": kind,
                                                    "observer": sender, "evidence": {"id": reason}}],
                                      "cancellation": {"participant": sender, "reason": None}}}},
        ]
        with tempfile.TemporaryDirectory() as folder:
            folder = Path(folder)
            path = folder / "cancel.trace"
            trace = Trace(path, "mcp-stdio")
            for frame in frames:
                trace.record("in" if "method" in frame else "out", frame)
            trace.close()
            output = folder / "support.zip"
            bundle(output, traces=[path])
            with zipfile.ZipFile(output) as archive:
                bundled = archive.read("traces/00-cancel.trace").decode()
            traced = path.read_text()
        for text in (traced, bundled):
            for value in (reason, sender, kind, "agent_ack", "queued"):
                self.assertNotIn(value, text)
        written = [json.loads(line)["frame"] for line in traced.splitlines() if "frame" in json.loads(line)]
        self.assertEqual(written, [json.loads(line)["frame"] for line in bundled.splitlines()
                                   if "frame" in json.loads(line)])
        call, cancelled, status = written[0], written[1]["result"]["structuredContent"], \
            written[2]["result"]["structuredContent"]
        self.assertEqual(("xordination_cancel", "m-7"), (call["params"]["name"], call["params"]["arguments"]["message_id"]))
        self.assertEqual("<redacted 26 chars>", call["params"]["arguments"]["reason"])
        self.assertEqual(("m-7", "m-7"), (cancelled["id"], cancelled["message_id"]))
        self.assertEqual("<redacted 26 chars>", cancelled["cancellation"]["reason"])
        self.assertEqual((3, 2.5, False), (cancelled["cancellation"]["generation"],
                                           cancelled["cancellation"]["cancelled_at"],
                                           cancelled["cancellation"]["may_have_been_submitted"]))
        self.assertEqual(("r-1", "m-7"), (status["receipts"][0]["id"], status["receipts"][0]["message_id"]))
        # A nested evidence "id" is not a correlation path.
        self.assertEqual("<redacted 26 chars>", status["receipts"][0]["evidence"]["id"])
        self.assertEqual(["<redacted 16 chars>", "<redacted 9 chars>"], status["receipt_kinds"])

    def test_trace_stops_with_marker_at_size_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "bounded.trace"
            trace = Trace(path, "mcp-stdio", max_bytes=600)
            for index in range(20):
                trace.record("out", {"jsonrpc": "2.0", "id": index, "result": {}})
            trace.close()
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertLess(path.stat().st_size, 600 + 400)
            self.assertEqual("trace_limit_reached", records[-1]["event"])
            self.assertNotIn("frame", records[-1])
            self.assertEqual(list(range(1, len(records) + 1)), [record["seq"] for record in records])

    def test_trace_option_reaches_spawned_stdio_frontend_as_absolute_path(self):
        args = _mcp_args("http://127.0.0.1:8765", channel=True, trace="relative.trace")
        self.assertEqual(os.path.abspath("relative.trace"), args[args.index("--trace") + 1])
        self.assertNotIn("--trace", _mcp_args("http://127.0.0.1:8765"))
        with patch.dict(os.environ, {"XORDINATION_TOKEN": "token"}), \
                patch("xordination.mcp.run_stdio", return_value=0) as stdio, \
                patch("xordination.adapters.run_adapter", return_value=0) as adapter:
            self.assertEqual(0, main(["stdio", "--trace", "frontend.trace"]))
            self.assertEqual(0, main(["adapter", "codex", "--project", ".", "--trace", "native.trace"]))
        self.assertEqual("frontend.trace", stdio.call_args.kwargs["trace"])
        self.assertEqual("native.trace", adapter.call_args.kwargs["trace"])


PARTICIPANTS = [{"name": "alice", "harness": "codex"}, {"name": "bob", "harness": "claude"},
                {"name": "carol", "harness": "grok"}]
RUNNER_SHA256 = "a" * 64


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def source_sha256(runner):
    """live.py's fingerprint of its own bytes and this package's modules, keyed by checkout path."""
    files = {"scripts/testing/live.py": runner}
    for path in sorted(Path(xordination.__file__).parent.glob("*.py")):
        files["src/xordination/" + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return digest(files)


def probe(harness, binary=None, status="supported"):
    """A doctor observation of the installed executable that fingerprinted reports name."""
    return {"harness": harness, "status": status, "binary": binary or "/opt/" + harness + "/bin/" + harness,
            "version": {"value": harness + " 1.0", "returncode": 0}}


def evidence_report(status="pass", cases=(("alice", "bob", "pass"),), participants=PARTICIPANTS, **overrides):
    """A report fingerprinted the way live.py does, to this package and to `probe` executables."""
    cases = [{"case_id": str(index), "sender": sender, "recipient": recipient, "scenario": "idle",
              "status": case_status, "phases": {}, "failures": []}
             for index, (sender, recipient, case_status) in enumerate(cases)]
    participants = [{**row, "version": row["harness"] + " 1.0", "binary": probe(row["harness"])["binary"]}
                    for row in participants]
    config = {"participants": participants, "pairs": [[case["sender"], case["recipient"]] for case in cases],
              "scenarios": ["idle"] if cases else []}
    report = {"schema_version": 1, "run_id": "run", "evidence_mode": "operator_native",
              "config_sha256": digest(config), "source_sha256": source_sha256(RUNNER_SHA256),
              "runner_sha256": RUNNER_SHA256, "status": status,
              "required_cases": len(cases), "passed_cases": sum(case["status"] == "pass" for case in cases),
              "cases": cases, "errors": [], "native_artifacts": [],
              "configured_participants": participants}
    report.update(overrides)
    return report


class FakeDistribution:
    """An installation whose files are the imported package itself plus a recorded console script."""
    version = xordination.__version__

    def __init__(self, entry_value="xordination.cli:main", script=None, recorded=True):
        self.entry_points = [importlib.metadata.EntryPoint("xordination", entry_value, "console_scripts")]
        self.script = script
        # RECORD lists console scripts relative to site-packages, e.g. ../../../bin/xordination.
        self.files = ([importlib.metadata.PackagePath("xordination/__init__.py")]
                      + ([importlib.metadata.PackagePath("../../../bin/xordination")] if script else [])
                      if recorded else None)

    def locate_file(self, name):
        if str(name).endswith("bin/xordination"):
            return self.script
        return Path(xordination.__file__).resolve().parent.parent / name


class ReadinessReport(unittest.TestCase):
    def readiness(self, *args, env=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, env or {}), contextlib.redirect_stdout(stdout), \
                contextlib.redirect_stderr(stderr):
            code = main(["readiness", *args])
        return code, json.loads(stdout.getvalue()) if code == 0 else stderr.getvalue()

    def write(self, folder, name, value):
        path = Path(folder) / name
        path.write_text(json.dumps(value))
        return str(path)

    def test_levels_are_separate_and_default_probes_nothing_native(self):
        with tempfile.TemporaryDirectory() as folder, \
                patch("xordination.adapters.doctor", side_effect=AssertionError("probed a harness")):
            state = Path(folder) / "absent.sqlite3"
            code, result = self.readiness("--state", str(state), env={"XORDINATION_TOKEN": ""})
            self.assertEqual(code, 0)
            self.assertEqual(set(result["levels"]), {"package", "service", "adapter", "communication"})
            self.assertFalse(result["native_sessions_started"])
            for name in ("service", "adapter", "communication"):
                self.assertEqual(result["levels"][name]["status"], "unverified", name)
                self.assertNotIn(name, result["passed"])
            self.assertEqual(result["levels"]["package"]["checks"]["state_location"], "passed")
            self.assertFalse(state.exists())

    def test_package_level_requires_the_imported_module_to_be_the_installed_distribution(self):
        with tempfile.TemporaryDirectory() as folder:
            script = Path(folder) / "env" / "bin" / "xordination"
            script.parent.mkdir(parents=True)
            script.write_text("#!/bin/sh\n")
            installed = Path(folder) / "installed" / "bin" / "xordination"
            installed.parent.mkdir(parents=True)
            installed.write_text("#!/bin/sh\n")
            which = patch("shutil.which", return_value=str(script))
            which.start()
            self.addCleanup(which.stop)
            state = str(Path(folder) / "state.sqlite3")
            with patch("importlib.metadata.distribution", return_value=FakeDistribution(script=script)):
                _, result = self.readiness("--state", state)
                package = result["levels"]["package"]
                self.assertEqual(package["status"], "passed")
                self.assertEqual(package["entry_point"], str(script))
                self.assertEqual(result["passed"], ["package"])
                _, result = self.readiness("--state", str(Path(folder) / "missing" / "state.sqlite3"))
                self.assertEqual(result["levels"]["package"]["checks"]["state_location"], "failed")
                _, result = self.readiness()
                self.assertEqual(result["levels"]["package"]["status"], "unverified")
            cases = {
                "checkout without an installation": dict(side_effect=importlib.metadata.PackageNotFoundError),
                "wrong entry point": dict(return_value=FakeDistribution("other:main", script=script)),
                # PATH finds another environment's executable while the import satisfies the declaration.
                "foreign PATH command": dict(return_value=FakeDistribution(script=installed)),
                "no console script recorded": dict(return_value=FakeDistribution()),
            }
            for label, options in cases.items():
                with self.subTest(label), patch("importlib.metadata.distribution", **options):
                    _, result = self.readiness("--state", state)
                    self.assertEqual(result["levels"]["package"]["status"], "failed")
                    self.assertNotIn("package", result["passed"])
            with patch("importlib.metadata.distribution", return_value=FakeDistribution(script=installed)):
                _, result = self.readiness("--state", state)
            self.assertEqual(result["levels"]["package"]["checks"]["console_entry_point"], "failed")
            self.assertIn("not this distribution's console script", result["levels"]["package"]["detail"])
            # Without an installed-file record the PATH command cannot be attributed either way.
            with patch("importlib.metadata.distribution", return_value=FakeDistribution(recorded=False)):
                _, result = self.readiness("--state", state)
            self.assertEqual(result["levels"]["package"]["checks"]["console_entry_point"], "unverified")
            self.assertEqual(result["levels"]["package"]["status"], "unverified")

    def test_service_level_needs_an_authenticated_operation_not_only_a_listening_port(self):
        with tempfile.TemporaryDirectory() as folder:
            with owned_broker(Path(folder) / "broker.sqlite3") as (store, _, endpoint):
                store.create_space("project")
                alice = store.join("project", "alice")
                _, result = self.readiness("--endpoint", endpoint, env={"XORDINATION_TOKEN": ""})
                service = result["levels"]["service"]
                self.assertEqual((service["status"], service["checks"]["health"]), ("unverified", "passed"))
                self.assertEqual(service["broker_version"], xordination.__version__)
                _, result = self.readiness("--endpoint", endpoint, "--token-env", "READINESS_TOKEN",
                                           env={"READINESS_TOKEN": alice["token"]})
                service = result["levels"]["service"]
                self.assertEqual(service["status"], "passed")
                self.assertEqual(service["participant"]["participant"], "alice")
                self.assertEqual(service["participant"]["generation"], alice["generation"])
                self.assertIn("service", result["passed"])
                self.assertNotIn(alice["token"], json.dumps(result))
                _, result = self.readiness("--endpoint", endpoint, env={"XORDINATION_TOKEN": "invalid"})
                service = result["levels"]["service"]
                self.assertEqual((service["status"], service["error"]), ("failed", "unauthorized"))
                code, error = self.readiness("--endpoint", endpoint, "--token-env", "UNSET_READINESS_TOKEN")
                self.assertEqual(code, 2)
                self.assertIn("UNSET_READINESS_TOKEN", error)
            _, result = self.readiness("--endpoint", endpoint, env={"XORDINATION_TOKEN": ""})
            self.assertEqual(result["levels"]["service"]["status"], "failed")
            self.assertEqual(result["levels"]["service"]["error"], "connection_error")

    def test_communication_and_session_identity_come_only_from_verified_evidence(self):
        observed = {"codex": probe("codex"), "claude": probe("claude"), "grok": probe("grok", status="unavailable")}
        doctor = patch("xordination.adapters.doctor", side_effect=lambda name, binary=None: observed[name])
        with tempfile.TemporaryDirectory() as folder, doctor:
            _, result = self.readiness("--harness", "codex", "--harness", "claude", "--harness", "grok")
            self.assertEqual(result["levels"]["communication"]["status"], "unverified")
            adapter = result["levels"]["adapter"]
            self.assertEqual(adapter["harnesses"]["codex"]["checks"],
                             {"installed_capability": "passed", "session_identity": "unverified"})
            self.assertEqual(adapter["harnesses"]["grok"]["status"], "failed")
            self.assertEqual(adapter["status"], "failed")
            passing = self.write(folder, "pass.json", evidence_report())
            _, result = self.readiness("--harness", "codex", "--harness", "claude", "--evidence", passing)
            communication = result["levels"]["communication"]
            self.assertEqual(communication["status"], "passed")
            self.assertEqual(communication["reports"][0]["pairs"][0]["recipient_harness"], "claude")
            self.assertEqual(len(communication["reports"][0]["sha256"]), 64)
            self.assertEqual(result["levels"]["adapter"]["status"], "passed")
            self.assertEqual(result["passed"][-2:], ["adapter", "communication"])
            # Neighboring verified pairs remain visible; the failed run fails the level.
            failing = self.write(folder, "fail.json", evidence_report(
                "fail", (("alice", "bob", "pass"), ("bob", "carol", "fail"))))
            _, result = self.readiness("--harness", "codex", "--harness", "claude", "--harness", "grok",
                                       "--evidence", passing, "--evidence", failing)
            self.assertEqual(result["levels"]["communication"]["status"], "failed")
            self.assertEqual([pair["status"] for pair in result["levels"]["communication"]["reports"][1]["pairs"]],
                             ["pass", "fail"])
            self.assertEqual(result["levels"]["adapter"]["harnesses"]["grok"]["checks"]["session_identity"],
                             "unverified")
            malformed = {
                "summary claims pass over a failed case": evidence_report("pass", (("alice", "bob", "fail"),)),
                "pass with verifier errors": evidence_report(errors=["native_binding_changed"]),
                "pass without cases": evidence_report(cases=()),
                "fixture evidence mode": evidence_report(evidence_mode="fixture"),
                "unknown participant": evidence_report(cases=(("alice", "mallory", "pass"),)),
                "miscounted cases": evidence_report(required_cases=2),
                "not an object": [],
            }
            for label, value in malformed.items():
                with self.subTest(label):
                    code, error = self.readiness("--evidence", self.write(folder, "bad.json", value))
                    self.assertEqual(code, 2)
                    self.assertIn("operator_native verification report", error)
            code, _ = self.readiness("--evidence", str(Path(folder) / "absent.json"))
            self.assertEqual(code, 2)
            code, error = self.readiness("--harness", "codex", "--harness", "grok", "--binary", "/bin/true")
            self.assertEqual((code, "--binary" in error), (2, True))

    def test_stale_or_unfingerprinted_evidence_cannot_certify_the_current_runtime(self):
        observed = {"codex": probe("codex"), "claude": probe("claude"), "grok": probe("grok")}
        def doctor(name, binary=None):
            return {**observed[name], "binary": binary} if binary else observed[name]
        codex_pair = (("alice", "dave", "pass"),)
        codex_only = [{"name": "alice", "harness": "codex"}, {"name": "dave", "harness": "codex"}]
        stale = {
            # Recorded by a run of other Xordination source than the imported package.
            "stale source": (evidence_report(source_sha256=source_sha256("b" * 64)), (), "source_mismatch"),
            # A report retained before runner_sha256 existed cannot be matched at all.
            "missing source fingerprint": (evidence_report(runner_sha256=None), (), "source_fingerprint_unverifiable"),
            "config digest of another configuration": (evidence_report(config_sha256="c" * 64), (), "config_mismatch"),
            # An old Codex run cannot certify a newly selected executable.
            "different binary": (evidence_report(cases=codex_pair, participants=codex_only),
                                 ("--binary", "/usr/local/bin/codex-next"), "alice: binary_mismatch"),
            "missing binary": (evidence_report(configured_participants=[
                {key: value for key, value in row.items() if key != "binary"}
                for row in evidence_report()["configured_participants"]]), (), "alice: binary_unrecorded"),
            "different version": (evidence_report(configured_participants=[
                {**row, "version": row["harness"] + " 0.9"}
                for row in evidence_report()["configured_participants"]]), (), "bob: version_mismatch"),
        }
        with tempfile.TemporaryDirectory() as folder, \
                patch("xordination.adapters.doctor", side_effect=doctor):
            for label, (value, extra, gap) in stale.items():
                with self.subTest(label):
                    path = self.write(folder, "stale.json", value)
                    harnesses = ("--harness", "codex") if extra else ("--harness", "codex", "--harness", "claude")
                    _, result = self.readiness(*harnesses, *extra, "--evidence", path)
                    communication = result["levels"]["communication"]
                    self.assertEqual(communication["status"], "unverified")
                    self.assertIn(gap, communication["reports"][0]["scope"]["gaps"])
                    self.assertNotIn("communication", result["passed"])
                    self.assertEqual(result["levels"]["adapter"]["harnesses"]["codex"]["checks"]["session_identity"],
                                     "unverified")
            # The same evidence passes against the executable it names, and with every harness probed.
            path = self.write(folder, "current.json", evidence_report(cases=codex_pair, participants=codex_only))
            _, result = self.readiness("--harness", "codex", "--binary", "/opt/codex/bin/codex", "--evidence", path)
            self.assertEqual(result["levels"]["communication"]["status"], "passed")
            path = self.write(folder, "current.json", evidence_report())
            _, result = self.readiness("--harness", "codex", "--evidence", path)
            self.assertEqual(result["levels"]["communication"]["status"], "unverified")
            self.assertIn("bob: harness_not_probed", result["levels"]["communication"]["reports"][0]["scope"]["gaps"])
            # A stale report beside a current one never lets the level pass.
            stale_path = self.write(folder, "stale.json", stale["stale source"][0])
            _, result = self.readiness("--harness", "codex", "--harness", "claude", "--evidence", path,
                                       "--evidence", stale_path)
            self.assertEqual(result["levels"]["communication"]["status"], "unverified")
            self.assertEqual(result["levels"]["adapter"]["harnesses"]["codex"]["checks"]["session_identity"],
                             "passed")


class OperatorView(unittest.TestCase):
    def test_view_summarizes_space_without_credentials_or_creating_state(self):
        with tempfile.TemporaryDirectory() as folder:
            state = str(Path(folder) / "state.sqlite")
            missing = str(Path(folder) / "missing.sqlite")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(2, main(["view", "--state", missing, "--space", "project"]))
            self.assertFalse(Path(missing).exists())
            store = Store(state)
            try:
                store.create_space("project")
                one = store.join("project", "one")
                two = store.join("project", "two")
                store.send(store.authenticate(one["token"]), "two", "private body", "key")
            finally:
                store.close()
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                self.assertEqual(0, main(["view", "--state", state, "--space", "project"]))
            view = json.loads(output.getvalue())
            self.assertEqual(["one", "two"], [entry["participant"] for entry in view["participants"]])
            self.assertEqual({"queued": 1}, view["participants"][1]["states"])
            self.assertEqual(1, view["waiting"]["delivery"])
            for secret in (one["token"], two["token"], "private body"):
                self.assertNotIn(secret, output.getvalue())
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(2, main(["view", "--state", state, "--space", "unknown"]))


if __name__ == "__main__":
    unittest.main()
