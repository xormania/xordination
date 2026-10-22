import contextlib
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


if __name__ == "__main__":
    unittest.main()
