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

from helpers import observe_fsync, owned_broker
from xordination.adapters import _mcp_args
from xordination.cli import main
from xordination.store import Store
from xordination.trace import Trace


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
            self.assertEqual("XORDINATION_TOKEN", records[3]["frame"]["params"]["env"][0]["name"])
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
