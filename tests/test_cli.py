import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from helpers import isolated_process_env, observe_fsync, owned_broker
from xordination.cli import main
from xordination.store import Store


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
