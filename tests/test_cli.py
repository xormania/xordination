import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from helpers import observe_fsync
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


if __name__ == "__main__":
    unittest.main()
