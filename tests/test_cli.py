import contextlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

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
            original_fsync = os.fsync
            reached_directory = []

            def fail_directory(descriptor_fd):
                info = os.fstat(descriptor_fd)
                if (stat.S_ISDIR(info.st_mode) and descriptor.parent.exists()
                        and (info.st_dev, info.st_ino) ==
                        (descriptor.parent.stat().st_dev, descriptor.parent.stat().st_ino)):
                    reached_directory.append(True)
                    raise OSError("directory sync unavailable")
                original_fsync(descriptor_fd)

            try:
                with patch("xordination.cli.os.fsync", side_effect=fail_directory):
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
            original_fsync = os.fsync
            synced = set()

            def observe_sync(descriptor_fd):
                info = os.fstat(descriptor_fd)
                if stat.S_ISDIR(info.st_mode) and descriptor.exists():
                    synced.add((info.st_dev, info.st_ino))
                original_fsync(descriptor_fd)

            try:
                with patch("xordination.cli.os.fsync", side_effect=observe_sync):
                    result = main(["join", "--state", state, "--space", "project",
                                   "--participant", "one", "--replace", "--output", str(descriptor)])
                self.assertEqual(result, 0)
                for directory in (descriptor.parent, descriptor.parent.parent, Path(folder)):
                    info = directory.stat()
                    self.assertIn((info.st_dev, info.st_ino), synced)
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
                original_fsync = os.fsync
                failed_descriptor = []

                def fail_descriptor(descriptor_fd):
                    info = os.fstat(descriptor_fd)
                    if (descriptor.exists() and (info.st_dev, info.st_ino) ==
                            (descriptor.stat().st_dev, descriptor.stat().st_ino)):
                        failed_descriptor.append(True)
                        raise OSError("disk unavailable")
                    original_fsync(descriptor_fd)

                with patch("xordination.cli.os.fsync", side_effect=fail_descriptor):
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(main(args), 2)
                self.assertTrue(failed_descriptor)
                self.assertFalse(descriptor.exists())
                self.assertEqual(store.authenticate(original["token"])["generation"], original["generation"])
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
