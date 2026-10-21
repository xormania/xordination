import contextlib
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

from helpers import observe_fsync, owned_broker
import xordination
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


def evidence_report(status="pass", cases=(("alice", "bob", "pass"),), **overrides):
    cases = [{"case_id": str(index), "sender": sender, "recipient": recipient, "scenario": "idle",
              "status": case_status, "phases": {}, "failures": []}
             for index, (sender, recipient, case_status) in enumerate(cases)]
    report = {"schema_version": 1, "run_id": "run", "evidence_mode": "operator_native",
              "config_sha256": "c", "source_sha256": "s", "status": status,
              "required_cases": len(cases), "passed_cases": sum(case["status"] == "pass" for case in cases),
              "cases": cases, "errors": [], "native_artifacts": [],
              "configured_participants": [{"name": "alice", "harness": "codex"},
                                          {"name": "bob", "harness": "claude"},
                                          {"name": "carol", "harness": "grok"}]}
    report.update(overrides)
    return report


class FakeDistribution:
    """An installation whose files are the imported package itself."""
    version = xordination.__version__

    def __init__(self, entry_value="xordination.cli:main"):
        self.entry_points = [importlib.metadata.EntryPoint("xordination", entry_value, "console_scripts")]

    def locate_file(self, name):
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
        which = patch("shutil.which", return_value="/opt/bin/xordination")
        with tempfile.TemporaryDirectory() as folder, which:
            state = str(Path(folder) / "state.sqlite3")
            with patch("importlib.metadata.distribution", return_value=FakeDistribution()):
                _, result = self.readiness("--state", state)
                package = result["levels"]["package"]
                self.assertEqual(package["status"], "passed")
                self.assertEqual(package["entry_point"], "/opt/bin/xordination")
                self.assertEqual(result["passed"], ["package"])
                _, result = self.readiness("--state", str(Path(folder) / "missing" / "state.sqlite3"))
                self.assertEqual(result["levels"]["package"]["checks"]["state_location"], "failed")
                _, result = self.readiness()
                self.assertEqual(result["levels"]["package"]["status"], "unverified")
            cases = {
                "checkout without an installation": dict(side_effect=importlib.metadata.PackageNotFoundError),
                "wrong entry point": dict(return_value=FakeDistribution("other:main")),
            }
            for label, options in cases.items():
                with self.subTest(label), patch("importlib.metadata.distribution", **options):
                    _, result = self.readiness("--state", state)
                    self.assertEqual(result["levels"]["package"]["status"], "failed")

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
        observed = {"codex": {"harness": "codex", "status": "supported"},
                    "claude": {"harness": "claude", "status": "supported"},
                    "grok": {"harness": "grok", "status": "unavailable"}}
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
            _, result = self.readiness("--harness", "grok", "--evidence", passing, "--evidence", failing)
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


if __name__ == "__main__":
    unittest.main()
