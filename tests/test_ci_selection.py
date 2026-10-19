"""CI may skip unrelated contracts only from a complete, understood change."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts" / "ci"
spec = importlib.util.spec_from_file_location("ci_selector", SCRIPTS / "select_checks.py")
selector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(selector)
benchmark_spec = importlib.util.spec_from_file_location("ci_benchmark_compare", SCRIPTS / "benchmark_compare.py")
benchmark = importlib.util.module_from_spec(benchmark_spec)
benchmark_spec.loader.exec_module(benchmark)


class SelectionTests(unittest.TestCase):
    def test_dependency_groups_and_packaging_inputs(self):
        cases = (
            (["src/xordination/mcp.py"], {"adapters", "mcp", "cli", "end_to_end", "behavior", "live_acceptance", "live_native_busy", "live_tmux"}, True, True),
            (["src/xordination/cli.py"], {"adapters", "mcp", "server", "cli", "end_to_end", "behavior", "live_acceptance", "live_native_busy", "live_tmux"}, True, True),
            (["src/xordination/rpc.py"], {"adapters", "behavior", "live_acceptance", "live_native_busy", "live_tmux"}, True, True),
            (["tests/test_store.py"], {"store"}, True, False),
            (["docs/ci.md"], set(), True, False),
            (["AGENTS.md"], set(), True, False),
            (["README.md"], set(), True, True),
            ([".dockerignore"], set(), False, True),
            (["MANIFEST.in"], set(selector.GROUPS), True, False),
            (["src/xordination/rpc.py", "tests/test_mcp.py"], {"adapters", "mcp", "behavior", "live_acceptance", "live_native_busy", "live_tmux"}, True, True),
            (["tests/test_behavior.py"], {"behavior"}, True, False),
            (["tests/test_performance.py"], {"performance"}, True, False),
            (["scripts/testing/benchmark.py"], {"performance"}, True, False),
            (["scripts/testing/live.py"], {"live_acceptance", "live_native_busy", "live_tmux"}, True, False),
            (["scripts/testing/live_tmux.py"], {"live_tmux"}, True, False),
            (["tests/test_live_tmux.py"], {"live_tmux"}, True, False),
            (["tests/test_live_native_busy.py"], {"live_native_busy"}, True, False),
            (["tests/live_native_fixture.py"], {"live_native_busy"}, True, False),
        )
        for paths, groups, package, image in cases:
            with self.subTest(paths=paths):
                result = selector.classify_paths(paths)
                self.assertEqual(set(result["groups"]), groups)
                self.assertEqual(result["package"], package)
                self.assertEqual(result["image"], image)
                self.assertEqual(result["benchmark"], "performance" in groups)

    def test_unknown_shared_and_ci_inputs_require_full(self):
        for path in ("unknown.txt", "tests/helpers.py", "tests/test_security.py", "src/xordination/new.py",
                     "scripts/ci/select_checks.py", ".github/workflows/ci.yml", "docs/generated.json",
                     "scripts/testing/unknown.py"):
            with self.subTest(path=path):
                self.assertEqual(selector.classify_paths([path])["groups"], list(selector.GROUPS))
                self.assertTrue(selector.classify_paths([path])["image"])

    def test_nul_parser_keeps_both_rename_paths_and_deletions(self):
        wire = b"R100\0src/xordination/mcp.py\0docs/old name\n.md\0D\0tests/test_cli.py\0"
        self.assertEqual(selector.parse_name_status(wire),
                         ["src/xordination/mcp.py", "docs/old name\n.md", "tests/test_cli.py"])
        for malformed in (b"M\0file", b"R100\0only-one\0", b"U\0file\0", b"M\0\0"):
            with self.subTest(wire=malformed), self.assertRaises(ValueError):
                selector.parse_name_status(malformed)

    def test_missing_base_manual_payload_and_git_errors_default_full(self):
        for event, payload, head in (("push", {"before": "0" * 40}, "a" * 40),
                                     ("push", {}, "a" * 40), ("workflow_dispatch", {}, None)):
            self.assertTrue(selector.select_change(ROOT, event, payload, head)["full"])
        with patch.object(selector.subprocess, "run", side_effect=OSError("unavailable")):
            self.assertTrue(selector.select_change(ROOT, "push", {"before": "b" * 40}, "a" * 40)["full"])
        with patch.object(selector, "changed_paths", side_effect=ValueError("malformed diff")):
            self.assertTrue(selector.select_change(ROOT, "push", {}, None)["full"])

    def test_real_complete_multicommit_rename_and_delete_range(self):
        with tempfile.TemporaryDirectory() as folder:
            repo = Path(folder)
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=repo, stderr=subprocess.PIPE, text=True).strip()
            git("init", "-q")
            git("config", "user.name", "xormania")
            git("config", "user.email", "xormania@example.invalid")
            (repo / "src/xordination").mkdir(parents=True)
            source = repo / "src/xordination/mcp.py"
            source.write_text("old source\n")
            (repo / "tests").mkdir()
            (repo / "tests/test_cli.py").write_text("old test\n")
            git("add", ".")
            git("commit", "-qm", "base")
            base = git("rev-parse", "HEAD")
            (repo / "docs").mkdir()
            git("mv", "src/xordination/mcp.py", "docs/moved.md")
            git("commit", "-qm", "rename runtime")
            git("rm", "tests/test_cli.py")
            git("commit", "-qm", "delete test")
            head = git("rev-parse", "HEAD")
            result = selector.select_change(repo, "pull_request", {"pull_request": {"base": {"sha": base}}}, head)
            self.assertEqual(set(result["paths"]), {"src/xordination/mcp.py", "docs/moved.md", "tests/test_cli.py"})
            self.assertEqual(set(result["groups"]), {"adapters", "mcp", "cli", "end_to_end", "behavior", "live_acceptance", "live_native_busy", "live_tmux"})
            self.assertTrue(result["image"])
            self.assertTrue(selector.select_change(repo, "push", {"before": "f" * 40}, head)["full"])

    def run_fixture_suite(self, groups, *, missing=None, empty=None, failing_unknown=False, replacement=None):
        with tempfile.TemporaryDirectory() as folder:
            repo = Path(folder)
            (repo / "tests").mkdir()
            for group in selector.GROUPS:
                if group != missing:
                    (repo / "tests" / ("test_" + group + ".py")).write_text(
                        "" if group == empty else
                        "import unittest\nclass Contract(unittest.TestCase):\n    def test_case(self): pass\n")
            if failing_unknown:
                (repo / "tests/test_new_security.py").write_text(
                    "import unittest\nclass NewContract(unittest.TestCase):\n    def test_new(self): self.fail('NEW_CONTRACT_EXECUTED')\n")
            if replacement:
                (repo / "tests/test_mcp.py").write_text(replacement)
            selection = repo / "selection.json"
            selection.write_text(json.dumps({"groups": list(groups)}))
            report = repo / "execution.json"
            result = subprocess.run([sys.executable, str(SCRIPTS / "run_contracts.py"), "--selection", str(selection),
                                     "--report", str(report)], cwd=repo, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True, timeout=20)
            result.census = json.loads(report.read_text()) if report.is_file() else None
            return result

    def test_full_runner_executes_unknown_failing_test(self):
        result = self.run_fixture_suite(selector.GROUPS, failing_unknown=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("NEW_CONTRACT_EXECUTED", result.stderr)
        self.assertIn("test_new_security.NewContract", result.stderr)

    def test_absent_or_empty_selected_modules_fail(self):
        for groups in (("mcp",), selector.GROUPS):
            for kind in ("missing", "empty"):
                with self.subTest(groups=groups, kind=kind):
                    result = self.run_fixture_suite(groups, **{kind: "mcp"})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("Selected test module", result.stderr)

    def test_skipped_required_contract_is_fatal_and_census_separates_execution(self):
        result = self.run_fixture_suite(("mcp",), replacement=
            "import unittest\nclass Contract(unittest.TestCase):\n"
            "    @unittest.skip('required native fixture missing')\n"
            "    def test_case(self): pass\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.census["collected"], 1)
        self.assertEqual(result.census["started"], 1)
        self.assertEqual(result.census["executed"], 0)
        self.assertEqual(result.census["skipped"][0]["reason"], "required native fixture missing")
        self.assertEqual(result.census["not_run"], [])
        self.assertFalse(result.census["required_exercised"])

    def test_class_setup_failure_records_unexercised_required_contracts(self):
        result = self.run_fixture_suite(("mcp",), replacement=
            "import unittest\nclass Contract(unittest.TestCase):\n"
            "    @classmethod\n"
            "    def setUpClass(cls): raise RuntimeError('fixture failed before case')\n"
            "    def test_first(self): pass\n"
            "    def test_second(self): pass\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.census["collected"], 2)
        self.assertEqual(result.census["executed"], 0)
        self.assertEqual(result.census["skipped"], [])
        self.assertEqual(len(result.census["not_run"]), 2)
        self.assertEqual(len(result.census["errors"]), 1)

    def test_expected_failure_cannot_authorize_required_contract_pass(self):
        result = self.run_fixture_suite(("mcp",), replacement=
            "import unittest\nclass Contract(unittest.TestCase):\n"
            "    @unittest.expectedFailure\n"
            "    def test_case(self): self.fail('contract regressed')\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.census["executed"], 1)
        self.assertEqual(len(result.census["expected_failures"]), 1)

    def test_broker_performance_dependencies_select_measurement_and_behavior(self):
        for source in ("store.py", "client.py", "server.py"):
            with self.subTest(source=source):
                selected = selector.classify_paths(["src/xordination/" + source])
                self.assertTrue(selected["benchmark"])
                self.assertIn("performance", selected["groups"])
                self.assertIn("behavior", selected["groups"])
        self.assertFalse(selector.classify_paths(["docs/testing.md"])["benchmark"])

    def test_manual_benchmark_bounds_and_baseline_identity(self):
        self.assertEqual(benchmark.benchmark_config("push", {"inputs": {"benchmark_messages": "9999"}}),
                         benchmark.DEFAULT_CONFIG)
        self.assertEqual(benchmark.benchmark_config("workflow_dispatch", {"inputs": {
            "benchmark_messages": "16", "benchmark_trials": "4", "benchmark_workers": "2"}}),
                         {"messages": 16, "trials": 4, "workers": 2})
        for name, value in (("messages", "129"), ("trials", "1"), ("workers", True),
                            ("messages", "1.5"), ("workers", "0")):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                benchmark.benchmark_config("workflow_dispatch", {"inputs": {"benchmark_" + name: value}})
        self.assertEqual(benchmark.baseline_ref("pull_request", {"pull_request": {"base": {"sha": "a" * 40}}}),
                         "a" * 40)
        self.assertIsNone(benchmark.baseline_ref("push", {"before": "0" * 40}))
        with self.assertRaises(ValueError):
            benchmark.baseline_ref("workflow_dispatch", {"inputs": {"benchmark_base": "master"}})

    def test_baseline_extraction_reads_product_only_and_refuses_symlinks(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            repo = root / "repo"
            repo.mkdir()
            def git(*args):
                return subprocess.check_output(["git", *args], cwd=repo, stderr=subprocess.PIPE, text=True).strip()
            git("init", "-q")
            git("config", "user.name", "xormania")
            git("config", "user.email", "xormania@example.invalid")
            package = repo / "src/xordination"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("BASE_PRODUCT_BYTES = True\n")
            (repo / "scripts").mkdir()
            (repo / "scripts/benchmark.py").write_text("raise RuntimeError('BASE_DRIVER_MUST_NOT_RUN')\n")
            git("add", ".")
            git("commit", "-qm", "base source and different driver")
            revision = git("rev-parse", "HEAD")
            extracted = benchmark.extract_source(repo, revision, root / "extracted")
            self.assertEqual((extracted / "xordination/__init__.py").read_text(), "BASE_PRODUCT_BYTES = True\n")
            self.assertFalse((root / "extracted/scripts").exists())
            os.symlink("../../scripts/benchmark.py", package / "escaped.py")
            git("add", ".")
            git("commit", "-qm", "symlink input")
            with self.assertRaisesRegex(ValueError, "regular product files"):
                benchmark.extract_source(repo, git("rev-parse", "HEAD"), root / "unsafe")

    def test_failed_baseline_is_retained_and_cannot_block_required_head_measurement(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            event = root / "event.json"
            event.write_text(json.dumps({"pull_request": {"base": {"sha": "a" * 40}}}))
            measured = []
            def measure(source, output, config, env, session_id):
                measured.append((source, output.name, session_id))
                output.write_text(json.dumps({"status": "failed" if output.name == "base.json" else "verified"}))
                if output.name == "base.json":
                    raise subprocess.CalledProcessError(1, ["benchmark"])
            with patch.object(benchmark, "ROOT", root), \
                    patch.object(benchmark, "extract_source", return_value=root / "base/src"), \
                    patch.object(benchmark, "run_measurement", side_effect=measure):
                self.assertEqual(benchmark.main(["--event", "pull_request", "--event-file", str(event),
                                                 "--output", str(root / "reports")]), 0)
            self.assertEqual([name for _, name, _ in measured], ["base.json", "head.json"])
            self.assertEqual(measured[0][2], measured[1][2])
            context = json.loads((root / "reports/ci-context.json").read_text())
            self.assertEqual(context["baseline_measurement"], "failed")
            self.assertEqual(context["measurement"], "passed")
            self.assertEqual(context["comparison"], "unavailable")
            self.assertEqual(json.loads((root / "reports/base.json").read_text())["status"], "failed")

    def test_head_correctness_failure_remains_fatal_and_retains_context(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch.object(benchmark, "ROOT", root), patch.object(benchmark, "run_measurement",
                    side_effect=subprocess.CalledProcessError(1, ["head-benchmark"])):
                with self.assertRaises(subprocess.CalledProcessError):
                    benchmark.main(["--event", "workflow_dispatch", "--output", str(root / "reports")])
            context = json.loads((root / "reports/ci-context.json").read_text())
            self.assertEqual(context["measurement"], "failed")
            self.assertEqual(context["comparison"], "unavailable")
            self.assertEqual(context["baseline_unavailable_reason"], "No baseline commit supplied")


if __name__ == "__main__":
    unittest.main()
