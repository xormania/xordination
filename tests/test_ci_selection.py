"""CI may skip unrelated contracts only from a complete, understood change."""
import importlib.util
import json
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


class SelectionTests(unittest.TestCase):
    def test_dependency_groups_and_packaging_inputs(self):
        cases = (
            (["src/xordination/mcp.py"], {"adapters", "mcp", "cli", "end_to_end"}, True, True),
            (["src/xordination/cli.py"], {"adapters", "mcp", "server", "cli", "end_to_end"}, True, True),
            (["src/xordination/rpc.py"], {"adapters"}, True, True),
            (["tests/test_store.py"], {"store"}, True, False),
            (["docs/ci.md"], set(), True, False),
            (["AGENTS.md"], set(), True, False),
            (["README.md"], set(), True, True),
            ([".dockerignore"], set(), False, True),
            (["MANIFEST.in"], set(selector.GROUPS), True, False),
            (["src/xordination/rpc.py", "tests/test_mcp.py"], {"adapters", "mcp"}, True, True),
        )
        for paths, groups, package, image in cases:
            with self.subTest(paths=paths):
                result = selector.classify_paths(paths)
                self.assertEqual(set(result["groups"]), groups)
                self.assertEqual(result["package"], package)
                self.assertEqual(result["image"], image)

    def test_unknown_shared_and_ci_inputs_require_full(self):
        for path in ("unknown.txt", "tests/helpers.py", "tests/test_security.py", "src/xordination/new.py",
                     "scripts/ci/select_checks.py", ".github/workflows/ci.yml", "docs/generated.json"):
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
            self.assertEqual(set(result["groups"]), {"adapters", "mcp", "cli", "end_to_end"})
            self.assertTrue(result["image"])
            self.assertTrue(selector.select_change(repo, "push", {"before": "f" * 40}, head)["full"])

    def run_fixture_suite(self, groups, *, missing=None, empty=None, failing_unknown=False):
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
            selection = repo / "selection.json"
            selection.write_text(json.dumps({"groups": list(groups)}))
            return subprocess.run([sys.executable, str(SCRIPTS / "run_contracts.py"), "--selection", str(selection)],
                                  cwd=repo, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=20)

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


if __name__ == "__main__":
    unittest.main()
