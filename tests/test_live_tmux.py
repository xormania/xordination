"""Managed launcher orchestration with a real broker and scripted natives; no tmux, vendor or model."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from xordination.store import Store


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "testing" / "live_tmux.py"
spec = importlib.util.spec_from_file_location("live_tmux", SCRIPT)
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)

# Owned stand-in for a native session: binds, observes its own tool inputs through
# the live observer, then acknowledges and replies through the public broker API.
SCRIPTED_NATIVE = r'''
import json, sys, time
from pathlib import Path
root, job, name = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
sys.path[:0] = [str(root / "src"), str(root / "scripts" / "testing")]
import live
from xordination.client import Client
run = job / "run"
record = json.loads((job / "session.json").read_text())
client = Client(record["endpoint"], live.load(run / (name + ".connection.json"))["token"])
native = "scripted-native-" + name
client.call("bind", native_id=native, capabilities={"transport": "launcher-fixture"})
calls = 0
def observe(tool, arguments):
    global calls
    calls += 1
    live.record_native(run, name, native, native + ":" + str(calls), tool, arguments, "codex/item/tool/call")
def acknowledge(message):
    observe("xordination_ack", {"message_id": message["id"]})
    client.call("ack", message_id=message["id"])
def send(arguments):
    observe("xordination_send", arguments)
    client.call("send", **arguments)
print("scripted native ready", flush=True)
while True:
    claim = client.call("claim")
    message = claim["message"]
    if not message:
        time.sleep(.05)
        continue
    client.call("finish", message_id=message["id"], attempt_id=claim["attempt_id"], outcome="submitted")
    try:
        body = json.loads(message["body"])
    except ValueError:
        body = {}  # Acceptance replies are plain text.
    acknowledge(message)
    if isinstance(body.get("tasks"), list):
        for task in body["tasks"]:
            send(task["arguments"])
    elif "reply" in body and not message["reply_to"]:
        send({"to": message["from"], "to_generation": message["from_generation"], "body": body["reply"],
              "reply_to": message["id"], "key": "reply-" + message["id"]})
'''


class ProcessTmux:
    """Implements the launcher's tmux surface with owned subprocesses and output files."""

    def __init__(self, folder):
        self.folder = Path(folder)
        self.owner = None
        self.panes = {}
        self.selected = []

    def owned(self, record):
        return self.owner == record["owner"]

    def create(self, record):
        self.owner = record["owner"]
        return "%dashboard"

    def launch(self, record, name, argv):
        if not self.owned(record):
            raise RuntimeError("not owned")
        output = (self.folder / (name + ".out")).open("w")
        process = subprocess.Popen(list(map(str, argv)), stdout=output, stderr=subprocess.STDOUT, cwd=ROOT)
        pane = "%" + name
        self.panes[pane] = (process, output)
        return pane

    def select(self, record, pane):
        self.selected.append(pane)

    def pane_owned(self, record, pane):
        return self.owned(record) and pane in self.panes

    def capture(self, pane):
        return (self.folder / (pane[1:] + ".out")).read_text()

    def kill(self, pane):
        process, output = self.panes.pop(pane)
        process.terminate()
        process.wait(10)
        output.close()

    def close(self, record):
        for pane in list(self.panes):
            self.kill(pane)
        self.owner = None


def supported(harness):
    return {"harness": harness, "status": "supported", "version": {"value": "fixture " + harness + " 1.0"}}


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.tmux = ProcessTmux(self.folder)
        self.addCleanup(lambda: self.tmux.close({"owner": self.tmux.owner}))

    def config(self, participants, **extra):
        path = self.folder / "config.json"
        path.write_text(json.dumps({"participants": participants, **extra}))
        return path

    def job(self, participants, doctor=supported, **timeouts):
        record = launcher.prepare_job(self.config(participants), self.folder / "job", doctor=doctor, **timeouts)
        self.tmux.create(record)  # As start does before respawning the dashboard as supervisor.
        return Path(record["directory"])

    def command(self, job, role):
        if role == "broker":
            return launcher.pane_command(job, role)
        return [sys.executable, "-c", SCRIPTED_NATIVE, ROOT, job, role]

    def test_supervised_pairs_pass_and_only_owned_windows_close(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}],
                       startup_timeout=30, reply_timeout=30)
        config = json.loads((job / "config.json").read_text())
        self.assertEqual(config["participants"][0]["version"], "fixture codex 1.0")
        processes = []
        launch = self.tmux.launch
        self.tmux.launch = lambda record, name, argv: processes.append(name) or launch(record, name, argv)
        self.assertEqual(launcher.supervise(job, self.tmux, command=self.command, poll=.05), 0)
        self.assertEqual(processes, ["broker", "alpha", "beta"])
        summary = json.loads((job / "summary.json").read_text())
        self.assertEqual((summary["status"], summary["passed_cases"], summary["required_cases"]), ("pass", 2, 2))
        record = json.loads((job / "session.json").read_text())
        self.assertEqual(record["state"], "finished")
        self.assertEqual([item["window"] for item in record["cleanup"]], ["alpha", "beta", "broker"])
        self.assertTrue(all(item["status"] == "closed" for item in record["cleanup"]))
        self.assertEqual(self.tmux.panes, {})
        self.assertEqual(len(list((job / "run").glob("report-*.json"))), 1)
        tokens = [json.loads(path.read_text())["token"] for path in (job / "run").glob("*.connection.json")]
        for log in (job / "logs").glob("*-terminal.log"):
            self.assertFalse(any(token in log.read_text() for token in tokens))
        self.assertIn("scripted native ready", (job / "logs" / "alpha-terminal.log").read_text())

    def test_missing_native_readiness_stops_before_capture_or_dispatch(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}],
                       startup_timeout=1)
        idle = lambda job, role: (launcher.pane_command(job, role) if role == "broker" else
                                  [sys.executable, "-c", "import time; time.sleep(60)"])
        self.assertEqual(launcher.supervise(job, self.tmux, command=idle, poll=.05), 1)
        self.assertFalse((job / "run" / "frozen.json").exists())
        self.assertFalse((job / "dispatch.json").exists())
        summary = json.loads((job / "summary.json").read_text())
        self.assertEqual(summary["status"], "incomplete")
        self.assertIn("startup timeout", summary["error"])
        self.assertEqual(self.tmux.panes, {})

    def test_stop_request_finalizes_without_dispatch(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}])
        (job / "stop.request").touch()
        self.assertEqual(launcher.supervise(job, self.tmux, command=self.command, poll=.05), 1)
        record = json.loads((job / "session.json").read_text())
        self.assertEqual(record["state"], "stopped")
        self.assertFalse((job / "dispatch.json").exists())
        self.assertEqual(self.tmux.panes, {})

    def test_preflight_gap_refuses_before_preparing_or_launching(self):
        observed = lambda harness: ({"harness": harness, "status": "unavailable"} if harness == "grok"
                                    else supported(harness))
        with self.assertRaisesRegex(ValueError, "grok: unavailable"):
            self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "grok"}], doctor=observed)
        job = self.folder / "job"
        self.assertFalse((job / "run").exists())
        self.assertEqual(json.loads((job / "session.json").read_text())["state"], "error")
        self.assertIsNone(self.tmux.owner)

    def test_reserved_window_names_are_refused(self):
        with self.assertRaisesRegex(ValueError, "reserved"):
            self.job([{"name": "broker", "harness": "codex"}, {"name": "beta", "harness": "codex"}])

    def test_claude_hook_is_additive_run_local_and_outlasts_the_hold(self):
        job = self.job([{"name": "alpha", "harness": "claude"}, {"name": "beta", "harness": "codex"}],
                       phase_timeout=45)
        settings = json.loads((job / "workspaces" / "alpha" / ".claude" / "settings.local.json").read_text())
        self.assertEqual(set(settings["hooks"]), {"PreToolUse"})
        entry = settings["hooks"]["PreToolUse"][0]
        self.assertEqual(entry["matcher"], "mcp__xordination__.*")
        hook = entry["hooks"][0]
        self.assertIn("observe-claude --run " + str(job / "run") + " --participant alpha", hook["command"])
        self.assertGreater(hook["timeout"], 2 * 45)
        self.assertFalse((job / "workspaces" / "beta" / ".claude").exists())

    def test_claude_readiness_waits_for_channel_registration(self):
        job = self.job([{"name": "alpha", "harness": "claude"}, {"name": "beta", "harness": "codex"}])
        run = job / "run"
        self.assertEqual(launcher.missing_readiness(run), ["alpha: native binding", "beta: native binding"])
        store = Store(run / "broker.sqlite3")
        self.addCleanup(store.close)
        contexts = {name: store.authenticate(json.loads((run / (name + ".connection.json")).read_text())["token"])
                    for name in ("alpha", "beta")}
        store.bind(contexts["alpha"], "native-alpha", capabilities={"channel_registered": False})
        store.bind(contexts["beta"], "native-beta")
        self.assertEqual(launcher.missing_readiness(run), ["alpha: channel registration"])
        store.bind(store.authenticate(json.loads((run / "alpha.connection.json").read_text())["token"]),
                   "native-alpha", capabilities={"channel_registered": True})
        self.assertEqual(launcher.missing_readiness(run), [])


if __name__ == "__main__":
    unittest.main()
