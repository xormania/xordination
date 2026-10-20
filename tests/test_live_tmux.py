"""Managed launcher orchestration with a real broker and scripted natives; no tmux, vendor or model."""
import importlib.util
import json
from pathlib import Path
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from helpers import owned_broker
from xordination.store import Store


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "testing" / "live_tmux.py"
spec = importlib.util.spec_from_file_location("live_tmux", SCRIPT)
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)

# Owned stand-in for a native session: binds, observes its own tool inputs through
# the live observer, then acknowledges and replies through the public broker API.
SCRIPTED_NATIVE = r'''
import json, os, sys, time
from pathlib import Path
root, job, name = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
reply_delay = float(os.environ.get("XORDINATION_FIXTURE_REPLY_DELAY", "0"))  # Simulated model latency.
sys.path[:0] = [str(root / "src"), str(root / "scripts" / "testing")]
import live
from xordination.client import Client
run = job / "run"
record = json.loads((job / "session.json").read_text())
token = live.load(run / (name + ".connection.json"))["token"]
print("diagnostic token=" + token, flush=True)  # Must be redacted from the captured pane log.
client = Client(record["endpoint"], token)
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
            if task["tool"] == "xordination_send":
                send(task["arguments"])  # No native wait hold: busy readiness never arrives.
    elif "reply" in body and not message["reply_to"]:
        time.sleep(reply_delay)
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
        self.dashboard_alive = True

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

    def pane_dead(self, record, pane):
        if pane == "%dashboard":
            return not (self.owned(record) and self.dashboard_alive)
        return not self.pane_owned(record, pane) or self.panes[pane][0].poll() is not None

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
        # No probe-then-bind gap: the broker bound port 0 and reported the result privately.
        bound = json.loads((job / "broker.json").read_text())
        self.assertGreater(bound["port"], 0)
        self.assertEqual((record["port"], record["endpoint"]), (bound["port"], "http://127.0.0.1:" + str(bound["port"])))
        self.assertEqual(oct((job / "broker.json").stat().st_mode & 0o777), "0o600")
        self.assertEqual([item["window"] for item in record["cleanup"]], ["alpha", "beta", "broker"])
        self.assertTrue(all(item["status"] == "closed" for item in record["cleanup"]))
        self.assertEqual(self.tmux.panes, {})
        self.assertEqual(len(list((job / "run").glob("report-*.json"))), 1)
        tokens = [json.loads(path.read_text())["token"] for path in (job / "run").glob("*.connection.json")]
        for log in (job / "logs").glob("*-terminal.log"):
            self.assertFalse(any(token in log.read_text() for token in tokens))
        alpha = (job / "logs" / "alpha-terminal.log").read_text()
        self.assertIn("scripted native ready", alpha)
        self.assertIn("diagnostic token=[redacted]", alpha)
        self.assertEqual(list((job / "environment").iterdir()), [])

    def test_missing_native_readiness_stops_before_capture_or_dispatch(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}],
                       startup_timeout=1)
        def idle(job, role):
            return (launcher.pane_command(job, role) if role == "broker" else
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

    def busy_job(self, **timeouts):
        path = self.folder / "config.json"
        path.write_text(json.dumps({"participants": [{"name": "alpha", "harness": "codex"},
                                                     {"name": "beta", "harness": "codex"}],
                                    "scenarios": ["idle", "busy"]}))
        record = launcher.prepare_job(path, self.folder / "job", doctor=supported, **timeouts)
        self.tmux.create(record)
        return Path(record["directory"])

    def test_failed_dispatch_finalizes_without_waiting_for_replies(self):
        job = self.busy_job(phase_timeout=1, startup_timeout=30, reply_timeout=120)
        started = time.monotonic()
        self.assertEqual(launcher.supervise(job, self.tmux, command=self.command, poll=.05), 1)
        self.assertLess(time.monotonic() - started, 60)  # Well inside the reply timeout.
        self.assertEqual(json.loads((job / "dispatch.json").read_text())["status"], "fail")
        record = json.loads((job / "session.json").read_text())
        self.assertTrue(record["error"].startswith("Dispatch failed:"), record["error"])
        self.assertNotEqual(record["state"], "awaiting_replies")
        summary = json.loads((job / "summary.json").read_text())
        self.assertEqual(summary["status"], "fail")
        self.assertEqual(sum(case["scenario"] == "idle" for case in summary["failed"]), 0)  # Idle evidence kept.
        self.assertEqual(self.tmux.panes, {})

    def test_stop_during_dispatch_cancels_the_held_phase(self):
        job = self.busy_job(phase_timeout=60, startup_timeout=30, reply_timeout=120)
        def stop_once_armed():
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not list((job / "run" / "gates").glob("*/armed.json")):
                time.sleep(.05)
            (job / "stop.request").touch()
        threading.Thread(target=stop_once_armed, daemon=True).start()
        started = time.monotonic()
        self.assertEqual(launcher.supervise(job, self.tmux, command=self.command, poll=.05), 1)
        self.assertLess(time.monotonic() - started, 45)  # Not the 60-second phase wait.
        record = json.loads((job / "session.json").read_text())
        self.assertEqual(record["state"], "stopped")
        failed = [json.loads(path.read_text()) for path in (job / "run" / "gates").glob("*/failed.json")]
        self.assertEqual([item["reason"] for item in failed], ["operator_stopped"])
        self.assertTrue(all((path.parent / "released.json").exists()
                            for path in (job / "run" / "gates").glob("*/armed.json")))
        self.assertEqual(len(list((job / "run").glob("report-*.json"))), 1)
        self.assertEqual(self.tmux.panes, {})

    def test_interrupt_during_busy_dispatch_is_an_operator_stop(self):
        job = self.busy_job(phase_timeout=60, startup_timeout=30, reply_timeout=120)
        def interrupt_once_armed():
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not list((job / "run" / "gates").glob("*/armed.json")):
                time.sleep(.05)
            os.kill(os.getpid(), signal.SIGINT)
        threading.Thread(target=interrupt_once_armed, daemon=True).start()
        started = time.monotonic()
        self.assertEqual(launcher.supervise(job, self.tmux, command=self.command, poll=.05), 1)
        self.assertLess(time.monotonic() - started, 45)  # Not a further 60-second busy phase.
        record = json.loads((job / "session.json").read_text())
        self.assertEqual((record["state"], record.get("stopped")), ("stopped", True))
        self.assertIn("SIGINT", record["error"])
        failed = [json.loads(path.read_text())["reason"] for path in (job / "run" / "gates").glob("*/failed.json")]
        self.assertEqual(failed, ["operator_stopped"])  # One armed gate; no further case was primed.
        self.assertEqual(len(list((job / "run" / "gates").glob("*/armed.json"))), 1)
        self.assertEqual(self.tmux.panes, {})

    def test_idle_round_trips_use_the_reply_budget_not_the_busy_phase(self):
        job = self.busy_job(phase_timeout=1, startup_timeout=30, reply_timeout=60)
        with patch.dict(os.environ, {"XORDINATION_FIXTURE_REPLY_DELAY": "1.5"}):
            self.assertEqual(launcher.supervise(job, self.tmux, command=self.command, poll=.05), 1)
        self.assertFalse((job / "run" / "idle-failed.json").exists())
        self.assertTrue((job / "run" / "idle-complete.json").exists())
        summary = json.loads((job / "summary.json").read_text())
        self.assertEqual([case["scenario"] for case in summary["failed"]], ["busy", "busy"])  # Idle passed.

    def test_termination_signal_finalizes_and_restores_handlers(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}],
                       startup_timeout=60)
        def idle(job, role):
            return (launcher.pane_command(job, role) if role == "broker" else
                    [sys.executable, "-c", "import time; time.sleep(60)"])
        before = signal.getsignal(signal.SIGTERM)
        timer = threading.Timer(2, os.kill, (os.getpid(), signal.SIGTERM))
        timer.start()
        self.addCleanup(timer.cancel)
        self.assertEqual(launcher.supervise(job, self.tmux, command=idle, poll=.05), 1)
        record = json.loads((job / "session.json").read_text())
        self.assertEqual(record["state"], "stopped")
        self.assertIn("SIGTERM", record["error"])
        self.assertEqual(self.tmux.panes, {})
        self.assertIs(signal.getsignal(signal.SIGTERM), before)

    def test_stop_finalizes_when_the_supervisor_already_died(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}])
        dead = subprocess.Popen([sys.executable, "-c", "pass"])
        dead.wait()
        record = json.loads((job / "session.json").read_text())
        record["panes"]["alpha"] = self.tmux.launch(record, "alpha", [sys.executable, "-c", "import time; time.sleep(60)"])
        record.update(state="waiting_for_native", supervisor_pid=dead.pid)
        launcher.write_json(job / "session.json", record)
        self.assertEqual(launcher.operate("stop", job, self.tmux), 0)
        record = json.loads((job / "session.json").read_text())
        self.assertEqual(record["state"], "stopped")
        self.assertIn("finalized by stop", record["error"])
        self.assertEqual(self.tmux.panes, {})
        self.assertFalse((job / "stop.request").exists())

    def test_occupied_port_is_refused_before_any_window_or_credential_use(self):
        with socket.socket() as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen(1)
            port = occupied.getsockname()[1]
            record = launcher.prepare_job(self.config([{"name": "alpha", "harness": "codex"},
                                                       {"name": "beta", "harness": "codex"}]),
                                          self.folder / "job", doctor=supported, port=port)
            self.tmux.create(record)
            launched = []
            self.tmux.launch = lambda record, name, argv: launched.append(name)
            self.assertEqual(launcher.supervise(Path(record["directory"]), self.tmux, command=self.command), 1)
        record = json.loads((Path(record["directory"]) / "session.json").read_text())
        self.assertIn("already in use", record["error"])
        self.assertEqual(launched, [])

    def test_health_from_another_broker_is_not_this_runs_broker(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}])
        other = launcher.prepare_job(self.config([{"name": "gamma", "harness": "codex"},
                                                  {"name": "delta", "harness": "codex"}]),
                                     self.folder / "other", doctor=supported)
        with owned_broker(Path(other["directory"]) / "run" / "broker.sqlite3") as (_, _, endpoint):
            self.assertTrue(launcher.broker_healthy(endpoint))
            with self.assertRaisesRegex(ValueError, "not this run's broker"):
                launcher.owned_broker(job / "run", endpoint)
        with owned_broker(job / "run" / "broker.sqlite3") as (_, _, endpoint):
            launcher.owned_broker(job / "run", endpoint)  # The run's own broker is accepted.

    def test_stop_without_a_recorded_supervisor_uses_the_dashboard_state(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}])
        record = json.loads((job / "session.json").read_text())
        record.update(state="starting", dashboard="%dashboard")
        launcher.write_json(job / "session.json", record)
        self.assertEqual(launcher.operate("stop", job, self.tmux), 0)  # Live dashboard: request only.
        self.assertTrue((job / "stop.request").exists())
        self.assertNotIn("finished", json.loads((job / "session.json").read_text()))
        self.tmux.dashboard_alive = False  # It died before recording a PID.
        self.assertEqual(launcher.operate("stop", job, self.tmux), 0)
        record = json.loads((job / "session.json").read_text())
        self.assertEqual(record["state"], "stopped")
        self.assertIn("finalized by stop", record["error"])
        self.assertEqual(launcher.operate("close", job, self.tmux), 0)

    def test_stop_finalizes_when_the_tmux_session_is_gone(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}])
        record = json.loads((job / "session.json").read_text())
        record.update(state="starting", dashboard="%dashboard")
        launcher.write_json(job / "session.json", record)
        self.tmux.close(record)
        self.assertEqual(launcher.operate("stop", job, self.tmux), 0)
        self.assertEqual(json.loads((job / "session.json").read_text())["state"], "stopped")

    def test_caller_environment_reaches_windows_once_and_never_stays_on_disk(self):
        job = self.job([{"name": "alpha", "harness": "codex"}, {"name": "beta", "harness": "codex"}])
        (job / "stop.request").touch()  # Stop right after the broker window is launched.
        handed = {}
        launch = self.tmux.launch
        def capture_handover(record, name, argv):
            handed[name] = json.loads((job / "environment" / (name + ".json")).read_text())
            return launch(record, name, argv)
        self.tmux.launch = capture_handover
        with patch.dict(os.environ):
            launcher.write_json(job / launcher.ENVIRONMENT, {**os.environ, "XORDINATION_CALLER_ONLY": "from-start"})
            launcher.supervise(job, self.tmux, command=self.command, poll=.05)
        self.assertEqual(handed["broker"]["XORDINATION_CALLER_ONLY"], "from-start")
        self.assertFalse((job / launcher.ENVIRONMENT).exists())
        self.assertEqual(list((job / "environment").iterdir()), [])
        # A pane consumes its handover file and keeps its own tmux identity.
        launcher.write_json(job / "environment" / "alpha.json", {"PATH": "/caller/bin", "TMUX": "stale"})
        with patch.dict(os.environ, {"TMUX": "current"}):
            environment = launcher.pane_environment(job, "alpha")
        self.assertEqual((environment["PATH"], environment["TMUX"]), ("/caller/bin", "current"))
        self.assertFalse((job / "environment" / "alpha.json").exists())

    def test_preflight_gap_refuses_before_preparing_or_launching(self):
        def observed(harness):
            return ({"harness": harness, "status": "unavailable"} if harness == "grok"
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
