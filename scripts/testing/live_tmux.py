"""Operator-owned tmux launcher for opt-in native acceptance.

Session ownership, window options and owned-pane cleanup follow the managed
launcher of multi-harness-proof (mhproof/sessions.py). This launcher only
orchestrates live.py and the public broker/adapter entry points; acceptance
evidence and verdicts remain live.py's. ``start`` invokes authenticated native
sessions and can incur provider charges; ordinary CI never runs it.
"""
import argparse
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import secrets
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen

HERE = Path(__file__).resolve()
ROOT = HERE.parents[2]
SOURCE = ROOT / "src"
# The run must exercise this checkout, which is also what live.py hashes.
sys.path[:0] = [str(SOURCE), str(HERE.parent)]
import live  # noqa: E402

RESERVED = {"dashboard", "broker"}
OWNER_OPTION = "@xordination_owner"


def default_root():
    # Outside the checkout: Codex and Claude read project guidance from parent directories.
    state = os.environ.get("XDG_STATE_HOME") or str(Path.home() / ".local" / "state")
    return Path(state) / "xordination" / "live-runs"


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        if default is None:
            raise
        return default


def write_json(path, value):
    """Replace a mutable lifecycle record; immutable evidence stays live.py's write_new."""
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix=".lifecycle-", dir=path.parent)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


class Tmux:
    def call(self, *args, check=True):
        try:
            result = subprocess.run(["tmux", *map(str, args)], text=True, capture_output=True, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            if check:
                raise
            return ""
        if check and result.returncode:
            raise RuntimeError("tmux: " + result.stderr.strip())
        return result.stdout.strip()

    def owned(self, record):
        # Option commands parse target-pane; the trailing colon names the exact session.
        actual = self.call("show-options", "-qv", "-t", "=" + record["session"] + ":", OWNER_OPTION, check=False)
        return actual == record["owner"]

    def _window_options(self, pane):
        for option in ("remain-on-exit on", "automatic-rename off", "allow-rename off"):
            self.call("set-option", "-w", "-t", pane, *option.split())

    def create(self, record):
        pane = self.call("new-session", "-d", "-P", "-F", "#{pane_id}", "-s", record["session"],
                         "-n", "dashboard", "-c", ROOT, sys.executable, "-c", "import time; time.sleep(60)")
        self.call("set-option", "-t", "=" + record["session"] + ":", OWNER_OPTION, record["owner"])
        self._window_options(pane)
        return pane

    def respawn(self, record, pane, argv):
        if not self.pane_owned(record, pane):
            raise RuntimeError("Managed tmux pane is missing or ownership differs")
        self.call("respawn-pane", "-k", "-t", pane, shlex.join(map(str, argv)))

    def launch(self, record, name, argv):
        if not self.owned(record):
            raise RuntimeError("Managed tmux session is missing or ownership differs")
        pane = self.call("new-window", "-d", "-P", "-F", "#{pane_id}", "-t", record["session"] + ":",
                         "-n", name, "-c", ROOT, shlex.join(map(str, argv)))
        self._window_options(pane)
        return pane

    def select(self, record, pane):
        if self.pane_owned(record, pane):
            self.call("select-window", "-t", pane, check=False)

    def pane_owned(self, record, pane):
        return self.owned(record) and self.call("display-message", "-p", "-t", pane,
                                              "#{session_name}", check=False) == record["session"]

    def capture(self, pane):
        return self.call("capture-pane", "-p", "-S", "-", "-t", pane, check=False)

    def kill(self, pane):
        self.call("kill-pane", "-t", pane)

    def close(self, record):
        if self.owned(record):
            self.call("kill-session", "-t", "=" + record["session"])

    def attach(self, record):
        if not self.owned(record):
            raise ValueError("This run has no open managed tmux session; use status")
        command = "switch-client" if os.environ.get("TMUX") else "attach-session"
        return subprocess.call(["tmux", command, "-t", "=" + record["session"]])


class Stopped(Exception):
    pass


def required_names(config):
    return sorted({name for pair in config["pairs"] for name in pair})


def claude_settings(job, name, phase_timeout):
    """Additive run-local project hook; the adapter's own SessionStart hook is unchanged."""
    command = shlex.join([sys.executable, str(HERE.parent / "live.py"), "observe-claude",
                          "--run", str(job / "run"), "--participant", name])
    # The held wait may last twice the phase timeout before its deadline closes it.
    timeout = math.ceil(2 * phase_timeout) + 30
    return {"hooks": {"PreToolUse": [{"matcher": "mcp__xordination__.*",
                                      "hooks": [{"type": "command", "command": command, "timeout": timeout}]}]}}


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def preflight(config, doctor):
    names = required_names(config)
    if RESERVED & set(names):
        raise ValueError("Participant names dashboard/broker are reserved by the launcher")
    observations = {}
    for participant in config["participants"]:
        if participant["name"] in names and participant["harness"] not in observations:
            observations[participant["harness"]] = doctor(participant["harness"])
    gaps = [harness + ": " + observation.get("status", "unknown") for harness, observation in observations.items()
            if observation.get("status") != "supported"]
    return {"harnesses": observations}, gaps


def prepare_job(config_path, job, *, phase_timeout=30, startup_timeout=600, reply_timeout=300, port=None,
                doctor=None):
    """Create a fresh job, probe harnesses without model calls, then prepare the live run."""
    if doctor is None:
        from xordination.adapters import doctor
    for value in (phase_timeout, startup_timeout, reply_timeout):
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError("Timeouts must be finite and positive")
    if phase_timeout > 300:
        raise ValueError("Phase timeout must be at most 300 seconds")
    job = Path(job).resolve()
    job.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    job.mkdir(mode=0o700)  # Exclusive: a run directory is never reused.
    config = live.validate_config(live.load(config_path))
    owner = secrets.token_hex(16)
    port = port or free_port()
    record = {"schema_version": 1, "directory": str(job), "session": "xordination-" + owner[:12],
              "owner": owner, "state": "preflight", "created": time.time(), "panes": {},
              "port": port, "endpoint": "http://127.0.0.1:" + str(port),
              "settings": {"phase_timeout": phase_timeout, "startup_timeout": startup_timeout,
                           "reply_timeout": reply_timeout}}
    write_json(job / "session.json", record)
    found, gaps = preflight(config, doctor)
    write_json(job / "preflight.json", found)
    if gaps:
        record.update(state="error", error="Preflight failed: " + "; ".join(gaps), finished=time.time())
        write_json(job / "session.json", record)
        raise ValueError(record["error"] + "; inspect " + str(job / "preflight.json"))
    for participant in config["participants"]:
        observed = found["harnesses"].get(participant["harness"], {}).get("version", {}).get("value")
        if "version" not in participant and observed:
            participant["version"] = observed[:500]  # Observed --version output, still an operator observation.
    write_json(job / "config.json", config)
    live.prepare(config, job / "run")
    for participant in config["participants"]:
        workspace = job / "workspaces" / participant["name"]
        workspace.mkdir(parents=True, mode=0o700)
        if participant["harness"] == "claude":
            (workspace / ".claude").mkdir(mode=0o700)
            write_json(workspace / ".claude" / "settings.local.json",
                       claude_settings(job, participant["name"], phase_timeout))
    record["state"] = "prepared"
    write_json(job / "session.json", record)
    return record


def pane_command(job, role):
    return [sys.executable, str(HERE), "pane", str(job), "--role", role]


def exec_pane(job, role):
    """Pane entry point: credentials come from run files, never the tmux command line."""
    job = Path(job).resolve()
    record = read_json(job / "session.json")
    run = job / "run"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(SOURCE), env.get("PYTHONPATH")]))
    env["XORDINATION_ENDPOINT"] = record["endpoint"]
    if role == "broker":
        argv = [sys.executable, "-m", "xordination", "serve", "--state", str(run / "broker.sqlite3"),
                "--host", "127.0.0.1", "--port", str(record["port"])]
        cwd = job
    else:
        participant = next(p for p in live.checked_draft(run)["config"]["participants"] if p["name"] == role)
        cwd = job / "workspaces" / role
        if participant["harness"] == "claude":
            env["XORDINATION_TOKEN"] = live.load(run / (role + ".connection.json"))["token"]
            argv = [sys.executable, "-m", "xordination", "adapter", "claude", "--project", str(cwd),
                    "--endpoint", record["endpoint"], "--prompt", live.INITIAL_PROMPT]
            for key in ("model", "reasoning"):
                if participant.get(key):
                    argv += ["--" + key, participant[key]]
        else:
            argv = [sys.executable, str(HERE.parent / "live.py"), "adapter", "--run", str(run),
                    "--participant", role, "--project", str(cwd)]
    os.chdir(cwd)
    os.execve(sys.executable, argv, env)


def missing_readiness(run):
    """Required native bindings, plus Claude's fresh channel registration, before capture."""
    draft = live.checked_draft(run)
    required = {name for case in draft["cases"] for name in (case["sender"], case["recipient"])}
    missing = []
    with live.readonly(run / "broker.sqlite3") as connection:
        for participant in draft["config"]["participants"]:
            name = participant["name"]
            if name not in required:
                continue
            row = connection.execute("""SELECT b.* FROM bindings b JOIN participants p
                ON b.generation=p.generation WHERE b.space=? AND b.participant=?""",
                                     (draft["space"], name)).fetchone()
            if (not row or not row["active"] or not row["native_id"] or
                    row["native_identity_source"] != "adapter_reported"):
                missing.append(name + ": native binding")
            elif participant["harness"] == "claude" and not json.loads(row["capabilities"]).get("channel_registered"):
                missing.append(name + ": channel registration")
    return missing


def broker_healthy(endpoint):
    try:
        with urlopen(endpoint + "/health", timeout=2) as response:
            return json.loads(response.read()).get("ok") is True
    except (OSError, ValueError):
        return False


def summarize(record, report):
    summary = {"state": record.get("state"), "error": record.get("error"),
               "status": report["status"] if report else "incomplete"}
    if report:
        summary.update(passed_cases=report["passed_cases"], required_cases=report["required_cases"],
                       participants=report["configured_participants"],
                       failed=[{"case_id": case["case_id"], "scenario": case["scenario"],
                                "sender": case["sender"], "recipient": case["recipient"],
                                "failures": case["failures"]} for case in report["cases"] if case["status"] != "pass"],
                       errors=report["errors"])
    return summary


def render(summary):
    lines = ["Verdict: " + summary["status"].upper()]
    if "required_cases" in summary:
        lines.append("Cases: " + str(summary["passed_cases"]) + "/" + str(summary["required_cases"]) + " passed")
        for case in summary["failed"]:
            lines.append("  " + case["scenario"] + " " + case["sender"] + " -> " + case["recipient"] + ": " +
                         ", ".join(case["failures"]))
    if summary.get("error"):
        lines.append("Supervisor: " + summary["error"])
    return "\n".join(lines) + "\n"


def supervise(job, tmux=None, command=pane_command, poll=.5):
    tmux = tmux or Tmux()
    job = Path(job).resolve()
    record = read_json(job / "session.json")
    settings = record["settings"]
    run = job / "run"
    os.environ["XORDINATION_ENDPOINT"] = record["endpoint"]  # Read by live.send_tasks.
    stop_request = job / "stop.request"
    report = None

    def save(**changes):
        record.update(changes)
        write_json(job / "session.json", record)

    def wait(condition, timeout, failure, progress=None):
        deadline = time.monotonic() + timeout
        shown = None
        while True:
            if stop_request.exists():
                raise Stopped("Operator requested stop")
            result = condition()
            if result:
                return result
            if progress:
                text = progress()
                if text != shown:
                    print(text, flush=True)
                    shown = text
            if time.monotonic() >= deadline:
                raise TimeoutError(failure)
            time.sleep(poll)

    try:
        if not tmux.owned(record):
            raise ValueError("Supervisor is not in its registered tmux session")
        save(state="starting", supervisor_pid=os.getpid())
        record["panes"]["broker"] = tmux.launch(record, "broker", command(job, "broker"))
        save()
        wait(lambda: broker_healthy(record["endpoint"]), 30, "Broker did not answer /health within 30 seconds")
        draft = live.checked_draft(run)
        harness = {p["name"]: p["harness"] for p in draft["config"]["participants"]}
        for name in required_names(draft["config"]):
            record["panes"][name] = tmux.launch(record, name, command(job, name))
            save()
        claude = [name for name in required_names(draft["config"]) if harness[name] == "claude"]
        if claude:
            tmux.select(record, record["panes"][claude[0]])  # Trust and channel prompts need the operator.
        save(state="waiting_for_native")
        print("Waiting for native sessions. Approvals remain yours; Ctrl+B, then W selects a window.", flush=True)
        wait(lambda: not missing_readiness(run), settings["startup_timeout"],
             "Native sessions were not ready within the startup timeout",
             progress=lambda: "Waiting for: " + ", ".join(missing_readiness(run)))
        live.capture(run)
        save(state="dispatching")
        dispatch = live.send_tasks(run, settings["phase_timeout"])
        write_json(job / "dispatch.json", dispatch)
        save(state="awaiting_replies")

        def progress():
            current = live.verify(run, retain=False)
            return "Verified " + str(current["passed_cases"]) + "/" + str(current["required_cases"]) + " cases"
        try:
            wait(lambda: live.verify(run, retain=False)["status"] == "pass", settings["reply_timeout"],
                 "Not every required case passed within the reply timeout", progress=progress)
        except TimeoutError as error:
            record["error"] = str(error)  # Partial cases stay in the retained report below.
    except Stopped as error:
        record["error"] = str(error)
        record["stopped"] = True
    except Exception as error:
        record["error"] = type(error).__name__ + ": " + str(error)
    finally:
        if (run / "frozen.json").exists():
            try:
                report = live.verify(run)  # Retained immutably beside earlier reports.
            except Exception as error:
                record["verify_error"] = type(error).__name__ + ": " + str(error)
        secrets_seen = [live.load(path)["token"] for path in sorted(run.glob("*.connection.json"))]
        (job / "logs").mkdir(mode=0o700, exist_ok=True)
        record["cleanup"] = []
        # Participants first, then the broker, so adapters settle against a live endpoint.
        for name in sorted(record["panes"], key=lambda name: name == "broker"):
            pane = record["panes"][name]
            try:
                if not tmux.pane_owned(record, pane):
                    record["cleanup"].append({"window": name, "status": "already-closed-or-unowned"})
                    continue
                text = tmux.capture(pane)
                for token in secrets_seen:
                    text = text.replace(token, "[redacted]")
                path = job / "logs" / (name + "-terminal.log")
                path.write_text(text)
                os.chmod(path, 0o600)
                tmux.kill(pane)
                record["cleanup"].append({"window": name, "status": "closed"})
            except Exception as error:
                record["cleanup"].append({"window": name, "status": "error", "detail": str(error)})
        if any(item["status"] == "error" for item in record["cleanup"]):
            record.setdefault("error", "One or more owned windows could not be closed; inspect cleanup records")
        state = "stopped" if record.get("stopped") else "finished" if report and "error" not in record else "error"
        save(state=state, finished=time.time())
        summary = summarize(record, report)
        write_json(job / "summary.json", summary)
        print("\n" + render(summary), end="", flush=True)
        print("Run files: " + str(job) + "\nClose this dashboard: " + shlex.join(
            [sys.executable, str(HERE), "close", str(job)]), flush=True)
    return 0 if report and report["status"] == "pass" and "error" not in record else 1


def start(args, tmux=None):
    if tmux is None and not shutil.which("tmux"):
        raise ValueError("tmux is not installed")  # Refuse before creating a job or credentials.
    tmux = tmux or Tmux()
    job = Path(args.dir) if args.dir else default_root() / (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(3))
    record = prepare_job(args.config, job, phase_timeout=args.phase_timeout,
                         startup_timeout=args.startup_timeout, reply_timeout=args.reply_timeout, port=args.port)
    job = Path(record["directory"])
    try:
        pane = tmux.create(record)
        record.update(state="starting", dashboard=pane)
        write_json(job / "session.json", record)
        tmux.respawn(record, pane, [sys.executable, str(HERE), "supervise", str(job)])
    except Exception as error:
        record.update(state="error", error=str(error), finished=time.time())
        write_json(job / "session.json", record)
        tmux.close(record)
        raise
    print("Run: " + str(job), flush=True)
    print("Status: " + shlex.join([sys.executable, str(HERE), "status", str(job)]), flush=True)
    print("Native sessions may ask for trust, channel and tool approvals. Ctrl+B, then W selects a window.",
          flush=True)
    return 0 if args.detach else tmux.attach(record)


def status(job):
    job = Path(job).resolve()
    record = read_json(job / "session.json")
    run = job / "run"
    print("Managed session: " + record.get("state", "unknown"))
    if record.get("error"):
        print("Supervisor: " + record["error"])
    summary = read_json(job / "summary.json", {})
    if summary:
        print(render(summary), end="")
    elif (run / "frozen.json").exists():
        print(render(summarize(record, live.verify(run, retain=False))), end="")
    elif (run / "prepared.json").exists() and (run / "broker.sqlite3").exists():
        missing = missing_readiness(run)
        print("Waiting for: " + (", ".join(missing) if missing else "capture"))
    return 0


def operate(action, job, tmux=None):
    tmux = tmux or Tmux()
    job = Path(job).resolve()
    record = read_json(job / "session.json")
    if action == "attach":
        return tmux.attach(record)
    if action == "stop":
        if record.get("finished"):
            print("Run already finished; use close to remove its dashboard.")
            return 0
        if not tmux.owned(record):
            raise ValueError("Managed session is absent; inspect session.json and the run directory")
        (job / "stop.request").touch(mode=0o600, exist_ok=True)
        print("Stop requested. The supervisor retains a report, closes its windows and writes summary.json.")
        return 0
    if action == "close":
        if not record.get("finished"):
            raise ValueError("Run is still active; use stop first so evidence can be finalized")
        tmux.close(record)
        print("Managed dashboard closed. Run files preserved.")
        return 0
    raise ValueError("Unknown session action")


class IsolatedTmux(Tmux):
    """A private server socket; never targets the operator's own tmux server."""

    def __init__(self, socket_path):
        self.socket = str(socket_path)

    def call(self, *args, **kwargs):
        return super().call("-S", self.socket, "-f", "/dev/null", *args, **kwargs)


def self_check():
    """Real tmux parsing on a private socket; no broker, harness or model is started."""
    if not shutil.which("tmux"):
        raise ValueError("tmux is not installed")
    with tempfile.TemporaryDirectory(prefix="xordination-tmux-") as folder:
        tmux = IsolatedTmux(Path(folder) / "server.sock")
        record = {"session": "xordination-check", "owner": secrets.token_hex(16)}
        sleeper = [sys.executable, "-c", "import time; time.sleep(60)"]
        try:
            tmux.call("new-session", "-d", "-s", "xordination-check-other", *sleeper)
            tmux.call("set-option", "-t", "=xordination-check-other:", OWNER_OPTION, record["owner"])
            checks = {"prefix_session_not_owned": not tmux.owned(record)}
            dashboard = tmux.create(record)
            checks["owned"] = tmux.owned(record)
            checks["remain_on_exit"] = tmux.call("show-options", "-wv", "-t", dashboard, "remain-on-exit") == "on"
            pane = tmux.launch(record, "alpha", [sys.executable, "-c", "print('pane-output'); import time; time.sleep(60)"])
            tmux.select(record, pane)
            checks["selected"] = tmux.call("display-message", "-p", "-t", "=xordination-check:",
                                           "#{window_name}") == "alpha"
            deadline = time.monotonic() + 5
            while "pane-output" not in tmux.capture(pane) and time.monotonic() < deadline:
                time.sleep(.05)
            checks["captured"] = "pane-output" in tmux.capture(pane)
            tmux.kill(pane)
            checks["killed"] = not tmux.pane_owned(record, pane)
            tmux.close(record)
            checks["closed"] = not tmux.owned(record)
            checks["foreign_preserved"] = tmux.call("list-sessions", "-F", "#{session_name}") == "xordination-check-other"
        finally:
            tmux.call("kill-server", check=False)
    print(json.dumps(checks, indent=2, sort_keys=True))
    return 0 if all(checks.values()) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("start", help="Prepare a fresh run and launch it in an owned tmux session")
    p.add_argument("--config", required=True)
    p.add_argument("--dir", help="Fresh job directory (default: XDG state xordination/live-runs/...)")
    p.add_argument("--port", type=int)
    p.add_argument("--phase-timeout", type=float, default=30)
    p.add_argument("--startup-timeout", type=float, default=600)
    p.add_argument("--reply-timeout", type=float, default=300)
    p.add_argument("--detach", action="store_true")
    for command in ("supervise", "status", "attach", "stop", "close", "pane"):
        p = sub.add_parser(command)
        p.add_argument("job")
        if command == "pane":
            p.add_argument("--role", required=True)
    sub.add_parser("self-check", help="Exercise real tmux commands on a private socket")
    args = parser.parse_args(argv)
    try:
        if args.command == "start":
            return start(args)
        if args.command == "supervise":
            return supervise(args.job)
        if args.command == "pane":
            exec_pane(args.job, args.role)
        if args.command == "status":
            return status(args.job)
        if args.command == "self-check":
            return self_check()
        return operate(args.command, args.job)
    except (OSError, ValueError, KeyError, RuntimeError) as error:
        print("Launcher failed: " + str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
