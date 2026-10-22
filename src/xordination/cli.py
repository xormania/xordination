"""Explicit operator bootstrap and native communication entry points."""
import argparse
import json
import os
from pathlib import Path
import sys

from . import __version__
from .client import Client, BrokerError


def token_from_env(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError("Set the participant token in environment variable " + name)
    return value


def write_json(value):
    text = json.dumps(value, indent=2, allow_nan=False) + "\n"
    sys.stdout.write(text)
    sys.stdout.flush()


def issue_binding(store, args):
    """Persist the new credential before committing a replacement binding."""
    options = {"harness": args.harness, "replace": args.replace}
    if not args.output:
        return store.join(args.space, args.participant, **options, on_issued=write_json)
    path = Path(args.output)
    missing_directories = []
    directory = path.parent
    while not directory.exists():
        missing_directories.append(directory)
        directory = directory.parent
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    inode = os.fstat(fd).st_ino
    try:
        with os.fdopen(fd, "w") as stream:
            def retain(value):
                stream.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
                # Persist the filename before retiring the old token. Newly
                # created ancestors need their own parent entries persisted too.
                directories = [path.parent] + [item.parent for item in missing_directories]
                for directory in dict.fromkeys(directories):
                    sync_directory(directory)
            return store.join(args.space, args.participant, **options, on_issued=retain)
    except BaseException:
        # Remove only the failed descriptor this invocation reserved.
        try:
            if path.stat().st_ino == inode:
                path.unlink()
        except FileNotFoundError:
            pass
        raise


def sync_directory(path):
    """Persist directory entries on POSIX; failure aborts credential issuance."""
    if os.name != "posix":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def parser():
    p = argparse.ArgumentParser(prog="xordination",
                                description="Durable communication between native harness sessions")
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="Run the shared communication broker")
    serve.add_argument("--state", required=True)
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8765)
    space = sub.add_parser("space", help="Create an explicit communication namespace")
    space.add_argument("--state", required=True)
    space.add_argument("space")
    join = sub.add_parser("join", help="Issue a participant binding and scoped token")
    join.add_argument("--state", required=True)
    join.add_argument("--space", required=True)
    join.add_argument("--participant", required=True)
    join.add_argument("--harness", choices=("external", "codex", "claude", "grok"), default="external")
    join.add_argument("--replace", action="store_true")
    join.add_argument("--output", help="Create a private connection descriptor; refuse overwrite")
    view = sub.add_parser("view", help="Summarize a space's participants, generations and pending delivery")
    view.add_argument("--state", required=True)
    view.add_argument("--space", required=True)
    for name in ("stdio", "adapter", "call", "claude-hook"):
        command = sub.add_parser(name)
        if name == "adapter":
            command.add_argument("harness", choices=("codex", "claude", "grok"))
            command.add_argument("--project", required=True)
            command.add_argument("--binary")
            command.add_argument("--model")
            command.add_argument("--reasoning")
            command.add_argument("--prompt")
            command.add_argument("--mode", choices=("launch", "attach", "resume"), default="launch")
            command.add_argument("--native-id")
        elif name == "stdio":
            command.add_argument("--channel", action="store_true")
        elif name == "call":
            command.add_argument("operation", choices=("peers", "send", "inbox", "wait", "ack",
                                 "status", "retry", "cancel", "claim", "finish", "observe",
                                 "recover", "stop"))
            command.add_argument("--params", default="{}", help="JSON operation parameters")
        if name in ("stdio", "adapter"):
            command.add_argument("--trace", help="Append a redacted protocol-frame trace to this file (created 0600)")
        command.add_argument("--endpoint", default="http://127.0.0.1:8765")
        command.add_argument("--token-env", default="XORDINATION_TOKEN")
    readiness = sub.add_parser("readiness", help="Report package, service, adapter and communication "
                               "readiness separately without starting native sessions")
    readiness.add_argument("--state", help="Durable state location to check; never created or opened")
    readiness.add_argument("--endpoint", help="Broker endpoint for the service level")
    readiness.add_argument("--token-env", help="Participant token variable (default XORDINATION_TOKEN, if set)")
    readiness.add_argument("--harness", action="append", choices=("codex", "claude", "grok"), default=[],
                           help="Probe installed adapter capabilities; repeatable")
    readiness.add_argument("--binary", help="Explicit executable for a single --harness")
    readiness.add_argument("--evidence", action="append", default=[],
                           help="Retained operator_native verification report; repeatable")
    doctor = sub.add_parser("doctor", help="Observe installed harness capabilities without model calls")
    doctor.add_argument("--harness", choices=("codex", "claude", "grok"))
    doctor.add_argument("--binary")
    diagnostics = sub.add_parser("diagnostics", help="Package redacted support diagnostics")
    bundle = diagnostics.add_subparsers(dest="diagnostics", required=True).add_parser(
        "bundle", help="Write redacted traces, doctor and health output to a new private archive")
    bundle.add_argument("--output", required=True, help="New zip archive; refuse overwrite")
    bundle.add_argument("--trace", action="append", default=[], help="Trace file to include (repeatable)")
    bundle.add_argument("--harness", choices=("codex", "claude", "grok"))
    bundle.add_argument("--binary")
    bundle.add_argument("--endpoint", help="Broker endpoint whose unauthenticated health is included")
    bundle.add_argument("--token-env", default="XORDINATION_TOKEN",
                        help="Scrub this environment variable's credential from the bundle")
    return p


def serve(store, host, port, ready=None):
    """Bind, announce the bound address, then serve until interrupted.

    ``ready(host, port)`` runs after binding and before serving, so a caller
    that binds port 0 learns the kernel-chosen port without a probe race.
    """
    from .server import BrokerServer
    server = BrokerServer((host, port), store)
    try:
        host, port = server.server_address[:2]
        if ready is not None:
            ready(host, port)
        print(json.dumps({"service": "xordination", "address": host, "port": port,
                          "readiness": "service"}), flush=True)
        server.serve_forever(poll_interval=.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def diagnostics_bundle(args):
    """Bundle support evidence without credentials, message bodies or model calls."""
    from .adapters import doctor
    from .client import health
    from .trace import bundle
    names = [args.harness] if args.harness else ["codex", "claude", "grok"]
    observed = {"harnesses": {name: doctor(name, binary=args.binary) for name in names},
                "live_communication_verified": False}
    service = None
    if args.endpoint:
        try:
            service = {"endpoint": args.endpoint, "status": "available", "health": health(args.endpoint)}
        except BrokerError as error:
            # Network details can carry endpoints or proxy secrets; keep the code only.
            service = {"endpoint": args.endpoint, "status": "unavailable", "error": error.code}
    manifest = bundle(args.output, traces=args.trace, doctor=observed, health=service,
                      secrets=[os.environ.get(args.token_env, "")])
    write_json({"output": os.path.abspath(args.output), "traces": manifest["traces"],
                "redaction": manifest["redaction"]})
    return 0


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.command in {"serve", "space", "join", "view"}:
            from .store import Store
            if args.command == "view" and not Path(args.state).expanduser().is_file():
                raise ValueError("No broker state exists at " + args.state)
            store = Store(args.state)
            try:
                if args.command == "view":
                    write_json(store.space_view(args.space))
                elif args.command == "space":
                    write_json(store.create_space(args.space))
                elif args.command == "join":
                    issue_binding(store, args)
                else:
                    serve(store, args.host, args.port)
            finally:
                store.close()
            return 0
        if args.command == "doctor":
            from .adapters import doctor
            names = [args.harness] if args.harness else ["codex", "claude", "grok"]
            write_json({"harnesses": {name: doctor(name, binary=args.binary) for name in names},
                        "live_communication_verified": False})
            return 0
        if args.command == "diagnostics":
            return diagnostics_bundle(args)
        if args.command == "readiness":
            from .readiness import report
            if args.binary and len(set(args.harness)) != 1:
                raise ValueError("--binary requires exactly one --harness")
            # An explicitly named variable must exist; the default is used only when present.
            token = (token_from_env(args.token_env) if args.token_env
                     else os.environ.get("XORDINATION_TOKEN") or None)
            write_json(report(state=args.state, endpoint=args.endpoint, token=token,
                              harnesses=args.harness, binary=args.binary, evidence=args.evidence))
            return 0
        token = token_from_env(args.token_env)
        if args.command == "stdio":
            from .mcp import run_stdio
            return run_stdio(args.endpoint, token, channel=args.channel, trace=args.trace)
        if args.command == "adapter":
            from .adapters import run_adapter
            return run_adapter(args.harness, args.endpoint, token, args.project,
                               binary=args.binary, model=args.model, reasoning=args.reasoning,
                               prompt=args.prompt, mode=args.mode, native_id=args.native_id,
                               trace=args.trace)
        if args.command == "claude-hook":
            from .adapters import claude_hook
            return claude_hook(args.endpoint, token)
        if args.command == "call":
            params = json.loads(args.params)
            if not isinstance(params, dict):
                raise ValueError("Parameters must be a JSON object")
            write_json(Client(args.endpoint, token).call(args.operation, **params))
            return 0
    except (ValueError, BrokerError, OSError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0

