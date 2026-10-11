"""Small owned-resource fixtures; no shared broker or database state."""

from contextlib import contextmanager, ExitStack
import os

import json
import queue
import subprocess
import sys
import tempfile
import time
from pathlib import Path
import threading
from unittest.mock import patch

from xordination.server import BrokerServer
from xordination.store import Store


def isolated_process_env(home, *, token=None):
    """Fresh test homes, explicit imports, and no ambient vendor credentials."""
    root = Path(__file__).resolve().parents[1]
    home = str(Path(home).resolve())
    env = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "WINDIR") if key in os.environ}
    env.update({"HOME": home, "USERPROFILE": home, "TMPDIR": home, "TEMP": home, "TMP": home,
                "XDG_CONFIG_HOME": home, "XDG_CACHE_HOME": home, "XDG_STATE_HOME": home,
                "XDG_DATA_HOME": home, "CODEX_HOME": home, "CLAUDE_CONFIG_DIR": home,
                "GROK_HOME": home, "PYTHONHASHSEED": "0"})
    paths = [str(root / "src")]
    # Measurement is explicitly requested by the canonical CI runner. Derive
    # its known bootstrap and installed coverage paths rather than inheriting
    # arbitrary PYTHONPATH entries from the operator environment.
    if os.environ.get("COVERAGE_PROCESS_START"):
        import coverage
        env["COVERAGE_PROCESS_START"] = os.environ["COVERAGE_PROCESS_START"]
        paths.extend((str(root / "scripts" / "ci"), str(Path(coverage.__file__).resolve().parent.parent)))
    env["PYTHONPATH"] = os.pathsep.join(paths)
    if token is not None:
        env["XORDINATION_TEST_TOKEN"] = token
    return env


@contextmanager
def owned_broker(path, **options):
    """Yield a fresh Store, running server and endpoint; retain caller's path."""
    with ExitStack() as cleanup:
        store = Store(str(path))
        cleanup.callback(store.close)
        server = BrokerServer(("127.0.0.1", 0), store, **options)
        cleanup.callback(server.server_close)
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()

        def stop():
            server.stopping.set()
            server.shutdown()
            thread.join(timeout=2)
            if thread.is_alive():
                raise AssertionError("Owned broker thread did not stop")

        cleanup.callback(stop)
        yield store, server, "http://127.0.0.1:" + str(server.server_port)


@contextmanager
def observe_fsync(targets, *, fail=None):
    """Observe selected filesystem objects; leave unrelated real fsyncs intact."""
    targets = tuple(Path(target) for target in targets)
    fail = Path(fail) if fail is not None else None
    observed = []
    original = os.fsync

    def fsync(descriptor):
        opened = os.fstat(descriptor)
        for target in targets:
            try:
                info = target.stat()
            except FileNotFoundError:
                continue
            if (opened.st_dev, opened.st_ino) == (info.st_dev, info.st_ino):
                observed.append(target)
                if target == fail:
                    raise OSError("Injected fsync failure for selected test object")
                break
        original(descriptor)

    with patch("xordination.cli.os.fsync", side_effect=fsync):
        yield observed


class MCPProcess:
    """An owned MCP subprocess with bounded response and shutdown waits."""

    def __init__(self, endpoint, token, cwd):
        self.home = tempfile.TemporaryDirectory(prefix="client-home-", dir=cwd)
        try:
            env = isolated_process_env(self.home.name, token=token)
            self.stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
            self.process = subprocess.Popen(
                [sys.executable, "-m", "xordination", "stdio", "--endpoint", endpoint,
                 "--token-env", "XORDINATION_TEST_TOKEN"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                text=True, encoding="utf-8", bufsize=1, cwd=cwd, env=env,
            )
        except BaseException:
            if hasattr(self, "stderr"):
                self.stderr.close()
            self.home.cleanup()
            raise
        self.responses = queue.Queue()
        self.next_id = 0
        self.buffered = {}
        self.closed = False

        def read():
            try:
                for line in self.process.stdout:
                    self.responses.put(line)
            finally:
                self.responses.put(None)

        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()

    def __enter__(self):
        try:
            result = self.request("initialize", {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "end-to-end-test", "version": "1"},
            })
            if result["protocolVersion"] != "2025-06-18":
                raise AssertionError("MCP frontend did not negotiate the requested supported revision")
            self.notify("notifications/initialized", {})
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *unused):
        self.close()

    def notify(self, method, params):
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        self.process.stdin.flush()

    def request(self, method, params):
        return self.response(self.start_request(method, params))

    def start_request(self, method, params):
        """Start an independent request without waiting for its response."""
        self.next_id += 1
        ident = self.next_id
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": ident,
                                            "method": method, "params": params}) + "\n")
        self.process.stdin.flush()
        return ident

    def response(self, ident, timeout=5):
        """Correlate out-of-order replies; a held wait cannot hide another call."""
        deadline = time.monotonic() + timeout
        while ident not in self.buffered:
            try:
                line = self.responses.get(timeout=max(0, deadline - time.monotonic()))
            except queue.Empty:
                raise AssertionError(f"MCP subprocess timed out responding to request {ident}") from None
            if line is None:
                self.stderr.seek(0)
                raise AssertionError("MCP subprocess closed stdout: " + self.stderr.read(2000))
            packet = json.loads(line)
            packet_id = packet.get("id")
            if (packet.get("jsonrpc") != "2.0" or type(packet_id) is not int
                    or not 0 < packet_id <= self.next_id or packet_id in self.buffered):
                raise AssertionError(f"Unexpected MCP response: {packet!r}")
            self.buffered[packet_id] = packet
        packet = self.buffered.pop(ident)
        if "error" in packet:
            raise AssertionError(f"MCP protocol error: {packet['error']!r}")
        return packet["result"]

    def tool(self, name, **arguments):
        result = self.request("tools/call", {"name": "xordination_" + name, "arguments": arguments})
        if result.get("isError"):
            raise AssertionError(f"MCP tool failed: {result!r}")
        structured = result["structuredContent"]
        if json.loads(result["content"][0]["text"]) != structured:
            raise AssertionError("Text and structured MCP results disagree")
        return structured

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.process.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        self.reader.join(timeout=2)
        self.process.stdout.close()
        self.stderr.close()
        self.home.cleanup()

