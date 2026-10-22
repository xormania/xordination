"""Bounded HTTP broker for authenticated project-scoped operations."""
import json
import os
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from . import __version__

MAX_REQUEST_BYTES = 262144
OPERATIONS = {"peers", "send", "inbox", "wait", "ack", "status", "retry", "cancel",
              "bind", "claim", "finish", "observe", "stop", "recover", "identity"}


class OwnershipLock:
    """One broker may recover and dispatch a given state file at a time."""
    def __init__(self, state_path):
        path = Path(str(state_path) + ".broker.lock")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            if os.name == "nt":
                import msvcrt
                if os.fstat(self.fd).st_size == 0:
                    os.write(self.fd, b"0")
                os.lseek(self.fd, 0, os.SEEK_SET)
                msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(self.fd)
            self.fd = None
            raise ValueError("Another broker already owns this state file") from None

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


class BrokerServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, store, *, max_requests=64, max_waits=16):
        self.store = store
        self.connections = threading.BoundedSemaphore(max_requests)
        self.slots = threading.BoundedSemaphore(max_requests)
        self.wait_slots = threading.BoundedSemaphore(max_waits)
        self.stopping = threading.Event()
        self.ownership = OwnershipLock(store.path)
        try:
            super().__init__(address, Handler)
            store.recover_claims()
        except Exception:
            self.ownership.close()
            raise

    def process_request(self, request, client_address):
        # Bound threads before reading headers: slow clients must not bypass
        # the operation-level semaphore by leaving requests unfinished.
        if not self.connections.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self.connections.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.connections.release()

    def server_close(self):
        self.stopping.set()
        super().server_close()
        self.ownership.close()

    def dispatch(self, token, operation, params):
        ctx = self.store.authenticate(token, allow_retired=operation == "finish")
        if operation not in OPERATIONS:
            raise ValueError("Unknown broker operation")
        if operation != "wait":
            return getattr(self.store, operation)(ctx, **params)
        timeout = params.pop("timeout", 20)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 <= timeout <= 30:
            raise ValueError("Wait timeout must be between 0 and 30 seconds")
        if not self.wait_slots.acquire(blocking=False):
            raise BusyError("Too many simultaneous waits; retry after a delay")
        try:
            deadline = time.monotonic() + timeout
            while True:
                result = self.store.inbox(ctx, **params)
                if result["messages"] or self.stopping.is_set() or time.monotonic() >= deadline:
                    return result
                self.stopping.wait(min(.1, max(0, deadline - time.monotonic())))
        finally:
            self.wait_slots.release()


class BusyError(ValueError):
    code = "backpressure"


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.settimeout(40)

    def log_message(self, *_args):
        # Do not emit paths, credentials or participant content into ambient logs.
        pass

    def respond(self, status, value):
        data = json.dumps(value, allow_nan=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass
        self.close_connection = True

    def do_GET(self):
        if urlsplit(self.path).path != "/health":
            self.respond(404, {"ok": False, "error": {"code": "not_found", "message": "Not found"}})
            return
        self.respond(200, {"ok": True, "service": "xordination", "version": __version__,
                           "readiness": "service", "native_communication_verified": False})

    def do_POST(self):
        if not self.server.slots.acquire(blocking=False):
            self.respond(503, {"ok": False, "error": {"code": "backpressure",
                                                    "message": "Broker concurrency limit reached"}})
            return
        try:
            self.handle_rpc()
        finally:
            self.server.slots.release()

    def handle_rpc(self):
        if self.path != "/rpc":
            self.respond(404, {"ok": False, "error": {"code": "not_found", "message": "Not found"}})
            return
        if self.headers.get("Origin"):
            self.respond(403, {"ok": False, "error": {"code": "origin_rejected",
                                                     "message": "Browser origins are not accepted"}})
            return
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer ") or not auth[7:]:
            self.respond(401, {"ok": False, "error": {"code": "unauthorized",
                                                     "message": "A participant token is required"}})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_REQUEST_BYTES or self.headers.get("Transfer-Encoding"):
                raise ValueError("Request size is missing or exceeds the broker limit")
            if self.headers.get_content_type() != "application/json":
                raise ValueError("Content-Type must be application/json")
            payload = json.loads(self.rfile.read(length))
            if (not isinstance(payload, dict) or set(payload) != {"operation", "params"}
                    or not isinstance(payload["operation"], str)
                    or not isinstance(payload["params"], dict)):
                raise ValueError("Expected operation and params objects")
            result = self.server.dispatch(auth[7:], payload["operation"], payload["params"])
        except PermissionError as exc:
            self.respond(403, {"ok": False, "error": {"code": getattr(exc, "code", "forbidden"),
                                                     "message": str(exc)}})
        except (ValueError, TypeError) as exc:
            code = getattr(exc, "code", "invalid_request")
            self.respond(503 if code == "backpressure" else 400,
                         {"ok": False, "error": {"code": code, "message": str(exc)}})
        except (TimeoutError, OSError):
            self.respond(503, {"ok": False, "error": {"code": "unavailable",
                                                     "message": "Broker operation unavailable"}})
        except Exception:
            self.respond(500, {"ok": False, "error": {"code": "internal_error",
                                                     "message": "Broker operation failed"}})
        else:
            self.respond(200, {"ok": True, "result": result})

