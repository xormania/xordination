"""Opt-in redacted protocol traces and support bundles.

A trace records each protocol frame under the same lock that serializes the
wire, so its sequence is the actual frame order for that stream. Frames keep
correlation identifiers; credentials, message bodies and other free text are
replaced by their length before anything reaches disk.
"""

from collections import deque
import json
import math
import os
from pathlib import Path
import platform
import re
import sys
import threading
import time
import uuid
import zipfile

from . import __version__


FORMAT = "xordination-trace/1"
BUNDLE_FORMAT = "xordination-diagnostics/1"
MAX_TRACE_BYTES = 64 * 1024 * 1024
MAX_BUNDLE_RECORDS = 10000
# Only these keys keep string values: protocol envelopes and the identities
# needed to correlate frames with broker messages, attempts and native turns.
CORRELATION_KEYS = frozenset({
    "jsonrpc", "id", "method", "requestId", "name", "tool", "type", "code", "status", "outcome",
    "protocolVersion", "message_id", "attempt_id", "reply_to", "native_id",
    "threadId", "turnId", "sessionId", "promptId", "callId", "itemId",
})
MAX_KEPT = 256
MAX_ITEMS = 64
MAX_DEPTH = 12
_PLACEHOLDER = re.compile(r"<redacted [0-9]+ chars>|<[0-9]+ more items>|<redacted depth>")
_RECORD_FIELDS = {"trace": str, "seq": int, "stream": str, "direction": str, "event": str,
                  "monotonic_ns": int, "time": (int, float)}


def _scrub(text, secrets):
    for secret in secrets:
        text = text.replace(secret, "<redacted secret>")
    return text


def redact(value, secrets=(), *, key=None, depth=0):
    """Return a frame with correlation fields only; repeated redaction is stable."""
    if depth > MAX_DEPTH:
        return "<redacted depth>"
    if isinstance(value, dict):
        return {_scrub(str(name), secrets)[:MAX_KEPT]: redact(item, secrets, key=name, depth=depth + 1)
                for name, item in value.items()}
    if isinstance(value, list):
        items = [redact(item, secrets, key=key, depth=depth + 1) for item in value[:MAX_ITEMS]]
        if len(value) > MAX_ITEMS:
            items.append(f"<{len(value) - MAX_ITEMS} more items>")
        return items
    if isinstance(value, str):
        if _PLACEHOLDER.fullmatch(value):
            return value
        if key in CORRELATION_KEYS and len(value) <= MAX_KEPT:
            return _scrub(value, secrets)
        return f"<redacted {len(value)} chars>"
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value if value is None or isinstance(value, (bool, int, float)) else "<redacted 0 chars>"


class Trace:
    """Append-only local JSON-lines trace for one protocol stream.

    Tracing never decides protocol behavior: a trace write failure disables the
    trace and reports once on stderr. Protocol stdout never carries diagnostics.
    """

    def __init__(self, path, stream, *, secrets=(), max_bytes=MAX_TRACE_BYTES, error_output=None):
        self.path = os.path.abspath(path)
        self.stream = stream
        self.secrets = tuple(secret for secret in secrets if secret)
        self.max_bytes = max_bytes
        self.error_output = error_output or sys.stderr
        self.identity = uuid.uuid4().hex
        self.lock = threading.Lock()
        self.sequence = 0
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(self.path, flags, 0o600)
        self.size = os.fstat(descriptor).st_size
        self.file = os.fdopen(descriptor, "a", encoding="utf-8")
        self.record("control", event="trace_start")

    def record(self, direction, packet=None, *, event="frame"):
        with self.lock:
            if self.file is None:
                return
            self.sequence += 1
            entry = {"format": FORMAT, "trace": self.identity, "seq": self.sequence, "stream": self.stream,
                     "direction": direction, "event": event, "monotonic_ns": time.monotonic_ns(),
                     "time": time.time()}
            try:
                if packet is not None:
                    entry["frame"] = redact(packet, self.secrets)
                line = json.dumps(entry, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n"
                if self.size + len(line.encode("utf-8")) > self.max_bytes:
                    entry.pop("frame", None)
                    self.file.write(json.dumps({**entry, "direction": "control",
                                                "event": "trace_limit_reached"}) + "\n")
                    self.file.close()
                    self.file = None
                    return
                self.file.write(line)
                self.file.flush()
                self.size += len(line.encode("utf-8"))
            except (OSError, ValueError, RecursionError) as error:
                stream, self.file = self.file, None
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass
                self.error_output.write(f"xordination: diagnostic trace disabled ({type(error).__name__})\n")
                self.error_output.flush()

    def close(self):
        with self.lock:
            if self.file is not None:
                try:
                    self.file.close()
                except OSError:
                    pass
                self.file = None


def open_trace(path, stream, *, secrets=()):
    return Trace(path, stream, secrets=secrets) if path else None


def _trace_record(line, secrets):
    """Rebuild one trace record from known fields only, re-applying redaction."""
    try:
        value = json.loads(line)
    except ValueError:
        return None
    if not isinstance(value, dict) or value.get("format") != FORMAT:
        return None
    record = {"format": FORMAT}
    for name, kind in _RECORD_FIELDS.items():
        item = value.get(name)
        if not isinstance(item, kind) or isinstance(item, bool):
            return None
        record[name] = _scrub(item, secrets)[:MAX_KEPT] if isinstance(item, str) else item
    if "frame" in value:
        record["frame"] = redact(value["frame"], secrets)
    return record


def bundle(output, *, traces=(), doctor=None, health=None, secrets=(), max_records=MAX_BUNDLE_RECORDS):
    """Create a new private support archive; refuse to overwrite an existing file."""
    secrets = tuple(secret for secret in secrets if secret)
    summaries, members = [], []
    for index, source in enumerate(traces):
        window, records, dropped = deque(maxlen=max_records), 0, 0
        with open(source, encoding="utf-8", errors="replace") as stream:
            for line in stream:
                record = _trace_record(line, secrets)
                if record is None:
                    dropped += 1
                    continue
                records += 1
                window.append(record)
        name = f"traces/{index:02d}-{_scrub(Path(source).name, secrets)}"
        members.append((name, "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in window)))
        summaries.append({"member": name, "records": records, "included": len(window),
                          "dropped_unrecognized_lines": dropped})
    manifest = {
        "format": BUNDLE_FORMAT, "xordination_version": __version__, "created": time.time(),
        "python": platform.python_version(), "platform": platform.platform(),
        "traces": summaries,
        "redaction": {"credentials": "excluded", "message_bodies": "excluded",
                      "trace_frames": "re-redacted to correlation fields",
                      "known_secrets_scrubbed": len(secrets)},
        "live_communication_verified": False,
    }
    for name, value in (("doctor.json", doctor), ("health.json", health)):
        if value is not None:
            members.append((name, json.dumps(_scrub_json(value, secrets), indent=2) + "\n"))
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(output, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream, zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("manifest.json", json.dumps(manifest, indent=2) + "\n")
            for name, text in members:
                archive.writestr(name, text)
    except BaseException:
        os.unlink(output)
        raise
    return manifest


def _scrub_json(value, secrets):
    if isinstance(value, dict):
        return {_scrub(str(name), secrets): _scrub_json(item, secrets) for name, item in value.items()}
    if isinstance(value, list):
        return [_scrub_json(item, secrets) for item in value]
    return _scrub(value, secrets) if isinstance(value, str) else value
