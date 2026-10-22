"""Transactional, durable communication state, independent of native harnesses.

One broker owns delivery. After obtaining exclusive runtime ownership it calls
recover_claims(): outstanding native submissions become uncertain. Other local
operator connections may provision identities without pretending to restart it.
Clients use the broker, rather than sharing the database file.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path


class StoreError(ValueError):
    """An expected contract failure with a stable machine-readable code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _fail(code, message):
    raise StoreError(code, message)


def _identifier(value, label):
    if not isinstance(value, str) or not value or len(value.encode()) > 200:
        _fail("invalid_argument", f"{label} must be a nonempty string of at most 200 bytes")
    if any(ord(character) < 32 for character in value):
        _fail("invalid_argument", f"{label} cannot contain control characters")
    return value


def _json(value, limit=16384, label="metadata"):
    try:
        result = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError):
        _fail("invalid_argument", f"{label} must be JSON serializable")
    if len(result.encode()) > limit:
        _fail("limit_exceeded", f"{label} exceeds {limit} bytes")
    return result


def _sync_directory(path):
    """Persist directory entries on POSIX or fail before claiming durability."""
    if os.name != "posix":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) |
                         getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0))
    try:
        if not stat.S_ISDIR(os.fstat(descriptor).st_mode):
            _fail("unsafe_state", "State directory changed during initialization")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _private_state_parent(path):
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        created = False
        try:
            directory.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            if not directory.is_dir():
                raise
        try:
            # The child's entry belongs to this parent, not to the child itself.
            _sync_directory(directory.parent)
        except BaseException:
            if created:
                try:
                    directory.rmdir()  # Only undo this initialization's empty directory.
                except OSError:
                    pass
            raise


def _private_state_file(path, *, create=False, retain=False):
    """Tighten owned POSIX files; Windows deployments require private OS ACLs.

    With ``retain`` the checked descriptor is returned open. Closing any
    descriptor for a file releases this process's POSIX locks on it, so files
    SQLite has already locked must not be closed until its connection closes.
    """
    if os.name != "posix":
        return None
    flags = os.O_RDWR if create else os.O_RDONLY
    flags |= (getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) |
              getattr(os, "O_NONBLOCK", 0))
    if create:
        flags |= os.O_CREAT
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileNotFoundError:
        if not create:
            return None
        raise
    except OSError:
        _fail("unsafe_state", "State files must be privately accessible regular files owned by the current user")
    keep = False
    try:
        _private_descriptor(descriptor, path)
        keep = retain
        return descriptor if retain else None
    finally:
        if not keep:
            os.close(descriptor)


def _private_descriptor(descriptor, path):
    opened = os.fstat(descriptor)
    if not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.geteuid():
        _fail("unsafe_state", "State files must be regular files owned by the current user")
    os.fchmod(descriptor, 0o600)
    os.fsync(descriptor)
    current = os.stat(path, follow_symlinks=False)
    if not stat.S_ISREG(current.st_mode) or (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
        _fail("unsafe_state", "State file identity changed during private initialization")


# POSIX locks belong to the process, and closing any descriptor for a file
# drops all of them. Stores sharing a state path in this process therefore
# share one set of retained descriptors: a later open rechecks those instead of
# opening its own, and only the last close releases them.
_OPEN_STATE = {}
_OPEN_STATE_LOCK = threading.RLock()


class Store:
    SCHEMA_VERSION = 1
    MAX_BODY_BYTES = 65536
    MAX_PAGE_SIZE = 100
    MAX_PENDING = 1000
    CLAIM_SECONDS = 60
    # Only acknowledgement kinds mean the recipient acknowledged. Native
    # insertion is a receipt too, but a weaker one.
    ACK_KINDS = ("client_ack", "native_ack")
    OBSERVED_KINDS = ("native_insertion", "native_ack")

    def __init__(self, path, *, recover_claims=False):
        self.path = str(path)
        self._lock = threading.RLock()
        self._shared = None
        with _OPEN_STATE_LOCK:
            self._open(recover_claims)

    def _open(self, recover_claims):
        shared = None
        if self.path != ":memory:":
            self.path = str(Path(self.path).expanduser().resolve())
            shared = _OPEN_STATE.get(self.path)
            if shared is not None:
                # Another Store here already holds SQLite's locks on these files.
                for suffix, descriptor in shared["descriptors"]:
                    _private_descriptor(descriptor, self.path + suffix)
            else:
                # Newly created state directories are private; existing directories
                # may contain unrelated operator files and are never chmod'ed here.
                _private_state_parent(Path(self.path).parent)
                _private_state_file(self.path, create=True)
                _private_state_file(self.path + "-wal")
                _private_state_file(self.path + "-shm")
                # We precreated the database, so persist its entry ourselves before
                # delegating transaction durability to SQLite's FULL WAL syncs.
                _sync_directory(Path(self.path).parent)
            # Refuse unknown state before anything, including the journal mode,
            # can rewrite it. There is no migration path yet. The probe is
            # read-only: closing the last read-write connection would checkpoint
            # a crashed newer broker's WAL into the main file and delete the
            # sidecars. It still reads through the WAL, so uncheckpointed schema
            # changes are seen (immutable=1 would miss them), and it keeps normal
            # locking against a live writer (so not nolock=1).
            self._check_schema(Path(self.path).as_uri() + "?mode=ro")
        self._db = sqlite3.connect(self.path, timeout=10, isolation_level=None, check_same_thread=False)
        try:
            self._db.row_factory = sqlite3.Row
            self._db.execute("PRAGMA foreign_keys=ON")
            self._db.execute("PRAGMA busy_timeout=10000")
            # Also the only check for :memory:, and it catches a change since the probe.
            self._check_version(self._db)
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            # SQLite inherits the main database's mode for newly created WAL/SHM.
            # Existing files are deliberately upgraded before opening, then checked
            # again once SQLite has initialized its journal. SQLite now holds the
            # WAL-index lock that tells other processes this database is in use;
            # closing a checked descriptor would drop it, letting another process
            # reset the WAL index under this mapping. Keep them until the last
            # Store sharing them closes.
            if self.path != ":memory:":
                if shared is None:
                    shared = _OPEN_STATE[self.path] = {"users": 0, "descriptors": []}
                shared["users"] += 1
                self._shared = shared
                self._retain()
            self._initialize(recover_claims)
            # A fresh database creates its WAL files only on first use.
            self._retain()
        except BaseException:
            # Any later failure, such as a locked database, must release the
            # retained descriptors as well as the connection.
            self._close_connection()
            raise

    def _check_schema(self, uri):
        probe = sqlite3.connect(uri, timeout=10, isolation_level=None, uri=True)
        try:
            self._check_version(probe)
        finally:
            probe.close()

    def _check_version(self, db):
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, self.SCHEMA_VERSION):
            _fail("schema_version", f"Database schema {version} is unsupported")

    def _retain(self):
        # Only the first opener opens descriptors; later ones never close any.
        shared = self._shared
        if shared is None or shared["users"] != 1:
            return
        retained = {suffix for suffix, _descriptor in shared["descriptors"]}
        for suffix in ("", "-wal", "-shm"):
            if suffix not in retained:
                descriptor = _private_state_file(self.path + suffix, retain=True)
                if descriptor is not None:
                    shared["descriptors"].append((suffix, descriptor))

    def _initialize(self, recover_claims):
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS settings (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS spaces (name TEXT PRIMARY KEY, created REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS participants (
                space TEXT NOT NULL REFERENCES spaces(name), participant TEXT NOT NULL,
                generation TEXT NOT NULL, visibility_epoch INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(space, participant)
            );
            CREATE TABLE IF NOT EXISTS bindings (
                generation TEXT PRIMARY KEY, space TEXT NOT NULL, participant TEXT NOT NULL,
                token_hash TEXT UNIQUE NOT NULL, harness TEXT NOT NULL, native_id TEXT,
                native_identity_source TEXT NOT NULL, capabilities TEXT NOT NULL,
                active INTEGER NOT NULL, created REAL NOT NULL, stopped REAL,
                FOREIGN KEY(space, participant) REFERENCES participants(space, participant)
            );
            CREATE TABLE IF NOT EXISTS messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                space TEXT NOT NULL REFERENCES spaces(name), sender TEXT NOT NULL,
                sender_generation TEXT NOT NULL, recipient TEXT NOT NULL,
                requested_to TEXT NOT NULL, requested_generation TEXT,
                original_generation TEXT NOT NULL, delivery_generation TEXT NOT NULL,
                idempotency_key TEXT NOT NULL, intent TEXT NOT NULL, body TEXT NOT NULL,
                metadata TEXT NOT NULL, reply_to TEXT REFERENCES messages(id),
                created REAL NOT NULL, expires REAL, state TEXT NOT NULL,
                current_attempt TEXT,
                UNIQUE(space, sender, idempotency_key)
            );
            CREATE INDEX IF NOT EXISTS inbox_index ON messages(space, recipient, delivery_generation, sequence);
            CREATE TABLE IF NOT EXISTS attempts (
                id TEXT PRIMARY KEY, message_id TEXT NOT NULL REFERENCES messages(id),
                generation TEXT NOT NULL REFERENCES bindings(generation), native_id TEXT,
                state TEXT NOT NULL, claimed REAL NOT NULL, lease_until REAL NOT NULL,
                finished REAL, detail TEXT
            );
            CREATE TABLE IF NOT EXISTS attempt_evidence (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL REFERENCES attempts(id),
                generation TEXT NOT NULL, outcome TEXT NOT NULL, detail TEXT, observed REAL NOT NULL,
                late INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS receipts (
                id TEXT PRIMARY KEY, message_id TEXT NOT NULL REFERENCES messages(id),
                generation TEXT NOT NULL REFERENCES bindings(generation), observer TEXT NOT NULL,
                kind TEXT NOT NULL, attempt_id TEXT REFERENCES attempts(id), native_id TEXT,
                evidence TEXT, observed REAL NOT NULL,
                UNIQUE(message_id, generation, observer, kind)
            );
            CREATE TABLE IF NOT EXISTS recoveries (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT NOT NULL REFERENCES messages(id),
                previous_generation TEXT NOT NULL, generation TEXT NOT NULL, observed REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cancellations (
                message_id TEXT PRIMARY KEY REFERENCES messages(id), participant TEXT NOT NULL,
                generation TEXT NOT NULL, previous_state TEXT NOT NULL, reason TEXT, observed REAL NOT NULL
            );
            PRAGMA user_version=1;
        """)
        with self._transaction():
            # Additive schema-1 extension: earlier brokers ignore this column.
            if not any(row["name"] == "retry_safe" for row in self._db.execute("PRAGMA table_info(attempts)")):
                self._db.execute("ALTER TABLE attempts ADD COLUMN retry_safe INTEGER")
            self._db.execute("INSERT OR IGNORE INTO settings VALUES ('cursor_secret', ?)", (secrets.token_hex(32),))
            self._cursor_secret = bytes.fromhex(self._db.execute("SELECT value FROM settings WHERE name='cursor_secret'").fetchone()[0])
            self._reconcile(restart=recover_claims)

    def _close_connection(self):
        # Close SQLite first: its locks must not outlive the descriptors that guard them.
        self._db.close()
        shared, self._shared = self._shared, None
        if shared is None:
            return
        with _OPEN_STATE_LOCK:
            shared["users"] -= 1
            if shared["users"]:
                return
            if _OPEN_STATE.get(self.path) is shared:
                del _OPEN_STATE[self.path]
            while shared["descriptors"]:
                os.close(shared["descriptors"].pop()[1])

    def close(self):
        with self._lock:
            self._close_connection()

    def recover_claims(self):
        """The exclusively owned broker startup path, never a client reconnect."""
        with self._transaction():
            self._reconcile(restart=True)

    @contextmanager
    def _transaction(self):
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
            else:
                try:
                    self._db.execute("COMMIT")
                except BaseException:
                    if self._db.in_transaction:
                        self._db.execute("ROLLBACK")
                    raise

    def _reconcile(self, restart=False):
        now = time.time()
        query = "SELECT * FROM attempts WHERE state='claimed'"
        arguments = ()
        if not restart:
            query += " AND lease_until<=?"
            arguments = (now,)
        for attempt in self._db.execute(query, arguments).fetchall():
            detail = _json({"reason": "broker_restart" if restart else "claim_lease_expired"})
            self._db.execute("UPDATE attempts SET state='uncertain',finished=?,detail=? WHERE id=?", (now, detail, attempt["id"]))
            self._db.execute("INSERT INTO attempt_evidence(attempt_id,generation,outcome,detail,observed,late) VALUES (?,?, 'uncertain',?,?,0)", (attempt["id"], attempt["generation"], detail, now))
            self._db.execute("UPDATE messages SET state='uncertain' WHERE id=? AND current_attempt=? AND state='claimed'", (attempt["message_id"], attempt["id"]))
        self._db.execute("UPDATE messages SET state='expired' WHERE expires<=? AND state IN ('queued','failed','uncertain')", (now,))

    def create_space(self, space):
        _identifier(space, "space")
        with self._transaction():
            self._db.execute("INSERT OR IGNORE INTO spaces VALUES (?,?)", (space, time.time()))
        return {"space": space}

    def join(self, space, participant, harness="external", native_id=None, capabilities=None, replace=False, on_issued=None):
        _identifier(space, "space")
        _identifier(participant, "participant")
        _identifier(harness, "harness")
        if native_id is not None:
            _identifier(native_id, "native_id")
        if capabilities is not None and not isinstance(capabilities, dict):
            _fail("invalid_argument", "capabilities must be an object")
        if type(replace) is not bool:
            _fail("invalid_argument", "replace must be a boolean")
        if on_issued is not None and not callable(on_issued):
            _fail("invalid_argument", "on_issued must be a callable local provisioning hook")
        capability_json = _json(capabilities or {}, label="capabilities")
        token = secrets.token_urlsafe(32)
        generation = uuid.uuid4().hex
        with self._transaction():
            if not self._db.execute("SELECT 1 FROM spaces WHERE name=?", (space,)).fetchone():
                _fail("unknown_space", "Create the space before joining")
            previous = self._db.execute("SELECT * FROM participants WHERE space=? AND participant=?", (space, participant)).fetchone()
            if previous:
                if not replace:
                    _fail("participant_exists", "Participant already has a binding; replacement must be explicit even after stop")
                self._retire(previous["generation"])
                self._db.execute("UPDATE participants SET generation=?, visibility_epoch=visibility_epoch+1 WHERE space=? AND participant=?", (generation, space, participant))
            else:
                self._db.execute("INSERT INTO participants(space,participant,generation) VALUES (?,?,?)", (space, participant, generation))
            self._db.execute("""INSERT INTO bindings(generation,space,participant,token_hash,harness,native_id,native_identity_source,capabilities,active,created)
                VALUES (?,?,?,?,?,?,?,?,1,?)""", (generation, space, participant, hashlib.sha256(token.encode()).hexdigest(), harness, native_id, "requested" if native_id else "unobserved", capability_json, time.time()))
            result = self._public_binding(self._db.execute("SELECT * FROM bindings WHERE generation=?", (generation,)).fetchone())
            result["token"] = token
            # The operator saves the only plaintext credential durably before
            # replacement is committed. Failure rolls back retirement and every
            # associated in-flight state change.
            if on_issued is not None:
                on_issued(result)
        return result

    def _retire(self, generation):
        now = time.time()
        self._db.execute("UPDATE bindings SET active=0,stopped=COALESCE(stopped,?) WHERE generation=?", (now, generation))
        for attempt in self._db.execute("SELECT * FROM attempts WHERE generation=? AND state='claimed'", (generation,)).fetchall():
            detail = _json({"reason": "binding_retired"})
            self._db.execute("UPDATE attempts SET state='uncertain',finished=?,detail=? WHERE id=?", (now, detail, attempt["id"]))
            self._db.execute("INSERT INTO attempt_evidence(attempt_id,generation,outcome,detail,observed,late) VALUES (?,?, 'uncertain',?,?,0)", (attempt["id"], generation, detail, now))
            self._db.execute("UPDATE messages SET state='uncertain' WHERE id=? AND current_attempt=? AND state='claimed'", (attempt["message_id"], attempt["id"]))

    def _authenticate(self, token, allow_retired=False):
        if not isinstance(token, str) or len(token) > 512 or not token:
            _fail("unauthorized", "A valid participant credential is required")
        token_hash = hashlib.sha256(token.encode()).hexdigest()
        row = self._db.execute("""SELECT b.*,p.visibility_epoch,p.generation AS current_generation FROM bindings b
            JOIN participants p ON p.space=b.space AND p.participant=b.participant WHERE b.token_hash=?""", (token_hash,)).fetchone()
        if row is None or not hmac.compare_digest(row["token_hash"], token_hash):
            _fail("unauthorized", "Invalid participant credential")
        if not allow_retired and (not row["active"] or row["generation"] != row["current_generation"]):
            _fail("stale_generation", "The participant binding is stopped or superseded")
        return row

    def authenticate(self, token, allow_retired=False):
        with self._transaction():
            row = self._authenticate(token, allow_retired)
            result = self._public_binding(row)
            result["visibility_epoch"] = row["visibility_epoch"]
            result["_token"] = token
            return result

    def _context(self, ctx, allow_retired=False):
        if not isinstance(ctx, dict):
            _fail("unauthorized", "Authenticated broker context is required")
        return self._authenticate(ctx.get("_token"), allow_retired)

    @staticmethod
    def _public_binding(row):
        return {"space": row["space"], "participant": row["participant"], "generation": row["generation"],
                "harness": row["harness"], "native_id": row["native_id"], "native_identity_source": row["native_identity_source"],
                "capabilities": json.loads(row["capabilities"]), "active": bool(row["active"]), "created_at": row["created"]}

    def peers(self, ctx):
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            rows = self._db.execute("""SELECT b.* FROM bindings b JOIN participants p ON b.generation=p.generation
                WHERE b.space=? ORDER BY b.participant""", (principal["space"],)).fetchall()
            return {"peers": [self._public_binding(row) for row in rows], "space": principal["space"]}

    def identity(self, ctx):
        with self._transaction():
            principal = self._context(ctx)
            result = self._public_binding(principal)
            result["visibility_epoch"] = principal["visibility_epoch"]
            return result

    def send(self, ctx, to, body, key, reply_to=None, to_generation=None, metadata=None, expires_in=None):
        _identifier(to, "to")
        _identifier(key, "idempotency key")
        if not isinstance(body, str):
            _fail("invalid_argument", "body must be a string")
        if len(body.encode()) > self.MAX_BODY_BYTES:
            _fail("limit_exceeded", f"body exceeds {self.MAX_BODY_BYTES} bytes")
        if to_generation is not None:
            _identifier(to_generation, "to_generation")
        if reply_to is not None:
            _identifier(reply_to, "reply_to")
        if metadata is not None and not isinstance(metadata, dict):
            _fail("invalid_argument", "metadata must be an object")
        metadata_json = _json(metadata or {})
        if expires_in is not None and (isinstance(expires_in, bool) or not isinstance(expires_in, (int, float)) or not math.isfinite(expires_in) or expires_in <= 0):
            _fail("invalid_argument", "expires_in must be a positive finite number of seconds")
        intent = _json({"to": to, "to_generation": to_generation, "body": body, "reply_to": reply_to,
                        "metadata": json.loads(metadata_json), "expires_in": expires_in}, limit=self.MAX_BODY_BYTES + 20000, label="message")
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            original = self._db.execute("SELECT * FROM messages WHERE space=? AND sender=? AND idempotency_key=?", (principal["space"], principal["participant"], key)).fetchone()
            if original:
                if original["intent"] != intent:
                    _fail("idempotency_conflict", "This sender key already identifies a different message intent")
                result = self._message(original)
                result["idempotent_replay"] = True
                return result
            target = self._db.execute("""SELECT b.* FROM bindings b JOIN participants p ON p.generation=b.generation
                WHERE p.space=? AND p.participant=?""", (principal["space"], to)).fetchone()
            if target is None:
                _fail("unknown_recipient", "The recipient is not registered in this space")
            if not target["active"]:
                _fail("recipient_unavailable", "The recipient has no active binding")
            if to_generation is not None and to_generation != target["generation"]:
                _fail("stale_target", "The requested recipient generation is no longer current")
            if reply_to:
                reference = self._db.execute("SELECT * FROM messages WHERE id=? AND space=?", (reply_to, principal["space"])).fetchone()
                if reference is None or not self._visible(reference, principal):
                    _fail("forbidden", "The reply reference is not visible to this participant")
                if to not in (reference["sender"], reference["recipient"]):
                    _fail("invalid_reply", "A direct reply must address a participant in the referenced exchange")
            pending = self._db.execute("""SELECT COUNT(*) FROM messages m WHERE space=? AND recipient=?
                AND state NOT IN ('expired','cancelled') AND NOT EXISTS(SELECT 1 FROM receipts r WHERE r.message_id=m.id)""", (principal["space"], to)).fetchone()[0]
            if pending >= self.MAX_PENDING:
                _fail("queue_full", "Recipient pending-message limit reached")
            identifier = uuid.uuid4().hex
            now = time.time()
            self._db.execute("""INSERT INTO messages(id,space,sender,sender_generation,recipient,requested_to,requested_generation,
                original_generation,delivery_generation,idempotency_key,intent,body,metadata,reply_to,created,expires,state)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'queued')""", (identifier, principal["space"], principal["participant"], principal["generation"],
                to, to, to_generation, target["generation"], target["generation"], key, intent, body, metadata_json,
                reply_to, now, now + expires_in if expires_in else None))
            result = self._message(self._db.execute("SELECT * FROM messages WHERE id=?", (identifier,)).fetchone())
            result["idempotent_replay"] = False
            return result

    def _message(self, row):
        received, acknowledged = self._db.execute("SELECT COUNT(*),COUNT(CASE WHEN kind IN (?,?) THEN 1 END) FROM receipts WHERE message_id=?",
                                                  (*self.ACK_KINDS, row["id"])).fetchone()
        return {"schema_version": 1, "id": row["id"], "message_id": row["id"], "sequence": row["sequence"], "space": row["space"],
                "from": row["sender"], "sender": row["sender"], "from_generation": row["sender_generation"],
                "to": row["recipient"], "requested_to": row["requested_to"], "requested_generation": row["requested_generation"],
                "to_generation": row["original_generation"], "delivery_generation": row["delivery_generation"],
                "body": row["body"], "key": row["idempotency_key"], "metadata": json.loads(row["metadata"]),
                "reply_to": row["reply_to"], "created_at": row["created"], "expires_at": row["expires"],
                "expired": bool(row["expires"] is not None and row["expires"] <= time.time()),
                "state": row["state"], "accepted": True, "received": bool(received), "acknowledged": bool(acknowledged)}

    def _cursor(self, principal, sequence):
        payload = _json({"v": 1, "s": principal["space"], "p": principal["participant"], "g": principal["generation"],
                         "e": principal["visibility_epoch"], "n": sequence}).encode()
        signature = hmac.new(self._cursor_secret, payload, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(payload + signature).decode().rstrip("=")

    def _read_cursor(self, cursor, principal):
        if cursor is None:
            return 0, False
        if not isinstance(cursor, str) or len(cursor) > 4096:
            _fail("invalid_cursor", "Cursor is invalid")
        try:
            wire = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
            payload, signature = wire[:-32], wire[-32:]
            if not hmac.compare_digest(signature, hmac.new(self._cursor_secret, payload, hashlib.sha256).digest()):
                raise ValueError("signature")
            parsed = json.loads(payload)
            if parsed["v"] != 1 or parsed["s"] != principal["space"] or parsed["p"] != principal["participant"]:
                raise ValueError("scope")
            sequence = parsed["n"]
            if type(sequence) is not int or sequence < 0:
                raise ValueError("position")
            if parsed["g"] != principal["generation"] or parsed["e"] != principal["visibility_epoch"]:
                return 0, True
            return sequence, False
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            _fail("invalid_cursor", "Cursor signature, scope, or position is invalid")

    def inbox(self, ctx, cursor=None, limit=20, unacknowledged=False):
        if type(limit) is not int or limit < 1 or limit > self.MAX_PAGE_SIZE:
            _fail("limit_exceeded", f"limit must be between 1 and {self.MAX_PAGE_SIZE}")
        if type(unacknowledged) is not bool:
            _fail("invalid_argument", "unacknowledged must be a boolean")
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            sequence, resync = self._read_cursor(cursor, principal)
            # One fixed filter serves both queries; the flag selects the
            # unacknowledged view. Native insertion alone is not the
            # recipient's acknowledgement.
            visible = """FROM messages m WHERE space=? AND recipient=? AND delivery_generation=? AND sequence>?
                AND (?=0 OR NOT EXISTS(SELECT 1 FROM receipts r WHERE r.message_id=m.id AND r.kind IN (?,?)))"""
            arguments = (principal["space"], principal["participant"], principal["generation"], sequence, int(unacknowledged), *self.ACK_KINDS)
            # Cancellation stops delivery, so a cancelled body never reaches
            # the recipient through inbox or wait. The state is terminal.
            rows = self._db.execute(f"SELECT m.* {visible} AND m.state!='cancelled' ORDER BY sequence LIMIT ?", (*arguments, limit + 1)).fetchall()
            page = rows[:limit]
            has_more = len(rows) > limit
            position = page[-1]["sequence"] if page else sequence
            if not has_more:
                # Advance past trailing cancelled rows so the cursor reflects
                # everything already disposed of.
                skipped = self._db.execute(f"SELECT MAX(m.sequence) {visible} AND m.state='cancelled'", arguments).fetchone()[0]
                position = max(position, skipped or 0)
            following = self._cursor(principal, position)
            return {"messages": [self._message(row) for row in page], "cursor": following, "next_cursor": following,
                    "resume_cursor": following, "has_more": has_more, "resync_required": resync,
                    "visibility_epoch": principal["visibility_epoch"], "position": position}

    @staticmethod
    def _visible(message, principal):
        return message["space"] == principal["space"] and (message["sender"] == principal["participant"] or
                (message["recipient"] == principal["participant"] and message["delivery_generation"] == principal["generation"]))

    def _get_message(self, identifier, principal, recipient_only=False):
        _identifier(identifier, "message_id")
        message = self._db.execute("SELECT * FROM messages WHERE id=?", (identifier,)).fetchone()
        authorized = message is not None and self._visible(message, principal)
        if recipient_only:
            authorized = message is not None and message["space"] == principal["space"] and message["recipient"] == principal["participant"] and message["delivery_generation"] == principal["generation"]
        if not authorized:
            _fail("forbidden", "The message is not authorized for this participant binding")
        return message

    def _own_attempt(self, attempt_id, message, principal):
        _identifier(attempt_id, "attempt_id")
        attempt = self._db.execute("SELECT * FROM attempts WHERE id=? AND message_id=?", (attempt_id, message["id"])).fetchone()
        if attempt is None or attempt["generation"] != principal["generation"]:
            _fail("forbidden", "The attempt does not belong to this message and recipient generation")
        return attempt

    def ack(self, ctx, message_id, attempt_id=None):
        """Record the client's acknowledgement; it never proves native ingestion."""
        with self._transaction():
            principal = self._context(ctx)
            message = self._get_message(message_id, principal, recipient_only=True)
            if attempt_id is not None:
                self._own_attempt(attempt_id, message, principal)
            self._db.execute("""INSERT OR IGNORE INTO receipts(id,message_id,generation,observer,kind,attempt_id,native_id,observed)
                VALUES (?,?,?,?, 'client_ack',?,?,?)""", (uuid.uuid4().hex, message_id, principal["generation"], principal["participant"], attempt_id, principal["native_id"], time.time()))
            return {"message_id": message_id, "acknowledged": True, "receipt": self._receipt(self._db.execute("SELECT * FROM receipts WHERE message_id=? AND generation=? AND kind='client_ack'", (message_id, principal["generation"])).fetchone()), "state": message["state"]}

    def observe(self, ctx, message_id, attempt_id, kind, evidence):
        """Record an adapter's independent native observation of one attempt.

        ``native_insertion`` means content was seen entering the bound native
        session; ``native_ack`` means a correlated native agent acknowledgement.
        Neither changes the attempt's recorded submission outcome.
        """
        if kind not in self.OBSERVED_KINDS:
            _fail("invalid_argument", "kind must be native_insertion or native_ack")
        if not isinstance(evidence, dict) or not evidence:
            _fail("invalid_argument", "evidence must be a nonempty object describing the observation")
        evidence_json = _json(evidence, label="receipt evidence")
        with self._transaction():
            principal = self._context(ctx)
            message = self._get_message(message_id, principal, recipient_only=True)
            attempt = self._own_attempt(attempt_id, message, principal)
            if principal["native_id"] is None or attempt["native_id"] != principal["native_id"]:
                _fail("native_identity_required", "Native observations require the attempt's bound native session identity")
            if attempt["state"] == "failed":
                _fail("attempt_failed", "A definitively failed attempt cannot carry native receipt evidence")
            self._db.execute("""INSERT OR IGNORE INTO receipts(id,message_id,generation,observer,kind,attempt_id,native_id,evidence,observed)
                VALUES (?,?,?,?,?,?,?,?,?)""", (uuid.uuid4().hex, message_id, principal["generation"], principal["participant"], kind,
                                               attempt_id, principal["native_id"], evidence_json, time.time()))
            receipt = self._db.execute("SELECT * FROM receipts WHERE message_id=? AND generation=? AND observer=? AND kind=?",
                                       (message_id, principal["generation"], principal["participant"], kind)).fetchone()
            return {"message_id": message_id, "received": True, "receipt": self._receipt(receipt), "state": message["state"]}

    @staticmethod
    def _receipt(row):
        return {"id": row["id"], "message_id": row["message_id"], "generation": row["generation"], "observer": row["observer"],
                "kind": row["kind"], "attempt_id": row["attempt_id"], "native_id": row["native_id"],
                "evidence": json.loads(row["evidence"]) if row["evidence"] else None, "observed_at": row["observed"]}

    def status(self, ctx, message_id, limit=None):
        """Report delivery history; ``limit`` keeps only the newest entries of each list.

        A limited report adds each list's total and ``truncated``. Uncertainty
        always reflects the complete history, including omitted entries.
        """
        if limit is not None and (type(limit) is not int or limit < 1 or limit > self.MAX_PAGE_SIZE):
            _fail("limit_exceeded", f"limit must be between 1 and {self.MAX_PAGE_SIZE}")
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            message = self._get_message(message_id, principal)
            result = self._message(message)
            totals = {}

            def rows(name, query, arguments, order):
                if limit is None:
                    return self._db.execute(f"SELECT * {query} ORDER BY {order}", arguments).fetchall()
                totals[name] = self._db.execute(f"SELECT COUNT(*) {query}", arguments).fetchone()[0]
                newest = ", ".join(column + " DESC" for column in order.split(","))
                selected = self._db.execute(f"SELECT * {query} ORDER BY {newest} LIMIT ?", (*arguments, limit)).fetchall()
                return selected[::-1]

            result["attempts"] = [self._attempt(row) for row in rows("attempts", "FROM attempts WHERE message_id=?", (message_id,), "claimed,id")]
            for attempt in result["attempts"]:
                attempt["evidence"] = [{"outcome": row["outcome"], "detail": json.loads(row["detail"]) if row["detail"] else None,
                                        "generation": row["generation"], "observed_at": row["observed"], "late": bool(row["late"])}
                                       for row in rows("evidence", "FROM attempt_evidence WHERE attempt_id=?", (attempt["id"],), "sequence")]
                if limit is not None:
                    attempt["evidence_total"] = totals.pop("evidence")
            result["receipts"] = [self._receipt(row) for row in rows("receipts", "FROM receipts WHERE message_id=?", (message_id,), "observed,id")]
            # The same visibility rule as _visible(), applied before limiting.
            result["replies"] = [self._message(row) for row in rows("replies", """FROM messages WHERE reply_to=? AND space=?
                AND (sender=? OR (recipient=? AND delivery_generation=?))""", (message_id, principal["space"], principal["participant"],
                principal["participant"], principal["generation"]), "sequence")]
            # Kinds cover every receipt, including any a limit omits.
            result["receipt_kinds"] = [row[0] for row in self._db.execute(
                "SELECT DISTINCT kind FROM receipts WHERE message_id=? ORDER BY kind", (message_id,))]
            result["recoveries"] = [dict(row) for row in rows("recoveries", "FROM recoveries WHERE message_id=?", (message_id,), "sequence")]
            result["cancellation"] = self._cancellation(message_id)
            result["uncertain"] = message["state"] == "uncertain" or self._db.execute("""SELECT EXISTS(SELECT 1 FROM attempts a
                WHERE a.message_id=? AND (a.state='uncertain' OR EXISTS(SELECT 1 FROM attempt_evidence e
                WHERE e.attempt_id=a.id AND e.outcome='uncertain')))""", (message_id,)).fetchone()[0] == 1
            result["unresolved_uncertainty"] = message["state"] == "uncertain" and not result["received"]
            if limit is not None:
                result["limit"] = limit
                result["totals"] = totals
                result["truncated"] = any(totals[name] > len(result[name]) for name in totals) or any(
                    attempt["evidence_total"] > len(attempt["evidence"]) for attempt in result["attempts"])
            return result

    @staticmethod
    def _attempt(row):
        return {"id": row["id"], "attempt_id": row["id"], "message_id": row["message_id"], "generation": row["generation"],
                "native_id": row["native_id"], "state": row["state"], "claimed_at": row["claimed"],
                "lease_until": row["lease_until"], "finished_at": row["finished"], "detail": json.loads(row["detail"]) if row["detail"] else None,
                "retry_safe": None if row["retry_safe"] is None else bool(row["retry_safe"])}

    def _cancellation(self, message_id):
        row = self._db.execute("SELECT * FROM cancellations WHERE message_id=?", (message_id,)).fetchone()
        if row is None:
            return None
        # Only a failure affirmatively reported retry-safe proves nothing
        # reached the native session; unknown or unsafe failures may have.
        submitted = self._db.execute("""SELECT 1 FROM attempts WHERE message_id=? AND (state IN ('submitted','uncertain')
            OR (state='failed' AND (retry_safe IS NULL OR retry_safe=0)))""", (message_id,)).fetchone()
        return {"participant": row["participant"], "generation": row["generation"], "previous_state": row["previous_state"],
                "reason": row["reason"], "cancelled_at": row["observed"],
                # Cancellation cannot retract content a native session may already hold.
                "may_have_been_submitted": submitted is not None}

    def cancel(self, ctx, message_id, reason=None):
        """Sender-authorized terminal stop of further delivery; history is retained."""
        if reason is not None and (not isinstance(reason, str) or not reason or len(reason.encode()) > 1000):
            _fail("invalid_argument", "reason must be a nonempty string of at most 1000 bytes")
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            message = self._get_message(message_id, principal)
            if message["sender"] != principal["participant"]:
                _fail("forbidden", "Only the authenticated sender can cancel a pending delivery")
            if message["state"] != "cancelled":
                if self._db.execute("SELECT 1 FROM receipts WHERE message_id=?", (message_id,)).fetchone():
                    _fail("already_received", "A received message has no pending delivery to cancel")
                if message["state"] == "expired":
                    _fail("expired", "Expiry already stopped delivery of this message")
                if message["state"] == "claimed":
                    _fail("in_flight", "A claimed attempt is in flight; cancel after its outcome is recorded")
                if message["state"] == "submitted":
                    _fail("already_submitted", "The native session accepted this message; cancellation cannot retract it")
                self._db.execute("INSERT INTO cancellations(message_id,participant,generation,previous_state,reason,observed) VALUES (?,?,?,?,?,?)",
                                 (message_id, principal["participant"], principal["generation"], message["state"], reason, time.time()))
                self._db.execute("UPDATE messages SET state='cancelled' WHERE id=?", (message_id,))
            result = self._message(self._db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone())
            result["cancellation"] = self._cancellation(message_id)
            result["idempotent_replay"] = message["state"] == "cancelled"
            return result

    def retry(self, ctx, message_id):
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            message = self._get_message(message_id, principal)
            if message["sender"] != principal["participant"]:
                _fail("forbidden", "Only the authenticated sender can authorize a delivery retry")
            if self._db.execute("SELECT 1 FROM receipts WHERE message_id=?", (message_id,)).fetchone():
                _fail("already_received", "A receipt suppresses further native submission")
            if message["state"] == "cancelled":
                _fail("cancelled", "A cancelled message cannot be retried")
            if message["state"] not in ("uncertain", "failed"):
                _fail("retry_ineligible", "Only an uncertain or definitively failed delivery may be retried")
            if message["expires"] is not None and message["expires"] <= time.time():
                _fail("expired", "Expired messages cannot be retried")
            self._db.execute("UPDATE messages SET state='queued' WHERE id=?", (message_id,))
            return self._message(self._db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone())

    def claim(self, ctx):
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            message = self._db.execute("""SELECT m.* FROM messages m WHERE space=? AND recipient=? AND delivery_generation=?
                AND state='queued' AND NOT EXISTS(SELECT 1 FROM receipts r WHERE r.message_id=m.id)
                ORDER BY sequence LIMIT 1""", (principal["space"], principal["participant"], principal["generation"])).fetchone()
            if message is None:
                return {"message": None, "attempt": None, "attempt_id": None}
            identifier = uuid.uuid4().hex
            now = time.time()
            self._db.execute("""INSERT INTO attempts(id,message_id,generation,native_id,state,claimed,lease_until)
                VALUES (?,?,?,?,'claimed',?,?)""", (identifier, message["id"], principal["generation"], principal["native_id"], now, now + self.CLAIM_SECONDS))
            self._db.execute("UPDATE messages SET state='claimed',current_attempt=? WHERE id=?", (identifier, message["id"]))
            message = self._db.execute("SELECT * FROM messages WHERE id=?", (message["id"],)).fetchone()
            attempt = self._db.execute("SELECT * FROM attempts WHERE id=?", (identifier,)).fetchone()
            return {"message": self._message(message), "attempt": self._attempt(attempt), "attempt_id": identifier}

    def finish(self, ctx, message_id, attempt_id, outcome, detail=None, retry_safe=None):
        """Settle an attempt. A definite failure may state whether replay is
        safe: true when evidence shows nothing reached the native session,
        false when a retry could duplicate effects, None when not established."""
        if outcome not in ("submitted", "uncertain", "failed"):
            _fail("invalid_argument", "outcome must be submitted, uncertain, or failed")
        if retry_safe is not None and (type(retry_safe) is not bool or outcome != "failed"):
            _fail("invalid_argument", "retry_safe is a boolean reported only with a failed outcome")
        _identifier(attempt_id, "attempt_id")
        detail_json = _json(detail, label="attempt detail") if detail is not None else None
        stale = False
        with self._transaction():
            principal = self._context(ctx, allow_retired=True)
            self._reconcile()
            attempt = self._db.execute("SELECT * FROM attempts WHERE id=? AND message_id=?", (attempt_id, message_id)).fetchone()
            message = self._db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
            if attempt is None or message is None or attempt["generation"] != principal["generation"] or message["space"] != principal["space"] or message["recipient"] != principal["participant"]:
                _fail("forbidden", "The attempt does not belong to this authenticated recipient generation")
            eligible_transition = attempt["state"] == "claimed" or (attempt["state"], outcome) in (
                ("submitted", "uncertain"), ("uncertain", "submitted"))
            stale = (not principal["active"] or principal["generation"] != principal["current_generation"] or
                     message["delivery_generation"] != principal["generation"] or message["current_attempt"] != attempt_id or
                     attempt["native_id"] != principal["native_id"] or not eligible_transition or
                     message["state"] not in ("claimed", "submitted", "uncertain"))
            self._db.execute("INSERT INTO attempt_evidence(attempt_id,generation,outcome,detail,observed,late) VALUES (?,?,?,?,?,?)", (attempt_id, principal["generation"], outcome, detail_json, time.time(), int(stale)))
            if not stale:
                self._db.execute("UPDATE attempts SET state=?,finished=?,detail=?,retry_safe=? WHERE id=?",
                                 (outcome, time.time(), detail_json, None if retry_safe is None else int(retry_safe), attempt_id))
                self._db.execute("UPDATE messages SET state=? WHERE id=?", (outcome, message_id))
                result = {"message_id": message_id, "attempt_id": attempt_id, "state": outcome}
        if stale:
            _fail("stale_attempt", "Late evidence was retained against its original attempt; it cannot settle current delivery")
        return result

    def bind(self, ctx, native_id, capabilities=None):
        _identifier(native_id, "native_id")
        if capabilities is not None and not isinstance(capabilities, dict):
            _fail("invalid_argument", "capabilities must be an object")
        with self._transaction():
            principal = self._context(ctx)
            merged_capabilities = json.loads(principal["capabilities"])
            if capabilities is not None:
                merged_capabilities.update(capabilities)
            capability_json = _json(merged_capabilities, label="capabilities")
            if principal["native_id"] is not None and principal["native_id"] != native_id:
                _fail("replacement_required", "Changing native session identity requires explicit participant replacement")
            # Setting an identity while an unbound submission is in flight would
            # invalidate its captured identity. Require the caller to settle it.
            if principal["native_id"] is None and self._db.execute("SELECT 1 FROM attempts WHERE generation=? AND state='claimed'", (principal["generation"],)).fetchone():
                _fail("binding_busy", "Settle in-flight attempts before establishing native identity")
            self._db.execute("UPDATE bindings SET native_id=?,native_identity_source='adapter_reported',capabilities=COALESCE(?,capabilities) WHERE generation=?", (native_id, capability_json, principal["generation"]))
            return self._public_binding(self._db.execute("SELECT * FROM bindings WHERE generation=?", (principal["generation"],)).fetchone())

    def stop(self, ctx):
        with self._transaction():
            principal = self._context(ctx)
            self._retire(principal["generation"])
            return {"space": principal["space"], "participant": principal["participant"], "generation": principal["generation"], "active": False}

    def recover(self, ctx, message_id):
        with self._transaction():
            principal = self._context(ctx)
            self._reconcile()
            message = self._db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
            if message is None or message["space"] != principal["space"] or message["recipient"] != principal["participant"]:
                _fail("forbidden", "Only the current recipient participant may recover a historical message")
            if message["delivery_generation"] == principal["generation"]:
                return {"message": self._message(message), "recovered": False, "visibility_epoch": principal["visibility_epoch"],
                        "retry_required": message["state"] in ("uncertain", "failed")}
            if self._db.execute("SELECT 1 FROM receipts WHERE message_id=?", (message_id,)).fetchone():
                _fail("already_received", "Receipt-bearing messages cannot be transferred as pending work")
            if message["expires"] is not None and message["expires"] <= time.time():
                _fail("expired", "Expired messages cannot be transferred as pending work")
            if message["state"] == "cancelled":
                _fail("cancelled", "Cancelled messages cannot be transferred as pending work")
            self._db.execute("INSERT INTO recoveries(message_id,previous_generation,generation,observed) VALUES (?,?,?,?)", (message_id, message["delivery_generation"], principal["generation"], time.time()))
            state = "uncertain" if message["state"] in ("claimed", "submitted") else message["state"]
            self._db.execute("UPDATE messages SET delivery_generation=?,state=? WHERE id=?", (principal["generation"], state, message_id))
            self._db.execute("UPDATE participants SET visibility_epoch=visibility_epoch+1 WHERE space=? AND participant=?", (principal["space"], principal["participant"]))
            epoch = principal["visibility_epoch"] + 1
            return {"message": self._message(self._db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()),
                    "recovered": True, "visibility_epoch": epoch, "retry_required": state in ("uncertain", "failed")}

    def space_view(self, space):
        """Bounded local operator summary of one space; it changes no state.

        Counts cover messages without any receipt, except per-state totals.
        ``recovery`` counts messages bound to a generation that is no longer the
        participant's active binding; they need explicit disposition.
        """
        _identifier(space, "space")
        with self._lock:
            self._db.execute("BEGIN")
            try:
                if not self._db.execute("SELECT 1 FROM spaces WHERE name=?", (space,)).fetchone():
                    _fail("unknown_space", "The space does not exist")
                participants = {}
                for row in self._db.execute("""SELECT b.*,p.visibility_epoch FROM participants p
                        JOIN bindings b ON b.generation=p.generation WHERE p.space=? ORDER BY p.participant""", (space,)):
                    entry = self._public_binding(row)
                    entry.update(visibility_epoch=row["visibility_epoch"], generations=[], states={}, pending=0, uncertain=0,
                                 waiting=dict.fromkeys(("delivery", "in_flight", "receipt", "explicit_retry", "recovery"), 0))
                    participants[row["participant"]] = entry
                for row in self._db.execute("SELECT * FROM bindings WHERE space=? ORDER BY created,generation", (space,)):
                    participants[row["participant"]]["generations"].append(
                        {"generation": row["generation"], "active": bool(row["active"]), "native_id": row["native_id"],
                         "created_at": row["created"], "stopped_at": row["stopped"]})
                for row in self._db.execute("""SELECT m.recipient,m.state,m.delivery_generation=p.generation AND b.active AS current,
                        EXISTS(SELECT 1 FROM receipts r WHERE r.message_id=m.id) AS received,COUNT(*) AS count
                        FROM messages m JOIN participants p ON p.space=m.space AND p.participant=m.recipient
                        JOIN bindings b ON b.generation=p.generation WHERE m.space=? GROUP BY 1,2,3,4""", (space,)):
                    entry, state, count = participants[row["recipient"]], row["state"], row["count"]
                    entry["states"][state] = entry["states"].get(state, 0) + count
                    if row["received"] or state in ("expired", "cancelled"):
                        continue
                    entry["pending"] += count
                    if state == "uncertain":
                        entry["uncertain"] += count
                    waiting = "recovery" if not row["current"] else {
                        "queued": "delivery", "claimed": "in_flight", "submitted": "receipt"}.get(state, "explicit_retry")
                    entry["waiting"][waiting] += count
            finally:
                self._db.execute("COMMIT")
        waiting = {}
        for entry in participants.values():
            for name, count in entry["waiting"].items():
                waiting[name] = waiting.get(name, 0) + count
        return {"space": space, "participants": list(participants.values()),
                "pending": sum(entry["pending"] for entry in participants.values()),
                "uncertain": sum(entry["uncertain"] for entry in participants.values()),
                "waiting": waiting, "observed_at": time.time(), "native_communication_verified": False}
