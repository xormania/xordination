"""Observable durability, authority, and replay contracts from the specification."""

import concurrent.futures
import base64
from contextlib import closing
import json
import os
import queue
import sqlite3
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from xordination.store import Store, StoreError


class StoreContracts(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "broker.sqlite"
        self.store = Store(self.path)
        self.store.create_space("project")
        self.a_join = self.store.join("project", "a", harness="codex", native_id="thread-a")
        self.b_join = self.store.join("project", "b", harness="claude", native_id="session-b")
        self.a = self.store.authenticate(self.a_join["token"])
        self.b = self.store.authenticate(self.b_join["token"])

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def send(self, key="message", **kwargs):
        return self.store.send(self.a, "b", "A question", key, **kwargs)

    def assertCode(self, code, function, *args, **kwargs):
        with self.assertRaises(StoreError) as failure:
            function(*args, **kwargs)
        self.assertEqual(code, failure.exception.code)

    def test_acceptance_survives_restart_and_inbox_is_not_consumption(self):
        accepted = self.send()
        self.store.close()
        self.store = Store(self.path)
        self.store.recover_claims()
        self.assertEqual(accepted["id"], self.send()["id"])
        first = self.store.inbox(self.b)
        second = self.store.inbox(self.b)
        self.assertEqual([accepted["id"]], [m["id"] for m in first["messages"]])
        self.assertEqual(first["messages"], second["messages"])
        receipt = self.store.ack(self.b, accepted["id"])
        self.assertEqual(receipt["receipt"]["id"], self.store.ack(self.b, accepted["id"])["receipt"]["id"])
        self.assertEqual(1, len(self.store.inbox(self.b)["messages"]))
        self.assertEqual([], self.store.inbox(self.b, unacknowledged=True)["messages"])

    def test_idempotency_uses_original_intent_before_binding_resolution(self):
        accepted = self.send(metadata={"job": 1})
        replacement = self.store.join("project", "b", replace=True, native_id="session-new")
        replay = self.send(metadata={"job": 1})
        self.assertEqual(accepted["id"], replay["id"])
        self.assertEqual(accepted["to_generation"], replay["to_generation"])
        self.assertNotEqual(replacement["generation"], replay["to_generation"])
        self.assertCode("idempotency_conflict", self.send, metadata={"job": 2})
        self.assertCode("idempotency_conflict", self.send, metadata={"job": 1}, to_generation=replacement["generation"])
        self.store.stop(self.store.authenticate(replacement["token"]))
        self.assertEqual(accepted["id"], self.send(metadata={"job": 1})["id"])

    def test_replacement_fences_captured_context_and_requires_recovery(self):
        message = self.send()
        with closing(Store(self.path)) as operator:
            new_join = operator.join("project", "b", replace=True)
        new = self.store.authenticate(new_join["token"])
        self.assertCode("stale_generation", self.store.send, self.b, "a", "stale", "stale")
        self.assertCode("stale_generation", self.store.claim, self.b)
        self.assertCode("stale_generation", self.store.ack, self.b, message["id"])
        self.assertEqual([], self.store.inbox(new)["messages"])
        self.assertCode("forbidden", self.store.status, new, message["id"])
        recovered = self.store.recover(new, message["id"])
        self.assertEqual(message["to_generation"], recovered["message"]["to_generation"])
        self.assertEqual(new["generation"], recovered["message"]["delivery_generation"])
        self.assertEqual(message["id"], self.store.claim(new)["message"]["id"])
        self.assertEqual(new["generation"], self.store.ack(new, message["id"])["receipt"]["generation"])

    def test_stopped_participant_requires_explicit_replacement_before_recovery(self):
        message = self.send()
        self.store.stop(self.b)
        self.assertCode("participant_exists", self.store.join, "project", "b")
        retired = self.store.authenticate(self.b_join["token"], allow_retired=True)
        self.assertEqual(self.b["generation"], retired["generation"])
        self.assertFalse(retired["active"])
        unchanged = self.store.status(self.a, message["id"])
        self.assertEqual(message["to_generation"], unchanged["delivery_generation"])
        self.assertEqual("queued", unchanged["state"])
        peer = next(peer for peer in self.store.peers(self.a)["peers"] if peer["participant"] == "b")
        self.assertEqual(self.b["generation"], peer["generation"])
        self.assertFalse(peer["active"])
        replacement = self.store.join("project", "b", replace=True)
        new = self.store.authenticate(replacement["token"])
        self.assertNotEqual(retired["generation"], new["generation"])
        self.assertEqual([], self.store.inbox(new)["messages"])
        self.store.recover(new, message["id"])
        self.assertEqual(message["id"], self.store.claim(new)["message"]["id"])

    def test_ambiguous_restart_requires_explicit_sender_retry(self):
        message = self.send()
        first = self.store.claim(self.b)
        self.store.close()
        self.store = Store(self.path)
        self.store.recover_claims()
        status = self.store.status(self.a, message["id"])
        self.assertEqual("uncertain", status["state"])
        self.assertIsNone(self.store.claim(self.b)["message"])
        self.assertCode("forbidden", self.store.retry, self.b, message["id"])
        self.store.retry(self.a, message["id"])
        second = self.store.claim(self.b)
        self.assertNotEqual(first["attempt_id"], second["attempt_id"])
        self.assertEqual(message["id"], second["message"]["id"])
        self.assertCode("stale_attempt", self.store.finish, self.b, message["id"], first["attempt_id"], "submitted", {"native_turn": "old-turn"})
        status = self.store.status(self.a, message["id"])
        self.assertEqual("claimed", status["state"])
        self.assertEqual("uncertain", status["attempts"][0]["state"])
        self.assertTrue(status["attempts"][0]["evidence"][-1]["late"])
        self.store.finish(self.b, message["id"], second["attempt_id"], "submitted")
        self.assertEqual("submitted", self.store.status(self.a, message["id"])["state"])

    def test_unclean_claim_writer_restart_requires_explicit_retry(self):
        """Exercise an unclean SQLite writer and explicit core reconciliation."""
        message = self.send()
        self.store.close()
        env = dict(os.environ, XORDINATION_TEST_TOKEN=self.b_join["token"],
                   PYTHONDONTWRITEBYTECODE="1")
        source = str(Path(__file__).resolve().parents[1] / "src")
        env["PYTHONPATH"] = source + os.pathsep + env.get("PYTHONPATH", "")
        code = """
import json, os, sys
from xordination.store import Store
store = Store(sys.argv[1])
context = store.authenticate(os.environ['XORDINATION_TEST_TOKEN'])
claimed = store.claim(context)
print(json.dumps({'message_id': claimed['message']['id'], 'attempt_id': claimed['attempt_id']}), flush=True)
sys.stdin.buffer.read(1)
"""
        with tempfile.TemporaryFile(mode="w+b") as stderr:
            process = subprocess.Popen([sys.executable, "-u", "-c", code, str(self.path)],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=stderr, env=env, text=True)
            ready = queue.Queue()
            reader = threading.Thread(target=lambda: ready.put(process.stdout.readline()), daemon=True)
            reader.start()
            try:
                notification = ready.get(timeout=5)
                self.assertTrue(notification, "Claim writer closed stdout before committing its claim")
                claimed = json.loads(notification)
                self.assertEqual(message["id"], claimed["message_id"])
                self.assertIsNone(process.poll())
                process.kill()
                self.assertNotEqual(0, process.wait(timeout=5))
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                reader.join(timeout=2)
                process.stdin.close()
                process.stdout.close()
        self.store = Store(self.path)
        persisted = self.store.status(self.a, message["id"])
        self.assertEqual("claimed", persisted["state"])
        self.assertEqual(claimed["attempt_id"], persisted["attempts"][0]["id"])
        self.store.recover_claims()
        self.assertEqual("uncertain", self.store.status(self.a, message["id"])["state"])
        self.assertIsNone(self.store.claim(self.b)["message"])
        self.store.retry(self.a, message["id"])
        retry = self.store.claim(self.b)
        self.assertEqual(message["id"], retry["message"]["id"])
        self.assertNotEqual(claimed["attempt_id"], retry["attempt_id"])

    def test_receipt_and_transport_uncertainty_remain_independent(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "uncertain", {"reason": "write response lost"})
        self.store.ack(self.b, message["id"])
        status = self.store.status(self.a, message["id"])
        self.assertTrue(status["acknowledged"])
        self.assertTrue(status["uncertain"])
        self.assertEqual("uncertain", status["attempts"][0]["state"])
        self.assertEqual("client_ack", status["receipts"][0]["kind"])
        self.assertCode("already_received", self.store.retry, self.a, message["id"])
        self.assertIsNone(self.store.claim(self.b)["message"])

    def test_submitted_transport_can_become_uncertain_without_automatic_replay(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "submitted", {"native_request": "written"})
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "uncertain", {"native_response": "connection lost"})
        status = self.store.status(self.a, message["id"])
        self.assertEqual("uncertain", status["state"])
        self.assertEqual(["submitted", "uncertain"], [event["outcome"] for event in status["attempts"][0]["evidence"]])
        self.assertIsNone(self.store.claim(self.b)["message"])
        self.store.retry(self.a, message["id"])
        self.assertCode("stale_attempt", self.store.finish, self.b, message["id"], attempt["attempt_id"], "submitted")

    def test_failed_retry_and_late_confirmation_preserve_attempt_history(self):
        message = self.send()
        failed = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], failed["attempt_id"], "failed", {"reason": "before_native_write"})
        self.store.retry(self.a, message["id"])
        retry = self.store.claim(self.b)
        self.assertNotEqual(failed["attempt_id"], retry["attempt_id"])
        self.store.finish(self.b, message["id"], retry["attempt_id"], "uncertain")
        self.store.finish(self.b, message["id"], retry["attempt_id"], "submitted", {"native_response": "confirmed"})
        status = self.store.status(self.a, message["id"])
        self.assertEqual("submitted", status["state"])
        self.assertEqual(["failed", "submitted"], [attempt["state"] for attempt in status["attempts"]])
        self.assertEqual(["uncertain", "submitted"], [event["outcome"] for event in status["attempts"][1]["evidence"]])
        self.assertTrue(status["uncertain"])
        self.assertFalse(status["unresolved_uncertainty"])
        self.assertFalse(status["acknowledged"])
        self.assertIsNone(self.store.claim(self.b)["message"])

    def test_recovery_of_submitted_unacknowledged_message_requires_explicit_retry(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "submitted")
        joined = self.store.join("project", "b", replace=True)
        new = self.store.authenticate(joined["token"])
        recovery = self.store.recover(new, message["id"])
        self.assertTrue(recovery["retry_required"])
        self.assertEqual("uncertain", recovery["message"]["state"])
        self.assertEqual("submitted", self.store.status(self.a, message["id"])["attempts"][0]["state"])
        self.assertIsNone(self.store.claim(new)["message"])
        self.store.retry(self.a, message["id"])
        self.assertEqual(message["id"], self.store.claim(new)["message"]["id"])

    def test_recovery_cannot_transfer_received_or_expired_messages(self):
        received = self.send("received")
        receipt = self.store.ack(self.b, received["id"])["receipt"]
        expired = self.send("expired", expires_in=10)
        replacement = self.store.join("project", "b", replace=True)
        new = self.store.authenticate(replacement["token"])
        epoch = self.store.identity(new)["visibility_epoch"]
        self.assertCode("already_received", self.store.recover, new, received["id"])
        with patch("xordination.store.time.time", return_value=time.time() + 11):
            self.assertCode("expired", self.store.recover, new, expired["id"])
        self.assertEqual(epoch, self.store.identity(new)["visibility_epoch"])
        self.assertEqual([], self.store.inbox(new)["messages"])
        for message in (received, expired):
            with self.subTest(message=message["key"]):
                status = self.store.status(self.a, message["id"])
                self.assertEqual(message["to_generation"], status["delivery_generation"])
                self.assertEqual([], status["recoveries"])
        self.assertEqual([receipt], self.store.status(self.a, received["id"])["receipts"])

    def test_delayed_retired_attempt_cannot_settle_recovered_generation(self):
        message = self.send()
        old_attempt = self.store.claim(self.b)
        new_join = self.store.join("project", "b", replace=True, native_id="new-session")
        new = self.store.authenticate(new_join["token"])
        self.store.recover(new, message["id"])
        self.store.retry(self.a, message["id"])
        new_attempt = self.store.claim(new)
        self.assertCode("stale_attempt", self.store.finish, self.b, message["id"], old_attempt["attempt_id"], "submitted")
        self.assertEqual("claimed", self.store.status(self.a, message["id"])["state"])
        self.assertEqual(new["generation"], self.store.status(new, message["id"])["attempts"][-1]["generation"])
        self.store.finish(new, message["id"], new_attempt["attempt_id"], "submitted")

    def test_cursor_retains_empty_page_position_and_handles_new_insertion(self):
        expected = [self.send(str(n))["id"] for n in range(5)]
        seen = []
        cursor = None
        while True:
            page = self.store.inbox(self.b, cursor=cursor, limit=2)
            seen += [message["id"] for message in page["messages"]]
            cursor = page["cursor"]
            if len(seen) == 2:
                expected.append(self.send("between-pages")["id"])
            if not page["has_more"]:
                break
        empty = self.store.inbox(self.b, cursor=cursor, limit=2)
        self.assertEqual(cursor, empty["cursor"])
        inserted = self.send("inserted")
        new = self.store.inbox(self.b, cursor=empty["cursor"], limit=2)
        self.assertEqual(expected, seen)
        self.assertEqual([inserted["id"]], [message["id"] for message in new["messages"]])

    def test_recovery_invalidates_high_water_to_cover_newly_visible_old_message(self):
        old = self.send("old")
        joined = self.store.join("project", "b", replace=True)
        new = self.store.authenticate(joined["token"])
        recent = self.send("recent")
        initial = self.store.inbox(new)
        self.assertEqual([recent["id"]], [m["id"] for m in initial["messages"]])
        self.store.recover(new, old["id"])
        resync = self.store.inbox(new, cursor=initial["cursor"])
        self.assertTrue(resync["resync_required"])
        self.assertEqual([old["id"], recent["id"]], [m["id"] for m in resync["messages"]])

    def test_authorization_binds_token_space_recipient_and_reply(self):
        message = self.send()
        outsider_join = self.store.join("project", "outsider")
        outsider = self.store.authenticate(outsider_join["token"])
        self.store.create_space("other")
        other = self.store.authenticate(self.store.join("other", "a")["token"])
        self.assertCode("forbidden", self.store.status, outsider, message["id"])
        self.assertCode("forbidden", self.store.status, other, message["id"])
        self.assertCode("forbidden", self.store.ack, self.a, message["id"])
        self.assertCode("forbidden", self.store.send, outsider, "a", "fake reply", "fake", reply_to=message["id"])
        self.assertCode("invalid_cursor", self.store.inbox, outsider, cursor=self.store.inbox(self.b)["cursor"])
        cursor = self.store.inbox(self.b)["cursor"]
        wire = bytearray(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        wire[-1] ^= 1
        forged = base64.urlsafe_b64encode(wire).decode().rstrip("=")
        self.assertCode("invalid_cursor", self.store.inbox, self.b, cursor=forged)
        spoofed = dict(self.a, participant="b", generation=self.b["generation"], space="other")
        self.assertCode("forbidden", self.store.ack, spoofed, message["id"])
        self.assertEqual("a", self.store.send(spoofed, "b", "actual identity", "real")["from"])
        self.assertCode("unauthorized", self.store.authenticate, "not-a-token")
        reply = self.store.send(self.b, "a", "answer", "reply", reply_to=message["id"])
        self.assertEqual([reply["id"]], [m["id"] for m in self.store.status(self.a, message["id"])["replies"]])

    def test_concurrent_duplicate_acceptance_and_claim_have_one_owner(self):
        connections = [Store(self.path), Store(self.path)]
        ready = threading.Barrier(3)

        def send(connection):
            context = connection.authenticate(self.a_join["token"])
            ready.wait(timeout=5)
            return connection.send(context, "b", "A question", "same-acceptance")

        def claim(connection):
            context = connection.authenticate(self.b_join["token"])
            ready.wait(timeout=5)
            return connection.claim(context)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(send, connection) for connection in connections]
                ready.wait(timeout=5)
                accepted = [future.result(timeout=5) for future in futures]
                self.assertEqual(1, len({message["id"] for message in accepted}))
                self.assertEqual(1, len(self.store.inbox(self.b)["messages"]))
                futures = [executor.submit(claim, connection) for connection in connections]
                ready.wait(timeout=5)
                claims = [future.result(timeout=5) for future in futures]
            self.assertEqual(1, sum(claim["message"] is not None for claim in claims))
        finally:
            for connection in connections:
                connection.close()

    def test_operator_connection_does_not_imply_broker_restart(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        operator = Store(self.path)
        try:
            operator.join("project", "third")
        finally:
            operator.close()
        self.assertEqual("claimed", self.store.status(self.a, message["id"])["state"])
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "submitted")

    def test_failed_credential_save_rolls_back_replacement_and_claim_retirement(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        issued = []

        def failed_save(binding):
            issued.append(binding)
            raise OSError("Credential output could not be durably saved")

        with self.assertRaises(OSError):
            self.store.join("project", "b", replace=True, on_issued=failed_save)
        self.assertEqual(self.b["generation"], self.store.identity(self.b)["generation"])
        self.assertEqual("claimed", self.store.status(self.a, message["id"])["state"])
        self.assertCode("unauthorized", self.store.authenticate, issued[0]["token"])
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "submitted")

    def test_database_alias_has_canonical_runtime_identity(self):
        alias = Path(self.directory.name) / "state-alias.sqlite"
        try:
            alias.symlink_to(self.path)
        except (OSError, NotImplementedError):
            self.skipTest("Symbolic links are unavailable on this platform")
        through_alias = Store(alias)
        try:
            self.assertEqual(self.store.path, through_alias.path)
        finally:
            through_alias.close()

    @unittest.skipUnless(os.name == "posix", "POSIX file modes; Windows requires operator-owned ACLs")
    def test_private_database_and_journal_under_normal_umask(self):
        parent = Path(self.directory.name) / "private-state"
        path = parent / "broker.sqlite"
        previous_umask = os.umask(0o022)
        private = None
        try:
            private = Store(path)
            private.create_space("a private message namespace")
            self.assertEqual(0o700, stat.S_IMODE(parent.stat().st_mode))
            for file in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
                self.assertTrue(file.exists())
                self.assertEqual(0o600, stat.S_IMODE(file.stat().st_mode))
        finally:
            os.umask(previous_umask)
            if private is not None:
                private.close()

    @unittest.skipUnless(os.name == "posix", "POSIX advisory locks guard SQLite's WAL index")
    def test_reopened_store_keeps_wal_index_in_use_for_other_processes(self):
        # A restarted broker opens existing state. Its permission recheck must not
        # release SQLite's WAL-index lock, or another process's reader concludes it
        # is the first connection and resets the index under the broker's mapping.
        self.store.close()
        self.store = Store(self.path)
        # Byte 128 of -shm is the unix VFS "connection in use" lock (UNIX_SHM_DMS).
        probe = ("import fcntl, os, sys\n"
                 "descriptor = os.open(sys.argv[1], os.O_RDWR)\n"
                 "try:\n"
                 "    fcntl.lockf(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB, 1, 128, 0)\n"
                 "    print('first-connection')\n"
                 "except OSError:\n"
                 "    print('in-use')\n")
        result = subprocess.run([sys.executable, "-c", probe, str(self.path) + "-shm"],
                                capture_output=True, text=True, timeout=30)
        self.assertEqual(result.stdout.strip(), "in-use", result.stderr)
        self.store.create_space("after reopen")

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "Process descriptor listing")
    def test_failed_reopen_releases_retained_wal_index_descriptors(self):
        def open_state_files():
            names = []
            for entry in os.listdir("/proc/self/fd"):
                try:
                    names.append(os.readlink("/proc/self/fd/" + entry))
                except OSError:
                    pass
            return sorted(name for name in names if name.startswith(str(self.path) + "-"))
        before = open_state_files()
        # A later initialization failure (for example a locked database) must not
        # leak the -wal/-shm descriptors retained for SQLite's WAL-index lock.
        with patch.object(Store, "_reconcile", side_effect=RuntimeError("initialization failed")):
            with self.assertRaisesRegex(RuntimeError, "initialization failed"):
                Store(self.path)
        self.assertEqual(open_state_files(), before)

    @unittest.skipUnless(os.name == "posix", "POSIX durable directory entries")
    def test_failed_directory_sync_rejects_fresh_state_before_sqlite_acceptance(self):
        path = Path(self.directory.name) / "new-ancestor" / "new-state" / "broker.sqlite"
        with patch("xordination.store.os.fsync", side_effect=OSError("Directory sync failed")), \
                patch("xordination.store.sqlite3.connect") as connect:
            with self.assertRaises(OSError):
                Store(path)
            connect.assert_not_called()
        self.assertFalse(path.exists())

    @unittest.skipUnless(os.name == "posix", "POSIX file modes; Windows requires operator-owned ACLs")
    def test_existing_state_files_are_upgraded_without_changing_shared_directory(self):
        parent = self.path.parent
        original_parent_mode = stat.S_IMODE(parent.stat().st_mode)
        files = (self.path, Path(str(self.path) + "-wal"), Path(str(self.path) + "-shm"))
        for file in files:
            file.chmod(0o644)
        operator = Store(self.path)
        try:
            for file in files:
                self.assertEqual(0o600, stat.S_IMODE(file.stat().st_mode))
            self.assertEqual(original_parent_mode, stat.S_IMODE(parent.stat().st_mode))
            self.assertEqual(self.a["generation"], operator.identity(self.a)["generation"])
        finally:
            operator.close()

    @unittest.skipUnless(os.name == "posix", "POSIX symlink protection")
    def test_journal_symlink_is_rejected_without_changing_unrelated_file(self):
        unrelated = Path(self.directory.name) / "unrelated.txt"
        unrelated.write_text("unrelated operator data")
        unrelated.chmod(0o644)
        path = Path(self.directory.name) / "unopened.sqlite"
        Path(str(path) + "-wal").symlink_to(unrelated)
        self.assertCode("unsafe_state", Store, path)
        self.assertEqual("unrelated operator data", unrelated.read_text())
        self.assertEqual(0o644, stat.S_IMODE(unrelated.stat().st_mode))

    def test_expired_claim_becomes_uncertain_without_automatic_replay(self):
        message = self.send()
        self.store.claim(self.b)
        with patch("xordination.store.time.time", return_value=time.time() + self.store.CLAIM_SECONDS + 1):
            self.assertEqual("uncertain", self.store.status(self.a, message["id"])["state"])
            self.assertIsNone(self.store.claim(self.b)["message"])

    def test_expiry_limits_and_session_binding_are_explicit(self):
        message = self.send(expires_in=10)
        with patch("xordination.store.time.time", return_value=time.time() + 11):
            self.assertEqual("expired", self.store.status(self.a, message["id"])["state"])
            self.assertIsNone(self.store.claim(self.b)["message"])
        self.assertCode("limit_exceeded", self.store.send, self.a, "b", "x" * (self.store.MAX_BODY_BYTES + 1), "large")
        self.assertCode("limit_exceeded", self.store.inbox, self.b, limit=1000)
        self.assertCode("replacement_required", self.store.bind, self.b, "different-native-id")
        self.store.bind(self.b, "session-b", {"channel_registered": True})
        self.store.bind(self.b, "session-b", {"native_idle_delivery": "supported"})
        identity = self.store.identity(self.b)
        self.assertTrue(identity["capabilities"]["channel_registered"])
        self.assertEqual("supported", identity["capabilities"]["native_idle_delivery"])

    def test_queue_backpressure_keeps_existing_acceptance_replay_available(self):
        self.store.MAX_PENDING = 2
        first = self.send("first")
        self.send("second")
        self.assertCode("queue_full", self.send, "third")
        self.assertEqual(first["id"], self.send("first")["id"])
        self.store.ack(self.b, first["id"])
        self.assertTrue(self.send("third")["accepted"])

    def test_sender_cancel_is_terminal_and_retains_uncertain_attempt_history(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        self.assertCode("in_flight", self.store.cancel, self.a, message["id"])
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "uncertain", {"reason": "write response lost"})
        self.assertCode("forbidden", self.store.cancel, self.b, message["id"])
        cancelled = self.store.cancel(self.a, message["id"], reason="superseded")
        self.assertEqual("cancelled", cancelled["state"])
        self.assertFalse(cancelled["idempotent_replay"])
        self.assertEqual({"participant": "a", "generation": self.a["generation"], "previous_state": "uncertain",
                          "reason": "superseded", "may_have_been_submitted": True},
                         {k: v for k, v in cancelled["cancellation"].items() if k != "cancelled_at"})
        self.assertTrue(self.store.cancel(self.a, message["id"])["idempotent_replay"])
        self.store.close()
        self.store = Store(self.path)
        self.store.recover_claims()
        status = self.store.status(self.a, message["id"])
        self.assertEqual("cancelled", status["state"])
        self.assertEqual("superseded", status["cancellation"]["reason"])
        self.assertEqual([attempt["attempt_id"]], [row["id"] for row in status["attempts"]])
        self.assertEqual("uncertain", status["attempts"][0]["state"])
        self.assertCode("cancelled", self.store.retry, self.a, message["id"])
        self.assertIsNone(self.store.claim(self.b)["message"])
        replacement = self.store.authenticate(self.store.join("project", "b", replace=True)["token"])
        self.assertCode("cancelled", self.store.recover, replacement, message["id"])

    def test_cancel_refuses_received_submitted_and_expired_delivery(self):
        queued = self.send("queued")
        self.assertEqual("queued", self.store.cancel(self.a, queued["id"])["cancellation"]["previous_state"])
        self.assertFalse(self.store.status(self.a, queued["id"])["cancellation"]["may_have_been_submitted"])
        self.assertIsNone(self.store.claim(self.b)["message"])
        received = self.send("received")
        self.store.ack(self.b, received["id"])
        self.assertCode("already_received", self.store.cancel, self.a, received["id"])
        submitted = self.send("submitted")
        attempt = self.store.claim(self.b)
        self.store.finish(self.b, submitted["id"], attempt["attempt_id"], "submitted")
        self.assertCode("already_submitted", self.store.cancel, self.a, submitted["id"])
        expired = self.send("expired", expires_in=10)
        with patch("xordination.store.time.time", return_value=time.time() + 11):
            self.assertCode("expired", self.store.cancel, self.a, expired["id"])
        self.assertCode("invalid_argument", self.store.cancel, self.a, submitted["id"], reason="")
        for message in (received, submitted, expired):
            self.assertIsNone(self.store.status(self.a, message["id"])["cancellation"])

    def test_cancelled_messages_release_recipient_pending_capacity(self):
        self.store.MAX_PENDING = 1
        first = self.send("first")
        self.assertCode("queue_full", self.send, "second")
        self.store.cancel(self.a, first["id"])
        self.assertTrue(self.send("second")["accepted"])

    def test_cancelled_messages_leave_recipient_inbox_and_the_cursor_passes_them(self):
        withdrawn = self.store.send(self.a, "b", "withdrawn secret", "withdrawn")
        kept = self.send("kept")
        self.store.cancel(self.a, withdrawn["id"])
        page = self.store.inbox(self.b)
        self.assertEqual([kept["id"]], [m["id"] for m in page["messages"]])
        self.assertNotIn("withdrawn secret", json.dumps(page))
        self.assertEqual([kept["id"]], [m["id"] for m in self.store.inbox(self.b, unacknowledged=True)["messages"]])
        trailing = self.send("trailing")
        self.store.cancel(self.a, trailing["id"])
        empty = self.store.inbox(self.b, cursor=page["cursor"])
        self.assertEqual(([], False), (empty["messages"], empty["has_more"]))
        self.assertEqual(trailing["sequence"], empty["position"])
        later = self.send("later")
        self.assertEqual([later["id"]], [m["id"] for m in self.store.inbox(self.b, cursor=empty["cursor"])["messages"]])
        # A full page stops at its last visible row; cancelled rows beyond it
        # are skipped on the next page rather than lost or returned.
        first = self.store.inbox(self.b, limit=1)
        self.assertEqual(([kept["id"]], True), ([m["id"] for m in first["messages"]], first["has_more"]))
        self.assertEqual([later["id"]], [m["id"] for m in self.store.inbox(self.b, cursor=first["cursor"], limit=1)["messages"]])

    def test_cancellation_keeps_submission_risk_unless_failure_was_retry_safe(self):
        for retry_safe, expected in ((None, True), (False, True), (True, False)):
            with self.subTest(retry_safe=retry_safe):
                message = self.send(f"failed-{retry_safe}")
                attempt = self.store.claim(self.b)
                self.store.finish(self.b, message["id"], attempt["attempt_id"], "failed", retry_safe=retry_safe)
                cancelled = self.store.cancel(self.a, message["id"])
                self.assertEqual(expected, cancelled["cancellation"]["may_have_been_submitted"])
                self.assertEqual(expected, self.store.status(self.a, message["id"])["cancellation"]["may_have_been_submitted"])

    def test_definite_failure_records_whether_retry_is_safe(self):
        message = self.send()
        unknown = self.store.claim(self.b)
        self.assertCode("invalid_argument", self.store.finish, self.b, message["id"], unknown["attempt_id"], "submitted", retry_safe=True)
        self.assertCode("invalid_argument", self.store.finish, self.b, message["id"], unknown["attempt_id"], "failed", retry_safe="yes")
        self.store.finish(self.b, message["id"], unknown["attempt_id"], "failed")
        self.store.retry(self.a, message["id"])
        safe = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], safe["attempt_id"], "failed", {"reason": "native_rejected"}, retry_safe=True)
        self.store.retry(self.a, message["id"])
        unsafe = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], unsafe["attempt_id"], "failed", retry_safe=False)
        attempts = self.store.status(self.a, message["id"])["attempts"]
        self.assertEqual([None, True, False], [attempt["retry_safe"] for attempt in attempts])

    def test_schema_one_store_gains_retry_safety_without_changing_history(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "failed")
        self.store.close()
        with closing(sqlite3.connect(self.path)) as legacy:
            legacy.execute("ALTER TABLE attempts DROP COLUMN retry_safe")
            legacy.execute("DROP TABLE cancellations")
            legacy.commit()
        self.store = Store(self.path)
        status = self.store.status(self.a, message["id"])
        self.assertEqual(("failed", None), (status["attempts"][0]["state"], status["attempts"][0]["retry_safe"]))
        self.assertIsNone(status["cancellation"])
        self.assertEqual("cancelled", self.store.cancel(self.a, message["id"])["state"])

    def test_receipts_distinguish_client_ack_native_insertion_and_native_ack(self):
        message = self.send()
        attempt = self.store.claim(self.b)
        self.store.finish(self.b, message["id"], attempt["attempt_id"], "uncertain", {"reason": "response lost"})
        self.assertCode("invalid_argument", self.store.observe, self.b, message["id"], attempt["attempt_id"], "client_ack", {"x": 1})
        self.assertCode("invalid_argument", self.store.observe, self.b, message["id"], attempt["attempt_id"], "native_insertion", {})
        self.assertCode("forbidden", self.store.observe, self.a, message["id"], attempt["attempt_id"], "native_insertion", {"x": 1})
        inserted = self.store.observe(self.b, message["id"], attempt["attempt_id"], "native_insertion",
                                      {"native_event": "user_message_item", "turn": "turn-1"})["receipt"]
        self.assertEqual(("native_insertion", attempt["attempt_id"], "session-b"),
                         (inserted["kind"], inserted["attempt_id"], inserted["native_id"]))
        status = self.store.status(self.a, message["id"])
        # A weaker observation never becomes an acknowledgement, and the
        # earlier transport uncertainty is retained beside the new evidence.
        self.assertEqual((True, False), (status["received"], status["acknowledged"]))
        self.assertEqual(["native_insertion"], status["receipt_kinds"])
        self.assertEqual("uncertain", status["attempts"][0]["state"])
        self.assertTrue(status["uncertain"])
        self.assertFalse(status["unresolved_uncertainty"])
        self.assertEqual([message["id"]], [m["id"] for m in self.store.inbox(self.b, unacknowledged=True)["messages"]])
        self.assertCode("already_received", self.store.retry, self.a, message["id"])
        self.assertCode("already_received", self.store.cancel, self.a, message["id"])
        self.store.observe(self.b, message["id"], attempt["attempt_id"], "native_ack", {"native_event": "agent_tool_call"})
        acknowledged = self.store.ack(self.b, message["id"], attempt_id=attempt["attempt_id"])["receipt"]
        self.assertEqual(("client_ack", attempt["attempt_id"]), (acknowledged["kind"], acknowledged["attempt_id"]))
        status = self.store.status(self.a, message["id"])
        self.assertTrue(status["acknowledged"])
        self.assertEqual(["client_ack", "native_ack", "native_insertion"], status["receipt_kinds"])
        self.assertEqual([], self.store.inbox(self.b, unacknowledged=True)["messages"])

    def test_native_receipts_require_the_recipient_attempts_native_identity(self):
        unbound = self.store.authenticate(self.store.join("project", "c")["token"])
        message = self.store.send(self.a, "c", "question", "unbound")
        attempt = self.store.claim(unbound)
        self.assertCode("native_identity_required", self.store.observe, unbound, message["id"], attempt["attempt_id"],
                        "native_insertion", {"native_event": "claimed"})
        self.store.finish(unbound, message["id"], attempt["attempt_id"], "failed", retry_safe=True)
        bound = self.store.authenticate(self.store.join("project", "c", replace=True, native_id="session-c")["token"])
        self.assertTrue(self.store.recover(bound, message["id"])["recovered"])
        # The recovered generation cannot attribute evidence to the old attempt.
        self.assertCode("forbidden", self.store.ack, bound, message["id"], attempt_id=attempt["attempt_id"])
        self.assertCode("forbidden", self.store.observe, bound, message["id"], attempt["attempt_id"],
                        "native_insertion", {"native_event": "late"})
        other = self.send("other")
        foreign = self.store.claim(self.b)
        self.assertCode("forbidden", self.store.ack, self.b, other["id"], attempt_id=attempt["attempt_id"])
        self.store.finish(self.b, other["id"], foreign["attempt_id"], "failed")
        self.assertCode("attempt_failed", self.store.observe, self.b, other["id"], foreign["attempt_id"],
                        "native_insertion", {"native_event": "late"})
        self.assertEqual([], self.store.status(self.a, other["id"])["receipts"])

    def test_operator_space_view_counts_pending_uncertain_and_waiting_states(self):
        queued = self.send("queued")
        uncertain = self.send("uncertain")
        received = self.send("received")
        cancelled = self.send("cancelled")
        self.store.cancel(self.a, cancelled["id"])
        self.store.ack(self.b, received["id"])
        first = self.store.claim(self.b)
        self.assertEqual(queued["id"], first["message"]["id"])
        self.store.finish(self.b, queued["id"], first["attempt_id"], "submitted")
        second = self.store.claim(self.b)
        self.store.finish(self.b, uncertain["id"], second["attempt_id"], "uncertain")
        retained = self.store.send(self.b, "a", "for the old generation", "old")
        self.store.join("project", "a", replace=True, native_id="thread-new")
        view = self.store.space_view("project")
        self.assertFalse(view["native_communication_verified"])
        a, b = view["participants"]
        self.assertEqual(["a", "b"], [a["participant"], b["participant"]])
        self.assertEqual({self.a["generation"]: False, a["generation"]: True},
                         {g["generation"]: g["active"] for g in a["generations"]})
        self.assertEqual({"queued": 1}, a["states"])
        self.assertEqual(1, a["waiting"]["recovery"])
        self.assertEqual({"submitted": 1, "uncertain": 1, "queued": 1, "cancelled": 1}, b["states"])
        self.assertEqual((2, 1), (b["pending"], b["uncertain"]))
        self.assertEqual({"delivery": 0, "in_flight": 0, "receipt": 1, "explicit_retry": 1, "recovery": 0}, b["waiting"])
        self.assertEqual((3, 1), (view["pending"], view["uncertain"]))
        self.assertEqual({"delivery": 0, "in_flight": 0, "receipt": 1, "explicit_retry": 1, "recovery": 1}, view["waiting"])
        self.assertNotIn(retained["body"], json.dumps(view))
        self.assertNotIn("token", json.dumps(view))
        self.assertCode("unknown_space", self.store.space_view, "missing")


if __name__ == "__main__":
    unittest.main()
