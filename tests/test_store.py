"""Observable durability, authority, and replay contracts from the specification."""

import concurrent.futures
import os
import stat
import tempfile
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
        new_join = self.store.join("project", "b", replace=True)
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
        spoofed = dict(self.a, participant="b", generation=self.b["generation"], space="other")
        self.assertCode("forbidden", self.store.ack, spoofed, message["id"])
        self.assertEqual("a", self.store.send(spoofed, "b", "actual identity", "real")["from"])
        self.assertCode("unauthorized", self.store.authenticate, "not-a-token")
        reply = self.store.send(self.b, "a", "answer", "reply", reply_to=message["id"])
        self.assertEqual([reply["id"]], [m["id"] for m in self.store.status(self.a, message["id"])["replies"]])

    def test_concurrent_duplicate_acceptance_and_claim_have_one_owner(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            accepted = list(executor.map(lambda _: self.send(), range(24)))
        self.assertEqual(1, len({m["id"] for m in accepted}))
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
            claims = list(executor.map(lambda _: self.store.claim(self.b), range(24)))
        self.assertEqual(1, sum(claim["message"] is not None for claim in claims))

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


if __name__ == "__main__":
    unittest.main()
