"""Signed retry-generation bounds and durable, manager-scoped rejection history."""

import base64
import json
import os
import unittest
from unittest.mock import patch

from test_app import TemporaryStateTestCase, _TEST_ENV, _network_is_forbidden, app
from state_store import StateStore


_ENV_PATCH = patch.dict(os.environ, _TEST_ENV)
_NETWORK_PATCH = patch.object(app.urllib.request, "urlopen", _network_is_forbidden)


def setUpModule():
    _ENV_PATCH.start()
    _NETWORK_PATCH.start()


def tearDownModule():
    _NETWORK_PATCH.stop()
    _ENV_PATCH.stop()


class TestSignedRejectionAfter(unittest.TestCase):
    def token(self, rejection_after=0):
        return app.issue_selection_token(
            "100", "42", "version-1", "a" * 64,
            now=1_000, rejection_after=rejection_after,
        )

    def test_accepts_zero_and_full_sqlite_integer_range(self):
        for after in (0, 1, 2**63 - 1):
            with self.subTest(after=after):
                payload = app.decode_selection_token(
                    self.token(after), "100", "42", now=1_000,
                )
                self.assertIsNotNone(payload)
                self.assertIs(type(payload["rejectionAfter"]), int)
                self.assertEqual(payload["rejectionAfter"], after)

    def test_issuer_rejects_non_integer_negative_and_overflow_generations(self):
        for after in (-1, "0", "1", False, True, None, 0.0, 1.5, 2**63):
            with self.subTest(after=repr(after)):
                with self.assertRaises(ValueError):
                    self.token(after)

    def test_valid_signature_cannot_bypass_generation_validation(self):
        valid_payload = app._decode_signed_token(self.token())
        for after in (-1, "0", "1", False, True, None, 0.0, 1.5, 2**63):
            with self.subTest(after=repr(after)):
                signed = app._issue_signed_token({
                    **valid_payload, "rejectionAfter": after,
                })
                self.assertIsNone(
                    app.decode_selection_token(signed, "100", "42", now=1_000),
                )

    def test_rejection_generation_is_covered_by_signature(self):
        token = self.token(7)
        encoded, signature = token.split(".")
        payload = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        payload["rejectionAfter"] = 8
        changed = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
        self.assertIsNone(
            app.decode_selection_token(f"{changed}.{signature}", "100", "42", now=1_000),
        )


class TestDurableRejectionRetryHistory(TemporaryStateTestCase):
    def append(self, semantic_key, *, root=None, after=0, manager_id="42", day=15):
        event = {
            "timestamp": f"2026-09-{day:02d}T12:00:00+06:00",
            "managerId": manager_id,
            "dealId": "100",
            "semanticKey": semantic_key,
            "reason": "not_my_country",
            "reasonLabel": "Не моя страна",
        }
        if root is not None:
            event.update({"rejectionRoot": root, "rejectionAfter": after})
        self.store.append_reject(event)
        with self.store._connect() as connection:
            row = connection.execute(
                "SELECT id FROM reject_events WHERE semantic_key = ?", (semantic_key,),
            ).fetchone()
        return int(row["id"])

    def test_pre_retry_semantic_key_is_retained_as_legacy_root(self):
        root = app.rejection_semantic_key("42", "100", "version-1")
        event_id = self.append(root)
        self.assertEqual(self.store.list_rejection_history("42"), {root: event_id})
        self.assertEqual(self.store.list_rejection_history(42), {root: event_id})
        saved = self.store.list_rejections(manager_id="42")
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["semanticKey"], root)
        self.assertNotIn("rejectionRoot", saved[0])

    def test_multiple_rounds_keep_all_audit_events_and_order_by_id(self):
        root = app.rejection_semantic_key("42", "100", "version-1")
        first_id = self.append(root, day=15)
        second_key = app.rejection_semantic_key("42", "100", "version-1", first_id)
        second_id = self.append(second_key, root=root, after=first_id, day=14)
        third_key = app.rejection_semantic_key("42", "100", "version-1", second_id)
        third_id = self.append(third_key, root=root, after=second_id, day=13)

        self.assertLess(first_id, second_id)
        self.assertLess(second_id, third_id)
        self.assertEqual(self.store.list_rejection_history("42"), {root: third_id})
        events = self.store.list_rejections(manager_id="42")
        self.assertEqual([event["semanticKey"] for event in events], [root, second_key, third_key])
        self.assertEqual([event.get("rejectionAfter", 0) for event in events], [0, first_id, second_id])
        self.assertTrue(all(event["reason"] == "not_my_country" for event in events))
        self.assertEqual(self.store.get_rejection_by_semantic_key(root), events[0])

    def test_other_manager_history_cannot_replace_current_manager_state(self):
        root = app.rejection_semantic_key("42", "100", "version-1")
        first_id = self.append(root)
        other_key = app.rejection_semantic_key("43", "100", "version-1")
        # Even a payload with an identical root remains scoped by the DB manager.
        other_id = self.append(other_key, root=root, manager_id="43")
        self.assertEqual(self.store.list_rejection_history("42"), {root: first_id})
        self.assertEqual(self.store.list_rejection_history("43"), {root: other_id})
        self.assertEqual(self.store.list_rejection_history("44"), {})

    def test_versions_have_independent_history_and_survive_restart(self):
        old_root = app.rejection_semantic_key("42", "100", "version-1")
        old_id = self.append(old_root)
        new_root = app.rejection_semantic_key("42", "100", "version-2")
        new_id = self.append(new_root)
        retry_key = app.rejection_semantic_key("42", "100", "version-1", old_id)
        retry_id = self.append(retry_key, root=old_root, after=old_id)
        audit_before_restart = self.store.list_rejections(manager_id="42")

        restarted = StateStore(self.data_dir, db_filename="state.sqlite3", local_timezone=app.LOCAL_TZ)
        self.assertEqual(restarted.list_rejection_history("42"), {old_root: retry_id, new_root: new_id})
        self.assertEqual(restarted.list_rejections(manager_id="42"), audit_before_restart)
        self.assertEqual(restarted.count_rejections(manager_id="42"), 3)

    def test_unversioned_legacy_audit_is_preserved_without_inventing_history(self):
        self.store.append_reject({
            "timestamp": "2026-09-15T12:00:00+06:00",
            "managerId": "42", "dealId": "100", "reason": "other",
        })
        self.assertEqual(self.store.list_rejection_history("42"), {})
        self.assertEqual(self.store.count_rejections(manager_id="42"), 1)


if __name__ == "__main__":
    unittest.main()
