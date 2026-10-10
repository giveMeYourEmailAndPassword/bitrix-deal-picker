"""Ownership guard regressions; injected Bitrix calls never use the network."""

import unittest
from unittest.mock import Mock, patch

from claim_chat_guard import ClaimChatGuardUnavailable, read_claim_chat_ownership


DEAL = "572219"
MANAGER = "27337"
CHAT = "71883"
OTHER_MANAGER = "33"
BOT = "262867"


def dialog(owner=OTHER_MANAGER, chat=CHAT):
    return {
        "id": int(chat), "type": "lines", "entity_type": "LINES",
        "entity_data_2": f"LEAD|0|DEAL|{DEAL}|CONTACT|123|COMPANY|0",
        "owner": int(owner),
    }


def user(owner=OTHER_MANAGER, bot=False, active=True):
    return {
        "id": int(owner), "bot": bot, "connector": False,
        "extranet": False, "active": active,
        "departments": [] if bot else [187],
    }


class TestClaimChatGuard(unittest.TestCase):
    def guard(self, *responses, **kwargs):
        call = Mock(side_effect=list(responses))
        result = read_claim_chat_ownership(DEAL, MANAGER, call, **kwargs)
        return result, call

    def assert_unavailable(self, *responses):
        call = Mock(side_effect=list(responses))
        with self.assertRaises(ClaimChatGuardUnavailable):
            read_claim_chat_ownership(DEAL, MANAGER, call)
        return call

    def test_no_accepted_dialog_leaves_queue_eligible(self):
        for chats in ([], {}):
            with self.subTest(chats=chats):
                occupied, call = self.guard(chats)
                self.assertFalse(occupied)
                self.assertEqual(call.call_count, 1)
                args, kwargs = call.call_args
                self.assertEqual(args, (
                    "imopenlines.crm.chat.get",
                    {"CRM_ENTITY_TYPE": "DEAL", "CRM_ENTITY": DEAL, "ACTIVE_ONLY": "Y"},
                ))
                self.assertGreater(kwargs["timeout"], 0)
                self.assertLessEqual(kwargs["timeout"], 8)

    def test_foreign_employee_blocks_manual_take_even_if_deal_stage_waiting(self):
        occupied, call = self.guard([{"CHAT_ID": CHAT}], dialog(), user())
        self.assertTrue(occupied)
        self.assertEqual([entry.args[0] for entry in call.call_args_list], [
            "imopenlines.crm.chat.get", "imopenlines.dialog.get", "im.user.get",
        ])
        self.assertEqual(call.call_args_list[1].args[1], {"CHAT_ID": CHAT})
        self.assertEqual(call.call_args_list[2].args[1], {"ID": OTHER_MANAGER})

    def test_inactive_employee_does_not_release_dialog(self):
        occupied, _ = self.guard([{"CHAT_ID": CHAT}], dialog(), user(active=False))
        self.assertTrue(occupied)

    def test_requesting_manager_can_continue_their_own_dialog(self):
        occupied, call = self.guard([{"CHAT_ID": CHAT}], dialog(MANAGER))
        self.assertFalse(occupied)
        self.assertEqual(call.call_count, 2)

    def test_verified_bot_does_not_occupy_deal_as_employee(self):
        occupied, _ = self.guard([{"CHAT_ID": CHAT}], dialog(BOT), user(BOT, bot=True))
        self.assertFalse(occupied)

    def test_dictionary_maps_and_duplicate_ids_are_supported(self):
        for chats in (
            {CHAT: {"CHAT_ID": CHAT}},
            {"0": {"CHAT_ID": CHAT}, "1": {"CHAT_ID": int(CHAT)}},
            [{"CHAT_ID": CHAT}, {"CHAT_ID": int(CHAT)}],
        ):
            with self.subTest(chats=chats):
                occupied, call = self.guard(chats, dialog(MANAGER))
                self.assertFalse(occupied)
                self.assertEqual(call.call_count, 2)

    def test_bot_or_own_chat_does_not_mask_foreign_employee_in_another_chat(self):
        second_chat = "71884"
        for owner in (MANAGER, BOT):
            with self.subTest(owner=owner):
                responses = [
                    [{"CHAT_ID": CHAT}, {"CHAT_ID": second_chat}], dialog(owner),
                ]
                if owner == BOT:
                    responses.append(user(BOT, bot=True))
                responses.extend([dialog(chat=second_chat), user()])
                occupied, _ = self.guard(*responses)
                self.assertTrue(occupied)

    def test_malformed_list_is_unavailable_instead_of_empty_queue(self):
        for chats in (None, False, 0, "", "[]", {"result": []}, [None], [CHAT], [{}]):
            with self.subTest(chats=chats):
                self.assert_unavailable(chats)

    def test_active_chat_limit_is_fail_closed_without_partial_scan(self):
        chats = [{"CHAT_ID": str(index + 1)} for index in range(11)]
        call = self.assert_unavailable(chats)
        self.assertEqual(call.call_count, 1)

    def test_invalid_chat_ids_and_input_ids_are_unavailable(self):
        invalid = (None, True, False, 0, "0", -1, 1.5, "x", "1.0", "9" * 5000)
        for identity in invalid:
            with self.subTest(identity=repr(identity)[:30]):
                self.assert_unavailable([{"CHAT_ID": identity}])
                call = Mock()
                for deal, manager in ((identity, MANAGER), (DEAL, identity)):
                    with self.assertRaises(ClaimChatGuardUnavailable):
                        read_claim_chat_ownership(deal, manager, call)
                call.assert_not_called()

    def test_malformed_or_incomplete_dialog_is_unavailable(self):
        malformed = [None, [], {}, {**dialog(), "id": "999"}]
        for field in ("id", "type", "entity_type", "entity_data_2", "owner"):
            missing = dialog()
            missing.pop(field)
            malformed.append(missing)
        for field, value in (
            ("type", "chat"), ("entity_type", "CRM"), ("owner", 0),
            ("owner", "0"), ("owner", True), ("owner", -1),
        ):
            malformed.append({**dialog(), field: value})
        for item in malformed:
            with self.subTest(dialog=item):
                self.assert_unavailable([{"CHAT_ID": CHAT}], item)

    def test_binding_must_be_exact_typed_and_unambiguous(self):
        for binding in (
            None, {}, "", f"CONTACT|{DEAL}", f"DEAL|{DEAL}0",
            f"DEAL|99|CONTACT|{DEAL}", f"DEAL|{DEAL}|DEAL|99",
            f"DEAL|{DEAL}|DEAL|{DEAL}", f"DEAL|{DEAL}|CONTACT",
            f"DEAL|{DEAL}|CONTACT|invalid", f"DEAL|{DEAL}|CONTACT|123|",
        ):
            with self.subTest(binding=binding):
                self.assert_unavailable(
                    [{"CHAT_ID": CHAT}], {**dialog(), "entity_data_2": binding},
                )

    def test_missing_or_unknown_owner_identity_fails_closed(self):
        malformed = [None, [], {}, {**user(), "id": 99}]
        for field in ("id", "bot", "connector", "extranet", "departments"):
            missing = user()
            missing.pop(field)
            malformed.append(missing)
        for field, value in (
            ("bot", "false"), ("bot", "true"), ("bot", 0),
            ("connector", True), ("connector", "N"), ("extranet", True),
            ("departments", []), ("departments", None), ("departments", {}),
            ("departments", [0]), ("departments", [True]),
        ):
            malformed.append({**user(), field: value})
        for item in malformed:
            with self.subTest(user=item):
                self.assert_unavailable([{"CHAT_ID": CHAT}], dialog(), item)

    def test_bot_exemption_requires_complete_consistent_identity(self):
        for field in ("bot", "connector", "extranet", "departments"):
            incomplete = user(BOT, bot=True)
            incomplete.pop(field)
            with self.subTest(field=field):
                self.assert_unavailable([{"CHAT_ID": CHAT}], dialog(BOT), incomplete)

    def test_read_errors_at_each_step_fail_closed_with_safe_message(self):
        for prefix in ([], [[{"CHAT_ID": CHAT}]], [[{"CHAT_ID": CHAT}], dialog()]):
            for error in (TimeoutError("private remote URL"), RuntimeError("private API text")):
                with self.subTest(prefix=len(prefix), error=type(error)):
                    call = Mock(side_effect=[*prefix, error])
                    with self.assertRaisesRegex(ClaimChatGuardUnavailable, "^chat_guard_read_failed$"):
                        read_claim_chat_ownership(DEAL, MANAGER, call)

    def test_calls_share_one_deadline_and_never_receive_more_than_eight_seconds(self):
        now = [100.0]
        responses = iter(([{"CHAT_ID": CHAT}], dialog(), user()))
        elapsed = iter((1.0, 2.0, 3.0))
        timeouts = []

        def call(method, params, *, timeout):
            timeouts.append(timeout)
            now[0] += next(elapsed)
            return next(responses)

        with patch("claim_chat_guard.time.monotonic", side_effect=lambda: now[0]):
            self.assertTrue(read_claim_chat_ownership(DEAL, MANAGER, call, timeout=30))
        self.assertEqual(timeouts, [8.0, 7.0, 5.0])

    def test_slow_response_cannot_authorize_claim_after_deadline(self):
        for responses in ([[]], [[{"CHAT_ID": CHAT}], dialog(MANAGER)]):
            now = [100.0]
            queue = list(responses)

            def call(method, params, *, timeout):
                now[0] += 8.0 if len(queue) == 1 else 1.0
                return queue.pop(0)

            with patch("claim_chat_guard.time.monotonic", side_effect=lambda: now[0]):
                with self.assertRaisesRegex(ClaimChatGuardUnavailable, "chat_guard_timeout"):
                    read_claim_chat_ownership(DEAL, MANAGER, call)

    def test_expired_budget_prevents_next_network_call(self):
        call = Mock()
        with patch("claim_chat_guard.time.monotonic", side_effect=[100.0, 108.0]):
            with self.assertRaisesRegex(ClaimChatGuardUnavailable, "chat_guard_timeout"):
                read_claim_chat_ownership(DEAL, MANAGER, call)
        call.assert_not_called()

    def test_invalid_timeouts_are_unavailable(self):
        for timeout in (None, False, 0, -1, "invalid", float("nan"), float("inf")):
            with self.subTest(timeout=timeout):
                call = Mock()
                with self.assertRaises(ClaimChatGuardUnavailable):
                    read_claim_chat_ownership(DEAL, MANAGER, call, timeout=timeout)
                call.assert_not_called()

    def test_ownership_is_read_again_without_queue_or_user_cache(self):
        call = Mock(side_effect=[
            [], [{"CHAT_ID": CHAT}], dialog(), user(),
            [{"CHAT_ID": CHAT}], dialog(BOT), user(BOT, bot=True),
        ])
        self.assertFalse(read_claim_chat_ownership(DEAL, MANAGER, call))
        self.assertTrue(read_claim_chat_ownership(DEAL, MANAGER, call))
        self.assertFalse(read_claim_chat_ownership(DEAL, MANAGER, call))
        self.assertEqual(call.call_count, 7)


if __name__ == "__main__":
    unittest.main()
