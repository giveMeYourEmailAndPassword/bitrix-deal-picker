#!/usr/bin/env python3
"""
Comprehensive test suite for Bitrix24 Deal Picker (app.py).

Contracts defined FIRST — tests assert CORRECT behavior, not current bugs.
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

# ── Bootstrap: hardcoded test env, never touches real Bitrix ──────────
# WARNING: these OVERRIDE any existing real env vars.
os.environ["BITRIX_WEBHOOK_BASE"] = "https://test-fake.bitrix24.kz/rest/1/test/"
os.environ["APP_DATA_DIR"] = tempfile.mkdtemp(prefix="bitrix_test_fake_")
os.environ["DRY_RUN"] = "1"
os.environ["ADMIN_USER_IDS"] = "1,2"
os.environ["HOST"] = "127.0.0.1"
os.environ["PORT"] = "0"
os.environ["PUBLIC_APP_URL"] = "http://localhost:3000"
os.environ["LIMIT_FREE_WINDOW_START"] = "18:00"
os.environ["LIMIT_FREE_WINDOW_END"] = "21:30"
os.environ["APP_TZ_OFFSET_HOURS"] = "6"
os.environ["GREETING_AUTO_SEND"] = "0"

sys.path.insert(0, str(Path(__file__).resolve().parent))
import app

# ── Global network guard (module-scoped) ─────────────────────────
# Blocks ALL HTTP(S) calls at the socket level only for this module.
# Other test modules are not affected.
import urllib.request as _ur

def _blocking_urlopen(url, data=None, timeout=None, /, **kwargs):
    raise RuntimeError(
        f"NETWORK BLOCKED: {url!r} — mock app.bitrix_call or patch urllib.request.urlopen "
        f"for this test before it reaches production Bitrix."
    )

_urlopen_blocker = patch.object(_ur, 'urlopen', _blocking_urlopen)

def setUpModule():
    _urlopen_blocker.start()

def tearDownModule():
    _urlopen_blocker.stop()


# ===========================================================================
# 1. UTILITIES
# ===========================================================================

class TestParseHhmm(unittest.TestCase):
    def test_valid_time(self):
        self.assertEqual(app.parse_hhmm("18:00", "00:00"), 18 * 60)
        self.assertEqual(app.parse_hhmm("00:00", "00:00"), 0)
        self.assertEqual(app.parse_hhmm("23:59", "00:00"), 23 * 60 + 59)

    def test_invalid_format_falls_back(self):
        """Empty, None, non-matching regex → returns fallback."""
        self.assertEqual(app.parse_hhmm("", "18:00"), 18 * 60)
        self.assertEqual(app.parse_hhmm("abc", "00:00"), 0)
        self.assertEqual(app.parse_hhmm(None, "18:00"), 18 * 60)

    def test_out_of_range_clamps(self):
        """Hours >23 clamp to 23, minutes >59 clamp to 59 — not fallback."""
        self.assertEqual(app.parse_hhmm("25:00", "00:00"), 23 * 60)       # 25→23
        self.assertEqual(app.parse_hhmm("12:61", "00:00"), 12 * 60 + 59)  # 61→59
        self.assertEqual(app.parse_hhmm("99:99", "00:00"), 23 * 60 + 59)  # both clamped


class TestNormalizeDate(unittest.TestCase):
    def test_valid_date_passthrough(self):
        self.assertEqual(app.normalize_date("2026-07-29", "2026-01-01"), "2026-07-29")

    def test_invalid_uses_fallback(self):
        self.assertEqual(app.normalize_date("", "2026-01-01"), "2026-01-01")
        self.assertEqual(app.normalize_date(None, "fallback"), "fallback")
        self.assertEqual(app.normalize_date("2026/07/29", "fallback"), "fallback")
        self.assertEqual(app.normalize_date("29.07.2026", "fallback"), "fallback")


class TestEntryDate(unittest.TestCase):
    """Contract: returns 'YYYY-MM-DD' for parseable timestamps; '' for missing/unparseable."""

    def test_parses_iso(self):
        self.assertEqual(app.entry_date({"timestamp": "2026-07-29T14:30:00+06:00"}), "2026-07-29")

    def test_converts_tz(self):
        """18:00 UTC → 00:00+1 in +06 → shifts date."""
        self.assertEqual(app.entry_date({"timestamp": "2026-07-29T18:00:00+00:00"}), "2026-07-30")

    def test_naive_datetime_assumes_utc(self):
        self.assertEqual(app.entry_date({"timestamp": "2026-07-29T23:00:00"}), "2026-07-30")

    def test_missing_timestamp_returns_empty_string(self):
        """BUG: missing timestamp must NOT match any date range."""
        result = app.entry_date({})
        self.assertEqual(result, "")

    def test_bad_format_returns_empty_string(self):
        """Unparseable timestamps must NOT produce a valid date string."""
        self.assertEqual(app.entry_date({"timestamp": "bad-date"}), "")

    def test_none_timestamp_returns_empty(self):
        self.assertEqual(app.entry_date({"timestamp": None}), "")


class TestCleanText(unittest.TestCase):
    def test_removes_html_tags(self):
        self.assertEqual(app.clean_text("Hello <b>world</b>"), "Hello world")

    def test_removes_bbcodes(self):
        self.assertEqual(app.clean_text("Hello [B]world[/B]"), "Hello world")

    def test_unescapes_html_entities(self):
        self.assertEqual(app.clean_text("Hello &amp; world"), "Hello & world")
        self.assertEqual(app.clean_text("&lt;tag&gt;"), "<tag>")

    def test_removes_urls(self):
        self.assertEqual(app.clean_text("Check https://example.com/page"), "Check")

    def test_removes_emoji_fcodes(self):
        self.assertEqual(app.clean_text("Hello :f09f988a: world"), "Hello world")

    def test_collapses_whitespace(self):
        self.assertEqual(app.clean_text("Hello    world\n\n  test"), "Hello world test")

    def test_empty_input(self):
        self.assertEqual(app.clean_text(""), "")
        self.assertEqual(app.clean_text(None), "")


class TestIsServiceText(unittest.TestCase):
    def test_known_service_patterns(self):
        self.assertTrue(app.is_service_text("Чат открытой линии"))
        self.assertTrue(app.is_service_text("Подключение #12345"))
        self.assertTrue(app.is_service_text("Создана новая сделка"))
        self.assertTrue(app.is_service_text("с вами на связи менеджер"))
        self.assertTrue(app.is_service_text("подбор туров"))
        self.assertTrue(app.is_service_text("обращение направлено"))
        self.assertTrue(app.is_service_text("обращение перенаправлено"))
        self.assertTrue(app.is_service_text("все отели"))

    def test_non_service_text(self):
        self.assertFalse(app.is_service_text("Хочу поехать в Турцию"))
        self.assertFalse(app.is_service_text("Сколько стоит тур в Египет?"))
        self.assertFalse(app.is_service_text(""))

    def test_case_insensitive(self):
        self.assertTrue(app.is_service_text("ЧАТ ОТКРЫТОЙ ЛИНИИ"))
        self.assertTrue(app.is_service_text("Подключение #5"))

    def test_none_returns_false(self):
        """BUG: must not crash on None input."""
        self.assertFalse(app.is_service_text(None))


class TestUsefulFragments(unittest.TestCase):
    def test_filters_short_fragments(self):
        self.assertEqual(app.useful_fragments("short"), [])

    def test_filters_fragments_without_letters(self):
        self.assertEqual(app.useful_fragments("12345 67890"), [])

    def test_filters_service_text(self):
        self.assertEqual(app.useful_fragments("Чат открытой линии с клиентом"), [])

    def test_keeps_valid_fragment(self):
        result = app.useful_fragments("Хочу поехать в Турцию всей семьей")
        self.assertIn("Хочу поехать в Турцию всей семьей", result)

    def test_empty_input(self):
        self.assertEqual(app.useful_fragments(""), [])
        self.assertEqual(app.useful_fragments(None), [])


class TestSplitMessageFragments(unittest.TestCase):
    def test_splits_with_timestamp(self):
        text = "Хочу в Турцию +77011234567 14:30 Подключение #5 15:00 Второе сообщение"
        result = app.split_message_fragments(text)
        self.assertGreaterEqual(len(result), 1)

    def test_empty_input(self):
        self.assertEqual(app.split_message_fragments(""), [])
        self.assertEqual(app.split_message_fragments(None), [])

    def test_no_split_points_returns_whole(self):
        result = app.split_message_fragments("Простое сообщение без разделителей")
        self.assertTrue(any("Простое сообщение без разделителей" in f for f in result))


class TestNormalizeRejectReason(unittest.TestCase):
    def test_known_reasons_pass_through(self):
        for reason in app.REJECT_REASONS:
            self.assertEqual(app.normalize_reject_reason(reason), reason)

    def test_unknown_defaults_to_other(self):
        self.assertEqual(app.normalize_reject_reason("made_up_reason"), "other")
        self.assertEqual(app.normalize_reject_reason(""), "other")
        self.assertEqual(app.normalize_reject_reason(None), "other")

    def test_strips_whitespace(self):
        self.assertEqual(app.normalize_reject_reason("  not_my_country  "), "not_my_country")


class TestLooksLikeInstallPayload(unittest.TestCase):
    def test_install_y_marker(self):
        self.assertTrue(app.looks_like_install_payload(b"install=Y"))
        self.assertTrue(app.looks_like_install_payload(b"foo=bar&install=y"))

    def test_onappinstall_event(self):
        self.assertTrue(app.looks_like_install_payload(b"event=ONAPPINSTALL"))

    def test_application_token(self):
        self.assertTrue(app.looks_like_install_payload(b"application_token=abc"))

    def test_app_sid(self):
        self.assertTrue(app.looks_like_install_payload(b"app_sid=xyz"))

    def test_normal_payload_returns_false(self):
        self.assertFalse(app.looks_like_install_payload(b'{"dealId":"123"}'))
        self.assertFalse(app.looks_like_install_payload(b""))

    def test_case_insensitive(self):
        self.assertTrue(app.looks_like_install_payload(b"INSTALL=Y"))
        self.assertTrue(app.looks_like_install_payload(b"APPLICATION_TOKEN=x"))


# ===========================================================================
# 2. CLASSIFICATION
# ===========================================================================

class TestClassify(unittest.TestCase):
    def test_classify_turkey_high(self):
        """2+ keyword matches → высокая."""
        result = app.classify(["Хочу в Турцию всей семьей", "Посоветуйте Анталью"])
        self.assertEqual(result["direction"], "Турция")
        self.assertEqual(result["confidence"], "высокая")

    def test_classify_turkey_single_match(self):
        """1 keyword match → средняя. 'Кемер' appears only in Турция list."""
        result = app.classify(["Интересует Кемер"])
        self.assertEqual(result["direction"], "Турция")
        self.assertEqual(result["confidence"], "средняя")

    def test_classify_egypt_substring_match(self):
        """'шарм' matches 'Шарм-эль-Шейх' — 1 match → средняя."""
        result = app.classify(["Отдых в Египте на курорте Шарм-эль-Шейх"])
        self.assertEqual(result["direction"], "Египет")
        self.assertEqual(result["confidence"], "средняя")

    def test_classify_undefined(self):
        result = app.classify(["Просто хочу отдохнуть"])
        self.assertEqual(result["direction"], "Не определено")
        self.assertEqual(result["confidence"], "низкая")

    def test_classify_multiple_destinations_picks_highest(self):
        result = app.classify(["Турция Анталья Египет"])
        self.assertEqual(result["direction"], "Турция")

    def test_classify_visa_match(self):
        result = app.classify(["Нужна виза в Европу"])
        self.assertEqual(result["direction"], "Визы")

    def test_classify_empty_messages(self):
        result = app.classify([])
        self.assertEqual(result["confidence"], "низкая")

    def test_classify_maldives(self):
        result = app.classify(["Мальдивы, атолл"])
        self.assertEqual(result["direction"], "Мальдивы")

    def test_classify_thailand(self):
        result = app.classify(["Таиланд Пхукет"])
        self.assertEqual(result["direction"], "Таиланд")

    def test_classify_keyword_matches_across_messages(self):
        """Keywords from different messages accumulate."""
        result = app.classify(["Хочу в Турцию", "Рассматриваю Анталью"])
        self.assertEqual(result["direction"], "Турция")
        self.assertEqual(result["confidence"], "высокая")

    def test_classify_matched_field_ordered_by_score(self):
        result = app.classify(["Турция Египет ОАЭ"])
        self.assertIn("Турция", result["matched"])
        self.assertIn("Египет", result["matched"])


# ===========================================================================
# 3. BUSINESS LOGIC
# ===========================================================================

class TestIsLimitFreeTime(unittest.TestCase):
    def test_within_window(self):
        now = datetime(2026, 7, 29, 19, 0, 0, tzinfo=app.LOCAL_TZ)
        self.assertTrue(app.is_limit_free_time(now))

    def test_before_window(self):
        now = datetime(2026, 7, 29, 17, 0, 0, tzinfo=app.LOCAL_TZ)
        self.assertFalse(app.is_limit_free_time(now))

    def test_after_window(self):
        now = datetime(2026, 7, 29, 22, 0, 0, tzinfo=app.LOCAL_TZ)
        self.assertFalse(app.is_limit_free_time(now))

    def test_exactly_at_start_inclusive(self):
        now = datetime(2026, 7, 29, 18, 0, 0, tzinfo=app.LOCAL_TZ)
        self.assertTrue(app.is_limit_free_time(now))

    def test_exactly_at_end_exclusive(self):
        """21:30 is the boundary — NOT in window."""
        now = datetime(2026, 7, 29, 21, 30, 0, tzinfo=app.LOCAL_TZ)
        self.assertFalse(app.is_limit_free_time(now))

    def test_wraparound_window_night(self):
        """start=22:00 → end=06:00: 23:00 should be inside."""
        with patch.object(app, 'LIMIT_FREE_WINDOW_START', '22:00'), \
             patch.object(app, 'LIMIT_FREE_WINDOW_END', '06:00'):
            self.assertTrue(app.is_limit_free_time(
                datetime(2026, 7, 29, 23, 0, 0, tzinfo=app.LOCAL_TZ)))

    def test_wraparound_morning(self):
        """start=22:00 → end=06:00: 05:00 should be inside."""
        with patch.object(app, 'LIMIT_FREE_WINDOW_START', '22:00'), \
             patch.object(app, 'LIMIT_FREE_WINDOW_END', '06:00'):
            self.assertTrue(app.is_limit_free_time(
                datetime(2026, 7, 30, 5, 0, 0, tzinfo=app.LOCAL_TZ)))

    def test_wraparound_outside(self):
        """start=22:00 → end=06:00: 10:00 should be outside."""
        with patch.object(app, 'LIMIT_FREE_WINDOW_START', '22:00'), \
             patch.object(app, 'LIMIT_FREE_WINDOW_END', '06:00'):
            self.assertFalse(app.is_limit_free_time(
                datetime(2026, 7, 29, 10, 0, 0, tzinfo=app.LOCAL_TZ)))


class TestIsLimitBypassedNow(unittest.TestCase):
    def test_sunday_bypass(self):
        now = datetime(2026, 7, 26, 10, 0, 0, tzinfo=app.LOCAL_TZ)  # Sunday
        self.assertTrue(app.is_limit_bypassed_now(now))

    def test_weekday_outside_window(self):
        now = datetime(2026, 7, 29, 10, 0, 0, tzinfo=app.LOCAL_TZ)  # Wednesday
        self.assertFalse(app.is_limit_bypassed_now(now))

    def test_weekday_inside_window(self):
        now = datetime(2026, 7, 29, 19, 0, 0, tzinfo=app.LOCAL_TZ)
        self.assertTrue(app.is_limit_bypassed_now(now))

    def test_saturday_not_bypassed(self):
        now = datetime(2026, 7, 25, 10, 0, 0, tzinfo=app.LOCAL_TZ)  # Saturday
        self.assertFalse(app.is_limit_bypassed_now(now))


class TestDealScoreForManager(unittest.TestCase):
    def setUp(self):
        self.manager = {"id": "123", "name": "Test", "competencies": ["Турция", "Египет"]}

    def test_competency_match(self):
        deal = {"classification": {"direction": "Турция"}, "messages": ["Турция Анталья"]}
        self.assertGreater(app.deal_score_for_manager(deal, self.manager), 0)

    def test_no_match_score_zero(self):
        deal = {"classification": {"direction": "Индонезия"}, "messages": ["Бали"]}
        self.assertEqual(app.deal_score_for_manager(deal, self.manager), 0)

    def test_undefined_direction_gets_base_score(self):
        deal = {"classification": {"direction": "Не определено"}, "messages": []}
        self.assertEqual(app.deal_score_for_manager(deal, self.manager), 1)

    def test_manager_without_competencies_scores_zero_for_known_direction(self):
        manager = {"id": "123", "name": "Test", "competencies": []}
        deal = {"classification": {"direction": "Турция"}, "messages": ["Турция"]}
        self.assertEqual(app.deal_score_for_manager(deal, manager), 0)

    def test_empty_competencies_field(self):
        manager = {"id": "123", "name": "Test"}
        deal = {"classification": {"direction": "Турция"}, "messages": ["Турция"]}
        self.assertEqual(app.deal_score_for_manager(deal, manager), 0)

    def test_score_structure(self):
        """direction match=4, text match=2, direction-bonus=1 → total≥7."""
        manager = {"id": "123", "name": "Test", "competencies": ["Турция"]}
        deal = {"classification": {"direction": "Турция"}, "messages": ["Турция Анталья"]}
        self.assertGreaterEqual(app.deal_score_for_manager(deal, manager), 7)


class TestCheckManagerAccess(unittest.TestCase):
    @patch("app.get_manager_rule")
    def test_disabled_manager_denied(self, mock_rule):
        mock_rule.return_value = {"enabled": False, "dailyLimit": None, "note": ""}
        self.assertFalse(app.check_manager_access("123")["ok"])

    @patch("app.get_manager_rule")
    @patch("app.is_limit_bypassed_now")
    def test_limit_bypassed_ok(self, mock_bypass, mock_rule):
        mock_rule.return_value = {"enabled": True, "dailyLimit": 5, "note": ""}
        mock_bypass.return_value = True
        result = app.check_manager_access("123")
        self.assertTrue(result["ok"])
        self.assertTrue(result.get("limitBypassed"))

    @patch("app.get_manager_rule")
    @patch("app.count_claims")
    @patch("app.is_limit_bypassed_now")
    @patch("app.local_date")
    def test_limit_reached(self, mock_date, mock_bypass, mock_count, mock_rule):
        mock_rule.return_value = {"enabled": True, "dailyLimit": 3, "note": ""}
        mock_bypass.return_value = False
        mock_date.return_value = "2026-07-29"
        mock_count.return_value = 3
        self.assertFalse(app.check_manager_access("123")["ok"])

    @patch("app.get_manager_rule")
    @patch("app.count_claims")
    @patch("app.is_limit_bypassed_now")
    @patch("app.local_date")
    def test_under_limit_ok(self, mock_date, mock_bypass, mock_count, mock_rule):
        mock_rule.return_value = {"enabled": True, "dailyLimit": 5, "note": ""}
        mock_bypass.return_value = False
        mock_date.return_value = "2026-07-29"
        mock_count.return_value = 2
        self.assertTrue(app.check_manager_access("123")["ok"])

    @patch("app.get_manager_rule")
    def test_no_daily_limit_ok(self, mock_rule):
        mock_rule.return_value = {"enabled": True, "dailyLimit": None, "note": ""}
        self.assertTrue(app.check_manager_access("123")["ok"])


class TestCountClaimsInLog(unittest.TestCase):
    """Contract: entries with unparseable timestamps must NOT match any range."""

    @staticmethod
    def entry(ts, mid="123"):
        return {"timestamp": ts, "managerId": mid}

    def test_counts_in_range(self):
        log = [self.entry("2026-07-29T10:00:00+06:00")]
        self.assertEqual(app.count_claims_in_log(log, "123", "2026-07-29", "2026-07-29"), 1)

    def test_empty_log(self):
        self.assertEqual(app.count_claims_in_log([], "123"), 0)

    def test_different_manager_filtered(self):
        log = [self.entry("2026-07-29T10:00:00+06:00", "456"),
               self.entry("2026-07-29T10:00:00+06:00", "123")]
        self.assertEqual(app.count_claims_in_log(log, "123", "2026-07-29", "2026-07-29"), 1)

    def test_missing_timestamp_not_counted(self):
        """BUG: entry without timestamp must NOT be counted in any range."""
        log = [{"managerId": "123", "timestamp": None}]
        self.assertEqual(app.count_claims_in_log(log, "123", "2026-07-29", "2026-07-29"), 0)

    def test_tz_conversion_crosses_date(self):
        """18:00 UTC → next day in +06."""
        log = [self.entry("2026-07-29T18:00:00+00:00")]
        self.assertEqual(app.count_claims_in_log(log, "123", "2026-07-29", "2026-07-29"), 0)
        self.assertEqual(app.count_claims_in_log(log, "123", "2026-07-30", "2026-07-30"), 1)

    def test_none_timestamp_not_counted(self):
        log = [{"managerId": "123"}]
        self.assertEqual(app.count_claims_in_log(log, "123", "2026-07-29", "2026-07-29"), 0)


class TestRejectionReasonSummary(unittest.TestCase):
    def make_entry(self, reason, mid="123", ts="2026-07-29T10:00:00+06:00"):
        return {"managerId": mid, "reason": reason, "timestamp": ts}

    def test_most_common_reason(self):
        log = [self.make_entry("not_my_country"), self.make_entry("not_my_country"),
               self.make_entry("unclear_request")]
        r = app.rejection_reason_summary(log, "123", "2026-07-29", "2026-07-29")
        self.assertIn("Не моя страна", r)
        self.assertIn("(2)", r)

    def test_empty_log(self):
        self.assertEqual(app.rejection_reason_summary([], "123"), "")

    def test_tie_alphabetical(self):
        log = [self.make_entry("duplicate"), self.make_entry("unclear_request")]
        r = app.rejection_reason_summary(log, "123", "2026-07-29", "2026-07-29")
        self.assertIn("Дубль", r)

class TestGetNextDealForManager(unittest.TestCase):
    @patch("app.get_manager_profile")
    def test_manager_not_found(self, mock_profile):
        mock_profile.return_value = None
        result = app.get_next_deal_for_manager("999")
        self.assertIsNone(result["deal"])
        self.assertIn("не найден", result.get("reason", ""))

    @patch("app.list_allowed_deal_headers")
    @patch("app.check_manager_access")
    @patch("app.get_manager_profile")
    def test_manager_without_competencies(self, mock_profile, mock_access, mock_headers):
        mock_profile.return_value = {"id": "123", "name": "Test", "competencies": []}
        mock_access.return_value = {"ok": True, "rule": {}}
        mock_headers.return_value = []
        result = app.get_next_deal_for_manager("123")
        self.assertIsNone(result["deal"])




# ===========================================================================
# 4. FILE I/O
# ===========================================================================

class TestReadJsonFile(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bitrix_test_"))

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_missing_returns_default(self):
        self.assertEqual(app.read_json_file(self.tmpdir / "nope.json", {"d": 1}), {"d": 1})

    def test_valid_json(self):
        p = self.tmpdir / "data.json"
        p.write_text('{"a": 1}')
        self.assertEqual(app.read_json_file(p, {}), {"a": 1})

    def test_corrupt_json_returns_default(self):
        p = self.tmpdir / "bad.json"
        p.write_text("not json")
        self.assertEqual(app.read_json_file(p, {"fallback": True}), {"fallback": True})

    def test_empty_file_returns_default(self):
        p = self.tmpdir / "empty.json"
        p.write_text("")
        self.assertEqual(app.read_json_file(p, []), [])


class TestWriteJsonFile(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="bitrix_test_"))

    def tearDown(self):
        import shutil; shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_writes_and_reads_back(self):
        p = self.tmpdir / "test.json"
        app.write_json_file(p, {"key": "value", "n": 42})
        self.assertEqual(json.loads(p.read_text()), {"key": "value", "n": 42})

    def test_atomic_no_tmp_leak(self):
        p = self.tmpdir / "atom.json"
        app.write_json_file(p, {"v": 1})
        self.assertFalse(p.with_suffix(".tmp").exists())

    def test_trim_on_overflow(self):
        p = self.tmpdir / "log.json"
        app.write_json_file(p, list(range(6000))[-5000:])
        self.assertEqual(len(json.loads(p.read_text())), 5000)


# ===========================================================================
# 5. AUTH
# ===========================================================================

class TestExtractInitialAuthFromValues(unittest.TestCase):
    def test_extracts_all_fields(self):
        parsed = {"AUTH_ID": ["a"], "REFRESH_ID": ["r"], "DOMAIN": ["d.bitrix24.kz"],
                  "member_id": ["m1"], "client_endpoint": ["https://d.bitrix24.kz/rest/"]}
        r = app.extract_initial_auth_from_values(parsed)
        self.assertEqual(r.get("AUTH_ID"), "a")
        self.assertEqual(r.get("DOMAIN"), "d.bitrix24.kz")

    def test_skips_empty_values(self):
        r = app.extract_initial_auth_from_values({"AUTH_ID": [""], "DOMAIN": ["d.bitrix24.kz"]})
        self.assertNotIn("AUTH_ID", r)

    def test_alternative_key_names(self):
        r = app.extract_initial_auth_from_values({"access_token": ["t"], "domain": ["d.bitrix24.kz"]})
        self.assertEqual(r.get("AUTH_ID"), "t")

    def test_none_input(self):
        self.assertEqual(app.extract_initial_auth_from_values(None), {})

    def test_member_id_variants(self):
        r = app.extract_initial_auth_from_values({"MEMBER_ID": ["m123"]})
        self.assertEqual(r.get("member_id"), "m123")


class TestExtractInitialAuth(unittest.TestCase):
    def test_parses_urlencoded(self):
        r = app.extract_initial_auth(b"AUTH_ID=abc&DOMAIN=test.bitrix24.kz")
        self.assertEqual(r.get("AUTH_ID"), "abc")

    def test_empty_input(self):
        self.assertEqual(app.extract_initial_auth(b""), {})
        self.assertEqual(app.extract_initial_auth(None), {})

    def test_malformed_input(self):
        self.assertEqual(app.extract_initial_auth(b"\xff\xfe\x00"), {})


class TestHasBitrixAuth(unittest.TestCase):
    def test_valid(self):
        self.assertTrue(app.has_bitrix_auth(
            {"auth": {"access_token": "tok", "domain": "d.bitrix24.kz"}}))

    def test_missing_auth_key(self):
        self.assertFalse(app.has_bitrix_auth({}))

    def test_none_payload(self):
        self.assertFalse(app.has_bitrix_auth(None))

    def test_no_token(self):
        self.assertFalse(app.has_bitrix_auth({"auth": {"domain": "d"}}))

    def test_no_domain(self):
        self.assertFalse(app.has_bitrix_auth({"auth": {"access_token": "tok"}}))

    def test_client_endpoint_as_domain(self):
        self.assertTrue(app.has_bitrix_auth(
            {"auth": {"access_token": "tok", "client_endpoint": "https://d.bitrix24.kz/rest/"}}))


class TestActorIdFromPayload(unittest.TestCase):
    def setUp(self):
        app._USER_VERIFY_CACHE.clear()

    @patch("app.verify_bitrix_user")
    def test_returns_verified_user_id(self, mock_verify):
        mock_verify.return_value = {"id": "42", "name": "Test"}
        r = app.actor_id_from_payload({"auth": {"access_token": "tok", "domain": "d.bitrix24.kz"}})
        self.assertEqual(r, "42")

    @patch("app.verify_bitrix_user")
    def test_verify_revoked_token_returns_none(self, mock_verify):
        mock_verify.return_value = None
        r = app.actor_id_from_payload(
            {"auth": {"access_token": "expired", "domain": "d.bitrix24.kz"}, "currentUserId": "99"})
        self.assertIsNone(r)

    @patch("app.verify_bitrix_user")
    def test_no_auth_uses_manager_id_when_unverified_allowed(self, mock_verify):
        mock_verify.return_value = None
        with patch.object(app, 'ALLOW_UNVERIFIED_USERS', True):
            r = app.actor_id_from_payload({"auth": {}, "managerId": "55"})
            self.assertEqual(r, "55")

    @patch("app.verify_bitrix_user")
    def test_no_auth_no_fallback_returns_none(self, mock_verify):
        mock_verify.return_value = None
        r = app.actor_id_from_payload({"auth": {}})
        self.assertIsNone(r)

    @patch("app.verify_bitrix_user")
    def test_client_endpoint_extracts_domain(self, mock_verify):
        """client_endpoint без domain — должен корректно извлекаться."""
        mock_verify.return_value = {"id": "77", "name": "Client EP User"}
        r = app.actor_id_from_payload({
            "auth": {"access_token": "tok", "client_endpoint": "https://my.bitrix24.kz/rest/"}})
        self.assertEqual(r, "77")


class TestRequireAdmin(unittest.TestCase):
    @patch("app.verify_bitrix_user")
    def test_admin_verified(self, mock_verify):
        mock_verify.return_value = {"id": "1", "name": "Admin"}
        self.assertIsNotNone(app.require_admin({"auth": {"access_token": "tok", "domain": "d"}}))

    @patch("app.verify_bitrix_user")
    def test_non_admin_denied(self, mock_verify):
        mock_verify.return_value = {"id": "99", "name": "User"}
        self.assertIsNone(app.require_admin({"auth": {"access_token": "tok", "domain": "d"}}))

    def test_admin_by_current_user_id_no_auth(self):
        """currentUserId без auth → не админ (impersonation защита)."""
        self.assertIsNone(app.require_admin({"currentUserId": "1"}))

    def test_non_admin_by_current_user_id(self):
        self.assertIsNone(app.require_admin({"currentUserId": "99"}))


# ===========================================================================
# 6. API HANDLERS
# ===========================================================================

class MockHandler:
    """Build a minimally functional HTTP handler for unit tests."""

    @staticmethod
    def make(method, path, body=None, query=None):
        full = path
        if query:
            qs = "&".join(f"{k}={v}" for k, v in query.items())
            full = f"{path}?{qs}"

        inst = app.Handler.__new__(app.Handler)
        inst.command = method
        inst.path = full
        inst.client_address = ("127.0.0.1", 0)
        inst.server = MagicMock()
        inst.close_connection = True
        inst.headers = {"Content-Length": str(len(json.dumps(body or {}).encode("utf-8")))}
        inst.rfile = BytesIO(json.dumps(body or {}).encode("utf-8"))
        inst.send_response = MagicMock()
        inst.send_header = MagicMock()
        inst.end_headers = MagicMock()
        inst.wfile = BytesIO()
        return inst


class TestHandlerRouting(unittest.TestCase):
    def _dispatch(self, h):
        if h.command == "GET":
            h.do_GET()
        else:
            h.do_POST()

    def _json(self, h):
        return json.loads(h.wfile.getvalue().decode("utf-8"))

    def _status(self, h):
        return h.send_response.call_args[0][0]

    # ── GET routes ──

    def test_health(self):
        h = MockHandler.make("GET", "/api/health")
        self._dispatch(h)
        self.assertTrue(self._json(h)["ok"])

    @patch.object(app, 'bitrix_call')
    def test_health_no_bitrix_calls(self, mock_bx):
        h = MockHandler.make("GET", "/api/health")
        self._dispatch(h)
        mock_bx.assert_not_called()

    def test_health_structure(self):
        h = MockHandler.make("GET", "/api/health")
        self._dispatch(h)
        d = self._json(h)
        self.assertIn("version", d)
        self.assertIn("sourceStages", d)
        self.assertIn("rejectReasons", d)
        self.assertIn("dryRun", d)

    def test_404_unknown_api(self):
        h = MockHandler.make("GET", "/api/unknown")
        self._dispatch(h)
        self.assertEqual(self._status(h), 404)

    def test_root_returns_html(self):
        h = MockHandler.make("GET", "/")
        self._dispatch(h)
        body = h.wfile.getvalue().decode("utf-8")
        self.assertIn("Получить сделку", body)
        self.assertIn("text/html", str(h.send_header.call_args_list))

    def test_non_api_returns_html(self):
        h = MockHandler.make("GET", "/some-page")
        self._dispatch(h)
        self.assertEqual(self._status(h), 200)

    def test_404_on_post_non_api(self):
        h = MockHandler.make("POST", "/not-api")
        self._dispatch(h)
        self.assertEqual(self._status(h), 404)

    def test_options_returns_204(self):
        h = MockHandler.make("OPTIONS", "/api/health")
        h.do_OPTIONS()
        self.assertEqual(self._status(h), 204)

    def test_cors_headers_on_json(self):
        h = MockHandler.make("GET", "/api/health")
        self._dispatch(h)
        sent = [c[0] for c in h.send_header.call_args_list]
        self.assertIn(("Access-Control-Allow-Origin", "*"), sent)
        self.assertIn(("Access-Control-Allow-Headers", "Content-Type"), sent)

    def test_install_path(self):
        h = MockHandler.make("GET", "/install", query={"AUTH_ID": "tok", "DOMAIN": "d.bitrix24.kz"})
        self._dispatch(h)
        body = h.wfile.getvalue().decode("utf-8")
        self.assertIn("INSTALL_MODE = true", body)
        self.assertIn("Получить сделку", body)

    # ── POST /api/next-deal ──

    @patch("app.get_next_deal_for_manager")
    @patch("app.actor_id_from_payload")
    def test_next_deal_post(self, mock_actor, mock_next):
        mock_actor.return_value = "42"
        mock_next.return_value = {"deal": None}
        h = MockHandler.make("POST", "/api/next-deal", {"skipped": ["1"]})
        self._dispatch(h)
        mock_next.assert_called_once_with("42", ["1"])

    @patch("app.actor_id_from_payload")
    def test_next_deal_unauthorized(self, mock_actor):
        mock_actor.return_value = None
        h = MockHandler.make("POST", "/api/next-deal")
        self._dispatch(h)
        self.assertEqual(self._status(h), 401)

    @patch("app.get_next_deal_for_manager")
    @patch("app.actor_id_from_payload")
    def test_next_deal_503_on_error(self, mock_actor, mock_next):
        mock_actor.return_value = "42"
        mock_next.side_effect = RuntimeError("Bitrix timeout")
        h = MockHandler.make("POST", "/api/next-deal")
        self._dispatch(h)
        self.assertEqual(self._status(h), 503)

    # ── POST /api/claim ──

    @patch("app.preview_claim")
    @patch("app.actor_id_from_payload")
    def test_claim_passes_all_args(self, mock_actor, mock_preview):
        mock_actor.return_value = "42"
        mock_preview.return_value = {"ok": True}
        h = MockHandler.make("POST", "/api/claim",
                             {"dealId": "100", "deal": {"id": "100"}, "auth": {"access_token": "tok"}})
        self._dispatch(h)
        mock_preview.assert_called_once_with("100", "42", {"id": "100"}, {"access_token": "tok"})

    @patch("app.actor_id_from_payload")
    def test_claim_unauthorized(self, mock_actor):
        mock_actor.return_value = None
        h = MockHandler.make("POST", "/api/claim")
        self._dispatch(h)
        self.assertEqual(self._status(h), 401)

    # ── POST /api/reject ──

    @patch("app.record_rejection")
    @patch("app.actor_id_from_payload")
    def test_reject_200(self, mock_actor, mock_reject):
        mock_actor.return_value = "42"
        mock_reject.return_value = {"ok": True}
        h = MockHandler.make("POST", "/api/reject", {"dealId": "100"})
        self._dispatch(h)
        self.assertEqual(self._status(h), 200)

    @patch("app.record_rejection")
    @patch("app.actor_id_from_payload")
    def test_reject_400(self, mock_actor, mock_reject):
        mock_actor.return_value = "42"
        mock_reject.return_value = {"ok": False, "message": "bad"}
        h = MockHandler.make("POST", "/api/reject", {"dealId": ""})
        self._dispatch(h)
        self.assertEqual(self._status(h), 400)

    # ── GET /api/next-deal (managerId query) ──
    def test_next_deal_get_returns_404(self):
        h = MockHandler.make("GET", "/api/next-deal")
        self._dispatch(h)
        self.assertEqual(self._status(h), 404)

    def test_next_deal_get_with_skip_returns_404(self):
        h = MockHandler.make("GET", "/api/next-deal", query={"skip[]": "1"})
        self._dispatch(h)
        self.assertEqual(self._status(h), 404)

    # ── POST /install / root ──

    def test_post_root_returns_html(self):
        h = MockHandler.make("POST", "/")
        self._dispatch(h)
        body = h.wfile.getvalue().decode("utf-8")
        self.assertIn("Получить сделку", body)

    def test_post_install_returns_html(self):
        h = MockHandler.make("POST", "/install", {"AUTH_ID": "tok", "DOMAIN": "d"})
        self._dispatch(h)
        body = h.wfile.getvalue().decode("utf-8")
        self.assertIn("Получить сделку", body)

    # ── API error handler ──

    def test_generic_api_error_500(self):
        with patch.object(app, 'load_managers', side_effect=RuntimeError("crash")):
            h = MockHandler.make("GET", "/api/managers")
            self._dispatch(h)
            self.assertEqual(self._status(h), 500)


class TestManagerProfile(unittest.TestCase):
    def test_parse_competencies_list(self):
        r = app.parse_competencies(["Турция, Египет", "ОАЭ"])
        self.assertIn("Турция", r)
        self.assertIn("Египет", r)
        self.assertIn("ОАЭ", r)

    def test_parse_competencies_string(self):
        r = app.parse_competencies("Турция, Египет; ОАЭ")
        self.assertIn("Турция", r)
        self.assertIn("Египет", r)
        self.assertIn("ОАЭ", r)

    def test_parse_competencies_empty(self):
        self.assertEqual(app.parse_competencies(""), [])
        self.assertEqual(app.parse_competencies(None), [])

    def test_parse_competencies_newline_separated(self):
        r = app.parse_competencies("Турция\nЕгипет")
        self.assertIn("Турция", r)

    @patch("app.bitrix_call")
    @patch("app.load_managers")
    def test_get_manager_profile_fallback_on_api_fail(self, mock_load, mock_bx):
        mock_load.return_value = [{"id": "42", "name": "Local", "competencies": ["Турция"]}]
        mock_bx.side_effect = RuntimeError("API down")
        r = app.get_manager_profile("42")
        self.assertEqual(r["name"], "Local")
        self.assertIn("Турция", r["competencies"])

    @patch("app.bitrix_call")
    @patch("app.load_managers")
    def test_uf_skills_takes_precedence(self, mock_load, mock_bx):
        mock_load.return_value = [{"id": "42", "name": "Local", "competencies": ["Old"]}]
        mock_bx.return_value = [{"ID": "42", "NAME": "Bitrix", "LAST_NAME": "User",
                                  "UF_SKILLS": "Турция, Египет"}]
        r = app.get_manager_profile("42")
        self.assertIn("Турция", r["competencies"])
        self.assertNotIn("Old", r["competencies"])

    def test_empty_id_returns_none(self):
        self.assertIsNone(app.get_manager_profile(""))
        self.assertIsNone(app.get_manager_profile(None))


class TestBuildGreetingText(unittest.TestCase):
    def test_with_direction(self):
        t = app.build_greeting_text({"name": "Иван Петров"}, {"direction": "Турция"})
        self.assertIn("Иван", t)
        self.assertIn("Турция", t)

    def test_without_direction(self):
        t = app.build_greeting_text({"name": "Иван"},
                                     {"direction": "Не определено", "confidence": "низкая"})
        self.assertIn("Иван", t)
        self.assertNotIn("эксперт по направлению", t)

    def test_minimal(self):
        t = app.build_greeting_text({"name": "Анна"}, {"direction": "Египет"})
        self.assertIn("Анна", t)
        self.assertIn("Египет", t)


class TestManagerAccessEdgeCases(unittest.TestCase):
    @patch("app.get_manager_rule")
    @patch("app.count_claims")
    @patch("app.is_limit_bypassed_now")
    @patch("app.local_date")
    def test_limit_taken_equals_limit_denied(self, md, bp, mc, mr):
        mr.return_value = {"enabled": True, "dailyLimit": 5, "note": ""}
        bp.return_value = False
        md.return_value = "2026-07-29"
        mc.return_value = 5
        self.assertFalse(app.check_manager_access("123")["ok"])

    @patch("app.get_manager_rule")
    @patch("app.count_claims")
    @patch("app.is_limit_bypassed_now")
    @patch("app.local_date")
    def test_limit_taken_under_limit_ok(self, md, bp, mc, mr):
        mr.return_value = {"enabled": True, "dailyLimit": 5, "note": ""}
        bp.return_value = False
        md.return_value = "2026-07-29"
        mc.return_value = 4
        self.assertTrue(app.check_manager_access("123")["ok"])


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
