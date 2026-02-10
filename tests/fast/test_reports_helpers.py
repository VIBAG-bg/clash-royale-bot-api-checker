import unittest
from datetime import datetime, timedelta, timezone

try:
    import sqlalchemy  # noqa: F401
except Exception:
    raise unittest.SkipTest("sqlalchemy not available")

from config import LAST_SEEN_RED_DAYS, LAST_SEEN_YELLOW_DAYS
from i18n import t
from reports import (
    _absence_label,
    _compare_simple,
    _compare_to_avg,
    _days_absent,
    _format_avg,
    _format_median,
    _format_relative,
    _normalize_tag,
    _parse_last_seen_string,
)


class ReportHelperTests(unittest.TestCase):
    def test_format_avg(self) -> None:
        self.assertEqual(_format_avg(1.234), "1.2")

    def test_format_median_empty(self) -> None:
        self.assertEqual(_format_median([], lang="en"), t("na", "en"))

    def test_format_median_values(self) -> None:
        self.assertEqual(_format_median([1, 2, 3], lang="en"), "2.0")

    def test_compare_to_avg(self) -> None:
        self.assertEqual(_compare_to_avg(10, 0, "en"), t("compare_near", "en"))
        self.assertEqual(_compare_to_avg(110, 100, "en"), t("compare_above", "en"))
        self.assertEqual(_compare_to_avg(90, 100, "en"), t("compare_below", "en"))

    def test_compare_simple(self) -> None:
        self.assertEqual(_compare_simple(5, 3, "en"), t("compare_above", "en"))
        self.assertEqual(_compare_simple(3, 5, "en"), t("compare_below", "en"))
        self.assertEqual(_compare_simple(4, 4, "en"), t("compare_equal", "en"))

    def test_parse_last_seen_string(self) -> None:
        parsed = _parse_last_seen_string("20240101T120000.000Z")
        self.assertIsInstance(parsed, datetime)
        self.assertEqual(parsed.tzinfo, timezone.utc)
        self.assertEqual(parsed.year, 2024)

    def test_format_relative(self) -> None:
        delta = timedelta(days=1, hours=2)
        self.assertEqual(
            _format_relative(delta, "en"),
            t("relative_days_hours", "en", days=1, hours=2),
        )

    def test_days_absent_and_absence_label(self) -> None:
        now = datetime(2024, 1, 10, tzinfo=timezone.utc)
        last_seen = datetime(2024, 1, 1, tzinfo=timezone.utc)
        self.assertEqual(_days_absent(last_seen, now), 9)
        self.assertEqual(
            _absence_label(LAST_SEEN_RED_DAYS, "en"), t("absence_red", "en")
        )
        self.assertEqual(
            _absence_label(LAST_SEEN_YELLOW_DAYS, "en"),
            t("absence_yellow", "en"),
        )
        self.assertEqual(_absence_label(None, "en"), t("absence_no_data", "en"))

    def test_normalize_tag(self) -> None:
        self.assertEqual(_normalize_tag("abc123"), "#ABC123")
        self.assertEqual(_normalize_tag("#abc123"), "#ABC123")
