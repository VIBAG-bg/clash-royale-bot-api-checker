import unittest

import i18n


class I18nChartKeyTests(unittest.TestCase):
    def test_chart_keys_exist(self) -> None:
        required_keys = {
            "chart.war_activity.title",
            "chart.war_activity.title_named",
            "chart.axis.decks",
            "chart.axis.fame",
            "chart.axis.week",
            "chart.legend.you.decks",
            "chart.legend.you.fame",
            "chart.legend.clan_avg.decks",
            "chart.legend.clan_avg.fame",
        }
        for lang in ("ru", "en", "uk"):
            lang_dict = i18n.TEXT.get(lang, {})
            missing = sorted(key for key in required_keys if key not in lang_dict)
            self.assertFalse(missing, f"Missing keys for {lang}: {missing}")

    def test_title_named_formats(self) -> None:
        for lang in ("ru", "en", "uk"):
            value = i18n.t("chart.war_activity.title_named", lang, name="Test")
            self.assertIn("Test", value)
            self.assertNotIn("[MISSING:", value)
