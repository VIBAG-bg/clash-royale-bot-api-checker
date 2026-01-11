import unittest

from charts import render_my_activity_decks_chart


def _matplotlib_available() -> bool:
    try:
        import matplotlib  # noqa: F401
        import matplotlib.pyplot as plt  # noqa: F401
    except Exception:
        return False
    return True


class ChartsRenderTests(unittest.TestCase):
    @unittest.skipUnless(_matplotlib_available(), "matplotlib not available")
    def test_render_returns_png_bytes(self) -> None:
        png_bytes = render_my_activity_decks_chart(
            title="Test",
            week_labels=["1/1", "1/2", "1/3"],
            player_decks=[8, 12, 16],
            player_fame=[400, 800, 1200],
            clan_avg_decks=10.0,
            clan_avg_fame=900.0,
            x_label="Week",
            y_left_label="Decks",
            y_right_label="Fame",
            legend_you_decks="You (decks)",
            legend_you_fame="You (fame)",
            legend_clan_avg_decks="Clan avg (decks)",
            legend_clan_avg_fame="Clan avg (fame)",
        )
        self.assertTrue(png_bytes.startswith(b"\x89PNG\r\n\x1a\n"))
        self.assertGreater(len(png_bytes), 1000)

    @unittest.skipUnless(_matplotlib_available(), "matplotlib not available")
    def test_render_validates_lengths(self) -> None:
        with self.assertRaises(ValueError):
            render_my_activity_decks_chart(
                title="Test",
                week_labels=["1/1"],
                player_decks=[8, 12],
                player_fame=[400],
                clan_avg_decks=None,
                clan_avg_fame=None,
                x_label="Week",
                y_left_label="Decks",
                y_right_label="Fame",
                legend_you_decks="You (decks)",
                legend_you_fame="You (fame)",
                legend_clan_avg_decks="Clan avg (decks)",
                legend_clan_avg_fame="Clan avg (fame)",
            )
