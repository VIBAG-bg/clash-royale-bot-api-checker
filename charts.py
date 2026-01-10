try:
    import matplotlib

    matplotlib.use("Agg")
except Exception as exc:
    matplotlib = None
    _MATPLOTLIB_IMPORT_ERROR = exc
else:
    _MATPLOTLIB_IMPORT_ERROR = None

from io import BytesIO

try:
    if matplotlib is None:
        raise RuntimeError("matplotlib import failed")
    import matplotlib.pyplot as plt
except Exception as exc:
    plt = None
    if _MATPLOTLIB_IMPORT_ERROR is None:
        _MATPLOTLIB_IMPORT_ERROR = exc


def _require_matplotlib() -> None:
    if plt is None:
        raise RuntimeError(
            "matplotlib is required to render charts. Install with `pip install matplotlib`."
        ) from _MATPLOTLIB_IMPORT_ERROR


def render_my_activity_decks_chart(
    *,
    title: str,
    week_labels: list[str],
    player_decks: list[int],
    clan_avg_decks: float | None = None,
) -> bytes:
    _require_matplotlib()
    if not week_labels or not player_decks:
        raise ValueError("week_labels and player_decks must be non-empty.")
    if len(week_labels) != len(player_decks):
        raise ValueError("week_labels and player_decks must have the same length.")

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.plot(week_labels, player_decks, marker="o")
    if clan_avg_decks is not None:
        ax.axhline(clan_avg_decks, label="Clan avg")
        ax.legend()
    ax.set_title(title)
    ax.tick_params(axis="x", rotation=20)
    fig.tight_layout()

    buf = BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()
