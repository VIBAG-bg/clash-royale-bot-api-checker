try:
    import matplotlib

    matplotlib.use("Agg")
except Exception as exc:
    matplotlib = None
    _MATPLOTLIB_IMPORT_ERROR = exc
else:
    _MATPLOTLIB_IMPORT_ERROR = None

from io import BytesIO

if matplotlib is None:
    plt = None
else:
    try:
        import matplotlib.pyplot as plt
    except Exception as exc:
        plt = None
        if _MATPLOTLIB_IMPORT_ERROR is None:
            _MATPLOTLIB_IMPORT_ERROR = exc


def _require_matplotlib() -> None:
    if plt is None:
        if _MATPLOTLIB_IMPORT_ERROR is not None:
            raise _MATPLOTLIB_IMPORT_ERROR
        raise RuntimeError


def render_my_activity_decks_chart(
    *,
    title: str,
    week_labels: list[str],
    player_decks: list[int],
    player_fame: list[int],
    clan_avg_decks: float | None = None,
    clan_avg_fame: float | None = None,
    x_label: str,
    y_left_label: str,
    y_right_label: str,
    legend_you_decks: str,
    legend_you_fame: str,
    legend_clan_avg_decks: str,
    legend_clan_avg_fame: str,
) -> bytes:
    _require_matplotlib()
    if not week_labels or not player_decks or not player_fame:
        raise ValueError
    if len(week_labels) != len(player_decks) or len(week_labels) != len(
        player_fame
    ):
        raise ValueError

    fig, ax = plt.subplots(figsize=(7, 4))
    line_decks = ax.plot(
        week_labels,
        player_decks,
        marker="o",
        label=legend_you_decks,
        color="tab:blue",
    )[0]
    y_top = max(player_decks) if player_decks else 16
    ax.set_ylim(0, y_top + 1)
    ax.set_ylabel(y_left_label)
    ax.set_xlabel(x_label)

    ax2 = ax.twinx()
    line_fame = ax2.plot(
        week_labels,
        player_fame,
        marker="o",
        label=legend_you_fame,
        color="tab:yellow",
    )[0]
    ax2.set_ylabel(y_right_label)

    legend_lines = [line_decks, line_fame]
    legend_labels = [legend_you_decks, legend_you_fame]
    if clan_avg_decks is not None:
        avg_line = ax.axhline(
            clan_avg_decks, label=legend_clan_avg_decks, color="tab:blue"
        )
        legend_lines.append(avg_line)
        legend_labels.append(legend_clan_avg_decks)
    if clan_avg_fame is not None:
        avg_fame_line = ax2.axhline(
            clan_avg_fame, label=legend_clan_avg_fame, color="tab:yellow"
        )
        legend_lines.append(avg_fame_line)
        legend_labels.append(legend_clan_avg_fame)
    ax.set_title(title)
    ax.legend(legend_lines, legend_labels)
    ax.tick_params(axis="x", rotation=20)
    fig.tight_layout()
    fig.subplots_adjust(top=0.90)

    buf = BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return buf.getvalue()
