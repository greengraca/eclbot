"""Matplotlib chart renderers for /graphs command.

Each function takes simple lists/values and returns a BytesIO PNG (150 DPI).
Uses the Agg backend (headless, Heroku-safe).
"""

from __future__ import annotations

import io
from typing import List

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# Discord dark theme colors
BG = "#2C2F33"
FG = "#FFFFFF"
GRID = "#40444B"
WIN = "#2ECC71"
LOSS = "#E74C3C"
DRAW = "#95A5A6"
ACCENT = "#3498DB"
RANK_COLOR = "#E67E22"


def _apply_dark_style(ax, fig):
    """Apply Discord-themed dark styling to a figure and axes."""
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.tick_params(colors=FG, which="both")
    ax.xaxis.label.set_color(FG)
    ax.yaxis.label.set_color(FG)
    ax.title.set_color(FG)
    for spine in ax.spines.values():
        spine.set_color(GRID)
    ax.grid(True, color=GRID, alpha=0.5, linestyle="--", linewidth=0.5)


def _save(fig) -> io.BytesIO:
    """Save figure to BytesIO PNG and close."""
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    buf.seek(0)
    plt.close(fig)
    return buf


# ---------------------------------------------------------------------------
# Day-by-day charts (current month)
# ---------------------------------------------------------------------------

def render_daily_points_rank(
    days: List[int],
    points: List[float],
    ranks: List[int],
    player_name: str,
    month_label: str,
) -> io.BytesIO:
    """Dual-axis line chart: points & rank by day within a month."""
    fig, ax1 = plt.subplots(figsize=(10, 5))
    _apply_dark_style(ax1, fig)

    x = range(len(days))
    day_labels = [f"Day {d}" for d in days]

    # Points line (left Y)
    line1 = ax1.plot(x, points, color=ACCENT, marker="o", linewidth=2, markersize=6, label="Points")
    ax1.set_ylabel("Points", color=ACCENT, fontsize=11)
    ax1.tick_params(axis="y", labelcolor=ACCENT)

    # Rank line (right Y, inverted)
    ax2 = ax1.twinx()
    ax2.set_facecolor("none")
    line2 = ax2.plot(x, ranks, color=RANK_COLOR, marker="s", linewidth=2, markersize=6, linestyle="--", label="Rank")
    ax2.set_ylabel("Rank", color=RANK_COLOR, fontsize=11)
    ax2.tick_params(axis="y", labelcolor=RANK_COLOR)
    ax2.invert_yaxis()
    ax2.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax1.set_xticks(list(x))
    ax1.set_xticklabels(day_labels, rotation=45, ha="right", fontsize=9, color=FG)

    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left", facecolor=BG, edgecolor=GRID, labelcolor=FG)

    ax1.set_title(f"{player_name} \u2014 Points & Rank ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_daily_winrate(
    days: List[int],
    win_pcts: List[float],
    player_name: str,
    month_label: str,
) -> io.BytesIO:
    """Line chart: cumulative win rate by day within a month."""
    fig, ax = plt.subplots(figsize=(10, 5))
    _apply_dark_style(ax, fig)

    x = range(len(days))
    pcts = [p * 100 for p in win_pcts]
    day_labels = [f"Day {d}" for d in days]

    ax.plot(x, pcts, color=ACCENT, marker="o", linewidth=2, markersize=6)
    ax.fill_between(x, pcts, alpha=0.15, color=ACCENT)
    ax.axhline(y=50, color=DRAW, linestyle="--", linewidth=1, alpha=0.7, label="50%")

    ax.set_ylim(0, 100)
    ax.set_ylabel("Win Rate %", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(day_labels, rotation=45, ha="right", fontsize=9, color=FG)

    ax.legend(loc="upper right", facecolor=BG, edgecolor=GRID, labelcolor=FG)
    ax.set_title(f"{player_name} \u2014 Win Rate ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_daily_activity(
    days: List[int],
    wins: List[int],
    losses: List[int],
    draws: List[int],
    player_name: str,
    month_label: str,
) -> io.BytesIO:
    """Stacked bar chart: wins (green), losses (red), draws (gray) by day."""
    fig, ax = plt.subplots(figsize=(12, 5))
    _apply_dark_style(ax, fig)

    x = range(len(days))
    bar_width = 0.7

    ax.bar(x, wins, bar_width, label="Wins", color=WIN)
    ax.bar(x, losses, bar_width, bottom=wins, label="Losses", color=LOSS)
    bottoms = [w + l for w, l in zip(wins, losses)]
    ax.bar(x, draws, bar_width, bottom=bottoms, label="Draws", color=DRAW)

    ax.set_xticks(list(x))
    ax.set_xticklabels([str(d) for d in days], fontsize=9, color=FG)
    ax.set_xlabel("Day of Month", fontsize=11)
    ax.set_ylabel("Games", fontsize=11)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.legend(loc="upper right", facecolor=BG, edgecolor=GRID, labelcolor=FG)
    ax.set_title(f"{player_name} \u2014 Daily Activity ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_season_record(
    wins: int,
    losses: int,
    draws: int,
    player_name: str,
    month_label: str,
) -> io.BytesIO:
    """Donut chart showing W/L/D breakdown with total games in center."""
    fig, ax = plt.subplots(figsize=(6, 6))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    sizes = []
    colors = []
    labels = []

    if wins > 0:
        sizes.append(wins)
        colors.append(WIN)
        labels.append(f"Wins ({wins})")
    if losses > 0:
        sizes.append(losses)
        colors.append(LOSS)
        labels.append(f"Losses ({losses})")
    if draws > 0:
        sizes.append(draws)
        colors.append(DRAW)
        labels.append(f"Draws ({draws})")

    if not sizes:
        sizes = [1]
        colors = [GRID]
        labels = ["No games"]

    total = wins + losses + draws

    wedges, texts, autotexts = ax.pie(
        sizes,
        labels=labels,
        colors=colors,
        autopct="%1.0f%%",
        startangle=90,
        wedgeprops=dict(width=0.4, edgecolor=BG, linewidth=2),
        textprops=dict(color=FG, fontsize=11),
        pctdistance=0.78,
    )
    for t in autotexts:
        t.set_color(FG)
        t.set_fontsize(10)

    # Center text
    ax.text(0, 0, f"{total}\ngames", ha="center", va="center", fontsize=18, fontweight="bold", color=FG)

    ax.set_title(f"{player_name} \u2014 Season Record ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


# ---------------------------------------------------------------------------
# All-Time (month-by-month) charts
# ---------------------------------------------------------------------------

def render_points_rank_alltime(
    months: List[str],
    points: List[float],
    ranks: List[int],
    player_name: str,
) -> io.BytesIO:
    """Dual-axis line chart: points & rank month-by-month (all time)."""
    fig, ax1 = plt.subplots(figsize=(10, 5))
    _apply_dark_style(ax1, fig)

    x = range(len(months))

    line1 = ax1.plot(x, points, color=ACCENT, marker="o", linewidth=2, markersize=6, label="Points")
    ax1.set_ylabel("Points", color=ACCENT, fontsize=11)
    ax1.tick_params(axis="y", labelcolor=ACCENT)

    ax2 = ax1.twinx()
    ax2.set_facecolor("none")
    line2 = ax2.plot(x, ranks, color=RANK_COLOR, marker="s", linewidth=2, markersize=6, linestyle="--", label="Rank")
    ax2.set_ylabel("Rank", color=RANK_COLOR, fontsize=11)
    ax2.tick_params(axis="y", labelcolor=RANK_COLOR)
    ax2.invert_yaxis()
    ax2.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax1.set_xticks(list(x))
    ax1.set_xticklabels(months, rotation=45, ha="right", fontsize=9, color=FG)

    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left", facecolor=BG, edgecolor=GRID, labelcolor=FG)

    ax1.set_title(f"{player_name} \u2014 All-Time Points & Rank", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_winrate_alltime(
    months: List[str],
    win_pcts: List[float],
    player_name: str,
) -> io.BytesIO:
    """Line chart: win rate month-by-month (all time)."""
    fig, ax = plt.subplots(figsize=(10, 5))
    _apply_dark_style(ax, fig)

    x = range(len(months))
    pcts = [p * 100 for p in win_pcts]

    ax.plot(x, pcts, color=ACCENT, marker="o", linewidth=2, markersize=6)
    ax.fill_between(x, pcts, alpha=0.15, color=ACCENT)
    ax.axhline(y=50, color=DRAW, linestyle="--", linewidth=1, alpha=0.7, label="50%")

    ax.set_ylim(0, 100)
    ax.set_ylabel("Win Rate %", fontsize=11)
    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right", fontsize=9, color=FG)

    ax.legend(loc="upper right", facecolor=BG, edgecolor=GRID, labelcolor=FG)
    ax.set_title(f"{player_name} \u2014 All-Time Win Rate", fontsize=13, color=FG, pad=12)

    return _save(fig)


# ---------------------------------------------------------------------------
# League-wide charts (/leaguegraphs)
# ---------------------------------------------------------------------------

def render_league_activity(
    days: List[int],
    wins: List[int],
    losses: List[int],
    draws: List[int],
    month_label: str,
) -> io.BytesIO:
    """Stacked bar chart: league-wide games by day (decisive vs draws)."""
    fig, ax = plt.subplots(figsize=(12, 5))
    _apply_dark_style(ax, fig)

    x = range(len(days))
    bar_width = 0.7

    ax.bar(x, wins, bar_width, label="Decisive", color=WIN)
    ax.bar(x, draws, bar_width, bottom=wins, label="Draws", color=DRAW)

    ax.set_xticks(list(x))
    ax.set_xticklabels([str(d) for d in days], fontsize=9, color=FG)
    ax.set_xlabel("Day of Month", fontsize=11)
    ax.set_ylabel("Games", fontsize=11)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.legend(loc="upper right", facecolor=BG, edgecolor=GRID, labelcolor=FG)
    ax.set_title(f"ECL League Activity ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_league_standings(
    names: List[str],
    points: List[float],
    month_label: str,
) -> io.BytesIO:
    """Horizontal bar chart of top players by points."""
    fig, ax = plt.subplots(figsize=(10, max(5, len(names) * 0.4)))
    _apply_dark_style(ax, fig)

    # Reverse so highest points appear at top
    y = range(len(names))
    ax.barh(list(y), points[::-1], color=ACCENT, height=0.6)
    ax.set_yticks(list(y))
    ax.set_yticklabels(names[::-1], fontsize=10, color=FG)
    ax.set_xlabel("Points", fontsize=11)
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_title(f"ECL Standings \u2014 Top {len(names)} ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_league_points_distribution(
    points_list: List[float],
    month_label: str,
) -> io.BytesIO:
    """Histogram of all player points."""
    fig, ax = plt.subplots(figsize=(10, 5))
    _apply_dark_style(ax, fig)

    bins = min(20, max(5, len(points_list) // 3))
    ax.hist(points_list, bins=bins, color=ACCENT, edgecolor=BG, linewidth=0.8, alpha=0.9)

    ax.set_xlabel("Points", fontsize=11)
    ax.set_ylabel("Number of Players", fontsize=11)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_title(f"ECL Points Distribution ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_league_games_distribution(
    games_list: List[int],
    month_label: str,
) -> io.BytesIO:
    """Histogram of games played per player."""
    fig, ax = plt.subplots(figsize=(10, 5))
    _apply_dark_style(ax, fig)

    bins = min(20, max(5, len(games_list) // 3))
    ax.hist(games_list, bins=bins, color=ACCENT, edgecolor=BG, linewidth=0.8, alpha=0.9)

    ax.set_xlabel("Games Played", fontsize=11)
    ax.set_ylabel("Number of Players", fontsize=11)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_title(f"ECL Games Distribution ({month_label})", fontsize=13, color=FG, pad=12)

    return _save(fig)


# ---------------------------------------------------------------------------
# League-wide all-time charts (/leaguegraphs all-time)
# ---------------------------------------------------------------------------

def render_league_activity_alltime(
    months: List[str],
    games_per_month: List[int],
) -> io.BytesIO:
    """Bar chart: total games per month across all historical months."""
    fig, ax = plt.subplots(figsize=(12, 5))
    _apply_dark_style(ax, fig)

    x = range(len(months))
    ax.bar(x, games_per_month, color=ACCENT, width=0.7, alpha=0.9)

    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right", fontsize=9, color=FG)
    ax.set_xlabel("Month", fontsize=11)
    ax.set_ylabel("Total Games", fontsize=11)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_title("ECL League Activity \u2014 All Time", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_league_participation_alltime(
    months: List[str],
    player_counts: List[int],
) -> io.BytesIO:
    """Bar chart: active player count per month."""
    fig, ax = plt.subplots(figsize=(12, 5))
    _apply_dark_style(ax, fig)

    x = range(len(months))
    ax.bar(x, player_counts, color=WIN, width=0.7, alpha=0.9)

    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right", fontsize=9, color=FG)
    ax.set_xlabel("Month", fontsize=11)
    ax.set_ylabel("Active Players", fontsize=11)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_title("ECL Participation \u2014 All Time", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_league_points_alltime(
    months: List[str],
    avg_pts: List[float],
    min_pts: List[float],
    max_pts: List[float],
) -> io.BytesIO:
    """Line chart with shaded min/max range: average points per month."""
    fig, ax = plt.subplots(figsize=(12, 5))
    _apply_dark_style(ax, fig)

    x = list(range(len(months)))

    ax.fill_between(x, min_pts, max_pts, alpha=0.15, color=ACCENT, label="Min\u2013Max range")
    ax.plot(x, avg_pts, color=ACCENT, marker="o", linewidth=2, markersize=6, label="Avg Points")

    ax.set_xticks(x)
    ax.set_xticklabels(months, rotation=45, ha="right", fontsize=9, color=FG)
    ax.set_xlabel("Month", fontsize=11)
    ax.set_ylabel("Points", fontsize=11)

    ax.legend(loc="upper left", facecolor=BG, edgecolor=GRID, labelcolor=FG)
    ax.set_title("ECL Points Spread \u2014 All Time", fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_turn_order_winrates(
    turn_rates: List[float],
    draw_rate: float,
    turn_wins: List[int],
    draws: int,
    total_pods: int,
    title: str,
) -> io.BytesIO:
    """Bar chart of win rate by seat position (1-4) + draw rate."""
    fig, ax = plt.subplots(figsize=(8, 5))
    _apply_dark_style(ax, fig)

    labels = ["Seat 1", "Seat 2", "Seat 3", "Seat 4", "Draw"]
    rates = [r * 100 for r in turn_rates] + [draw_rate * 100]
    counts = list(turn_wins) + [draws]
    colors = ["#2ECC71", "#3498DB", "#E67E22", "#E74C3C", "#95A5A6"]

    bars = ax.bar(labels, rates, color=colors, width=0.6, edgecolor=GRID, linewidth=0.5)

    # Add percentage + count labels on bars
    for bar, rate, count in zip(bars, rates, counts):
        y = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2, y + 0.5,
            f"{rate:.1f}%\n({count})",
            ha="center", va="bottom", fontsize=10, color=FG, fontweight="bold",
        )

    # 25% reference line (expected fair rate for 4 players)
    ax.axhline(y=25, color=FG, linestyle="--", linewidth=0.8, alpha=0.4)
    ax.text(len(labels) - 0.5, 25.5, "25% (fair)", fontsize=8, color=FG, alpha=0.5, ha="right")

    ax.set_ylabel("Win Rate %", fontsize=11)
    ax.set_ylim(0, max(rates) * 1.25 if rates else 40)
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))

    ax.set_title(title, fontsize=13, color=FG, pad=12)

    return _save(fig)


def render_player_stats_card(
    name: str,
    discord_handle: str,
    rank: int,
    total_players: int,
    wins: int,
    losses: int,
    draws: int,
    pts: int,
    win_pct: float,
    ow_pct: float,
    seat_stats: dict,
) -> io.BytesIO:
    """Render a dark-themed player stats card image."""
    CARD_BG = "#1E1F22"
    SECTION_BG = "#2B2D31"
    TEXT_PRIMARY = "#FFFFFF"
    TEXT_SECONDARY = "#B5BAC1"
    BORDER = "#3F4147"

    fig = plt.figure(figsize=(6, 5.5))
    fig.patch.set_facecolor(CARD_BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")
    ax.set_facecolor(CARD_BG)

    # ── Header: Name + Handle ──
    ax.text(50, 95, name, fontsize=16, fontweight="bold", color=TEXT_PRIMARY,
            ha="center", va="top")
    if discord_handle:
        ax.text(50, 90, discord_handle, fontsize=10, color=TEXT_SECONDARY,
                ha="center", va="top")

    # ── Standing + Record boxes ──
    for (x, label, value) in [
        (25, "TOURNAMENT STANDING", f"#{rank} / {total_players}"),
        (75, "RECORD", f"{wins}-{losses}-{draws}"),
    ]:
        rect = plt.Rectangle((x - 22, 72), 44, 14, facecolor=SECTION_BG,
                              edgecolor=BORDER, linewidth=0.8, clip_on=False)
        ax.add_patch(rect)
        ax.text(x, 84, label, fontsize=7, color=TEXT_SECONDARY, ha="center", va="center")
        ax.text(x, 77, value, fontsize=14, fontweight="bold", color=TEXT_PRIMARY,
                ha="center", va="center")

    # ── Tournament Stats section ──
    ax.text(5, 67, "Tournament Stats", fontsize=11, fontweight="bold", color=TEXT_PRIMARY, va="top")
    stats_rect = plt.Rectangle((3, 49), 94, 16, facecolor=SECTION_BG,
                                edgecolor=BORDER, linewidth=0.8)
    ax.add_patch(stats_rect)

    stat_rows = [
        ("Pts:", f"{pts:,}"),
        ("Win%:", f"{win_pct * 100:.2f}%"),
        ("OW%:", f"{ow_pct * 100:.2f}%"),
    ]
    for i, (label, value) in enumerate(stat_rows):
        y = 62 - i * 4.5
        ax.text(6, y, label, fontsize=9, color=TEXT_SECONDARY, va="center")
        ax.text(94, y, value, fontsize=9, color=TEXT_PRIMARY, ha="right", va="center")

    # ── Seat Position Distribution ──
    ax.text(5, 44, "Seat Position Distribution", fontsize=11, fontweight="bold",
            color=TEXT_PRIMARY, va="top")
    seat_rect = plt.Rectangle((3, 9), 94, 33, facecolor=SECTION_BG,
                               edgecolor=BORDER, linewidth=0.8)
    ax.add_patch(seat_rect)

    total_g = seat_stats.get("total_games", 0)
    for i in range(4):
        s = seat_stats.get(i, {"games": 0, "wins": 0, "win_rate": 0.0, "seat_pct": 0.0})
        y = 38 - i * 6.5
        label = f"Seat {i+1}:"
        pct = s["seat_pct"] * 100
        wr = s["win_rate"] * 100
        value = f"{pct:.1f}% ({s['games']} games) - {wr:.1f}% WR"
        ax.text(6, y, label, fontsize=9, color=TEXT_SECONDARY, va="center")
        ax.text(94, y, value, fontsize=9, color=TEXT_PRIMARY, ha="right", va="center")

    # Total games row
    ax.text(6, 12, "Total Games:", fontsize=9, color=TEXT_SECONDARY, va="center")
    ax.text(94, 12, str(total_g), fontsize=9, fontweight="bold", color=TEXT_PRIMARY,
            ha="right", va="center")

    return _save(fig)
