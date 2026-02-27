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
    """Stacked bar chart: league-wide wins/losses/draws by day."""
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
