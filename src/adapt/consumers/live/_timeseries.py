# Copyright © 2026, UChicago Argonne, LLC
# See LICENSE for terms and disclaimer.

"""Time-series axis helpers for the dashboard — no Tk, no self references."""

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt


def style_ts_ax(ax, ylabel: str, title: str) -> None:
    """Apply light-panel styling to a time-series axis."""
    ax.set_facecolor("#f5f5f5")
    ax.set_title(title, fontsize=8, color="#222222", pad=3)
    ax.set_ylabel(ylabel, fontsize=7, color="#444444")
    ax.yaxis.label.set_color("#444444")
    ax.tick_params(axis="y", colors="#333333", labelsize=7, which="both")
    for sp in ax.spines.values():
        sp.set_color("#aaaaaa")
    ax.yaxis.grid(True, color="#cccccc", linestyle="--", linewidth=0.5, alpha=0.8, zorder=0)
    ax.xaxis.grid(False)


def apply_time_axis(ax_bottom, axes) -> None:
    """Apply shared time-axis formatting. Call after plotting, using bottom axis."""
    ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax_bottom.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=3, maxticks=10))
    ax_bottom.tick_params(axis="x", colors="#333333", labelsize=8, rotation=30)
    ax_bottom.set_xlabel("UTC", fontsize=8, color="#444444")
    ax_bottom.xaxis.label.set_color("#444444")
    for ax in axes[:-1]:
        plt.setp(ax.get_xticklabels(), visible=False)
        ax.tick_params(axis="x", colors="#aaaaaa", which="both")


def clear_time_series(axes) -> None:
    """Clear all axes and show placeholder text. axes is a tuple of 3 matplotlib axes."""
    labels = [("km²", "Area"), ("dBZ", "Reflectivity"), ("dB", "ZDR")]
    for ax, (ylabel, title) in zip(axes, labels, strict=False):
        ax.cla()
        style_ts_ax(ax, ylabel, title)
        ax.text(
            0.5,
            0.5,
            "click a cell",
            transform=ax.transAxes,
            ha="center",
            va="center",
            color="#888",
            fontsize=8,
        )
    ax_bottom = axes[-1]
    ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax_bottom.set_xlabel("UTC", fontsize=7, color="#444444")
    ax_bottom.tick_params(axis="x", colors="#333333", labelsize=7, rotation=30)
    for ax in axes[:-1]:
        plt.setp(ax.get_xticklabels(), visible=False)


def update_track_legend(
    fig,
    selected_cells: dict[str, int],
    color_slots: list[str],
) -> None:
    """Replace the figure-level track legend with colored patches for each selected uid."""
    for leg in fig.legends:
        leg.remove()
    if not selected_cells:
        return
    handles = [
        mpatches.Patch(
            facecolor=color_slots[slot % len(color_slots)],
            label=uid[:4],
        )
        for uid, slot in selected_cells.items()
    ]
    fig.legend(
        handles=handles,
        loc="lower right",
        bbox_to_anchor=(0.97, 0.01),
        fontsize=7,
        ncol=min(7, len(handles)),
        handlelength=0.8,
        framealpha=0.85,
        title="Tracks",
        title_fontsize=6,
    )


_STYLE_CHAR = {"solid": "─", "dashed": "--", "dotted": "·", "dashdot": "-·"}


def build_ts_title(group_name: str, group: dict) -> str:
    """Build a compact single-line title encoding group name and variable styles.

    Example: "Area   ─ Cell area (km²)   -- Core area (km²)"
    """
    parts = [group_name]
    for style, label in zip(group.get("styles", []), group.get("labels", []), strict=False):
        char = _STYLE_CHAR.get(style, style)
        parts.append(f"{char} {label}")
    return "   ".join(parts)


def draw_scan_marker(axes, cur_t) -> None:
    """Draw a vertical line at *cur_t* on every axis to mark the current scan time.

    No-op when *cur_t* is None.  The line is drawn above data (zorder=8) so it
    is always visible regardless of how many tracks are plotted.
    """
    if cur_t is None:
        return
    for ax in axes:
        ax.axvline(
            cur_t,
            color="#888888",
            linewidth=1.0,
            linestyle="-",
            alpha=0.7,
            zorder=8,
        )
