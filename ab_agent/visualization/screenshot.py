from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from ab_agent.core.models import ABTestConfig
from ab_agent.stats.engine import ABS_METRICS, REL_METRICS
from ab_agent.visualization.chart_library import (
    BLUE, BLUE_LIGHT, CTRL_CLR, DARK, GOOD_CLR, BAD_CLR,
    GRAY_BG, GRAY_LINE, NEUT_CLR, TEST_CLR, WHITE,
    _shorten, calc_delta, fmt_value, metric_direction,
)


def _draw_section(ax, title, metrics, ctrl_m, test_m, ctrl_lbl, test_lbl):
    n = len(metrics)
    ctrl_row  = [fmt_value(ctrl_m.get(k), f)                      for k, _, f, _ in metrics]
    test_row  = [fmt_value(test_m.get(k), f)                      for k, _, f, _ in metrics]
    delta_row = [calc_delta(ctrl_m.get(k), test_m.get(k), f)[0]  for k, _, f, h in metrics]
    dpct_row  = [calc_delta(ctrl_m.get(k), test_m.get(k), f)[1]  for k, _, f, h in metrics]
    dirs      = [metric_direction(ctrl_m.get(k), test_m.get(k), h) for k, _, f, h in metrics]

    def dc(d):
        if d == "good": return "#16A34A26"
        if d == "bad":  return "#DC262626"
        return WHITE

    cell_text   = [ctrl_row, test_row, delta_row, dpct_row]
    cell_colors = [
        ["#EBF3FF"] * n, ["#FEF3E2"] * n,
        [dc(d) for d in dirs], [dc(d) for d in dirs],
    ]
    row_labels  = [ctrl_lbl, test_lbl, "Δ absolute", "Δ %"]
    row_colors  = ["#EBF3FF", "#FEF3E2", GRAY_BG, GRAY_BG]

    ax.axis("off")
    ax.set_facecolor(WHITE)
    ax.text(
        0.0, 1.02, title, transform=ax.transAxes,
        fontsize=9, fontweight="bold", color=DARK, va="bottom", ha="left",
        bbox=dict(boxstyle="round,pad=0.3", facecolor=BLUE_LIGHT, edgecolor=BLUE, linewidth=0.8),
    )
    tbl = ax.table(
        cellText=cell_text, rowLabels=row_labels,
        colLabels=[m[1] for m in metrics],
        cellColours=cell_colors, rowColours=row_colors,
        colColours=[DARK] * n,
        cellLoc="center", loc="center", colWidths=[1.0 / n] * n,
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8.5)
    tbl.scale(1, 1.7)

    for j in range(n):
        c = tbl[0, j]
        c.set_text_props(color=WHITE, fontweight="bold", fontsize=8)
        c.set_facecolor(DARK)
        c.set_edgecolor(GRAY_LINE)

    for i, (_, bg) in enumerate(zip(row_labels, row_colors)):
        c = tbl[i + 1, -1]
        c.set_text_props(
            fontweight="bold", fontsize=8,
            color=CTRL_CLR if i == 0 else TEST_CLR if i == 1 else DARK,
        )
        c.set_facecolor(bg)
        c.set_edgecolor(GRAY_LINE)

    for i in range(4):
        for j in range(n):
            c = tbl[i + 1, j]
            c.set_edgecolor(GRAY_LINE)
            c.set_linewidth(0.5)
            if i == 0:
                c.set_text_props(color=CTRL_CLR, fontweight="bold")
            elif i == 1:
                c.set_text_props(color=TEST_CLR, fontweight="bold")
            else:
                d = dirs[j]
                c.set_text_props(
                    color=GOOD_CLR if d == "good" else BAD_CLR if d == "bad" else NEUT_CLR,
                    fontweight="bold" if d in ("good", "bad") else "normal",
                )


def render_summary_png(
    ctrl_m: Dict[str, Any],
    test_m: Dict[str, Any],
    config: ABTestConfig,
    path: Path,
) -> Path:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    cs = _shorten(" + ".join(config.control.versions), 30)
    ts = _shorten(" + ".join(config.test.versions), 30)

    fig = plt.figure(figsize=(19, 5.8), facecolor=WHITE)
    hdr = 1.1 / 5.8
    ax_h = fig.add_axes([0, 1 - hdr, 1, hdr])
    ax_h.set_facecolor(DARK)
    ax_h.axis("off")
    ax_h.text(0.02, 0.62, config.test_name, transform=ax_h.transAxes,
              color=WHITE, fontsize=15, fontweight="bold", va="center")
    ax_h.text(0.02, 0.22, f"Release: {config.release_date.strftime('%Y-%m-%d %H:%M')} UTC",
              transform=ax_h.transAxes, color="#94A3B8", fontsize=9, va="center")
    ax_h.plot(0.68, 0.72, "o", color=CTRL_CLR, ms=8, transform=ax_h.transAxes)
    ax_h.text(0.695, 0.72, f"Control: {cs}", transform=ax_h.transAxes, color=WHITE, fontsize=9, va="center")
    ax_h.plot(0.68, 0.28, "o", color=TEST_CLR, ms=8, transform=ax_h.transAxes)
    ax_h.text(0.695, 0.28, f"Test:    {ts}", transform=ax_h.transAxes, color=WHITE, fontsize=9, va="center")

    top   = 1 - hdr - 0.01
    avail = top - 0.03
    ax_abs = fig.add_axes([0.01, top - avail * 0.56, 0.98, avail * 0.56])
    _draw_section(ax_abs, "Absolute Values", ABS_METRICS, ctrl_m, test_m, cs, ts)
    ax_rel = fig.add_axes([0.01, top - avail * 0.56 - 0.01 - avail * 0.40, 0.98, avail * 0.40])
    _draw_section(ax_rel, "Relative Values", REL_METRICS, ctrl_m, test_m, cs, ts)

    ax_f = fig.add_axes([0, 0, 1, 0.025])
    ax_f.set_facecolor(GRAY_BG)
    ax_f.axis("off")
    ax_f.text(0.5, 0.5, "▲ test better   ▼ test worse   — no data",
              transform=ax_f.transAxes, color=NEUT_CLR, fontsize=8, ha="center", va="center")

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(str(path), dpi=150, bbox_inches="tight", facecolor=WHITE)
    plt.close()
    return path
