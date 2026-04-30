from __future__ import annotations

import json
import logging
import uuid

from ab_agent.bigquery.client import BQClient
from ab_agent.bigquery.query_builder import build_query
from ab_agent.core.models import ABTestConfig
from ab_agent.db.repository import SnapshotRepo, TestRepo
from ab_agent.integrations.storage import ArtifactStore
from ab_agent.stats.engine import calc_metrics, serialize_metrics
from ab_agent.visualization.infographic import compute_slices, render_html_dashboard_string
from ab_agent.visualization.screenshot import render_summary_png

logger = logging.getLogger(__name__)


def run_refresh(test_id: str) -> None:
    logger.info("Running refresh for test %s", test_id)
    try:
        _do_refresh(test_id)
    except Exception:
        logger.exception("Refresh failed for test %s", test_id)


def _do_refresh(test_id: str) -> None:
    test_row = TestRepo().get(test_id)
    if not test_row:
        logger.warning("Test %s not found, skipping refresh", test_id)
        return

    config: ABTestConfig = ABTestConfig.model_validate_json(test_row["config_json"])

    sql = build_query(config)
    df = BQClient().execute(sql, use_cache=False)

    if df.empty:
        logger.warning("No data for test %s", test_id)
        return

    ctrl_df = df[df["split"].isin(config.control.versions)].copy()
    test_df = df[df["split"].isin(config.test.versions)].copy()

    ctrl_m = calc_metrics(ctrl_df)
    test_m = calc_metrics(test_df)
    slices, dim_values = compute_slices(ctrl_df, test_df, calc_metrics)

    store = ArtifactStore()
    store.ensure_dirs()
    snap_id = str(uuid.uuid4())

    png_path = store.screenshot_path(snap_id, "summary")
    render_summary_png(ctrl_m, test_m, config, png_path)

    dashboard_html = render_html_dashboard_string(slices, dim_values, config)

    SnapshotRepo().save(
        snapshot_id=snap_id,
        test_id=test_id,
        ctrl_metrics=serialize_metrics(ctrl_m),
        test_metrics=serialize_metrics(test_m),
        slices=slices,
        screenshot_path=str(png_path),
        dashboard_html=dashboard_html,
    )

    if config.slack_channel:
        try:
            from ab_agent.integrations.slack import SlackClient
            slack = SlackClient()
            if png_path.exists():
                slack.upload_file(
                    channel=config.slack_channel,
                    file_path=png_path,
                    title=f"[4h update] {config.test_name}",
                    initial_comment=f"Auto-refresh snapshot for *{config.test_name}*",
                )
            else:
                ctrl_s = serialize_metrics(ctrl_m)
                test_s = serialize_metrics(test_m)
                lines = [f"*[Refresh] {config.test_name}*"]
                for key in ("cvr", "ttp_rate", "gain_per_view"):
                    cv = ctrl_s.get(key)
                    tv = test_s.get(key)
                    if cv is not None and tv is not None:
                        lines.append(f"• {key}: ctrl={cv:.4f} test={tv:.4f} Δ={tv - cv:+.4f}")
                slack.send_message(channel=config.slack_channel, text="\n".join(lines))
        except Exception:
            logger.exception("Slack send failed for test %s", test_id)

    logger.info("Refresh complete for test %s, snapshot %s", test_id, snap_id)
