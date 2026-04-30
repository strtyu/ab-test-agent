from __future__ import annotations

import logging
import uuid

from ab_agent.bigquery.client import BQClient
from ab_agent.bigquery.query_builder import build_query
from ab_agent.core.models import ABTestConfig
from ab_agent.db.repository import SnapshotRepo, TestRepo
from ab_agent.stats.engine import calc_metrics, serialize_metrics
from ab_agent.visualization.infographic import compute_slices, render_html_dashboard_string

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

    snap_id = str(uuid.uuid4())

    # Render dashboard HTML as string — stored in DB, no filesystem needed
    dashboard_html = render_html_dashboard_string(slices, dim_values, config)

    SnapshotRepo().save(
        snapshot_id=snap_id,
        test_id=test_id,
        ctrl_metrics=serialize_metrics(ctrl_m),
        test_metrics=serialize_metrics(test_m),
        slices=slices,
        dashboard_html=dashboard_html,
    )

    # Send to Slack (text summary only — no file upload without filesystem)
    if config.slack_channel:
        try:
            from ab_agent.integrations.slack import SlackClient
            ctrl_s = serialize_metrics(ctrl_m)
            test_s = serialize_metrics(test_m)
            lines = [f"*[Refresh] {config.test_name}*"]
            for key in ("cvr", "ttp_rate", "gain_per_view"):
                cv = ctrl_s.get(key)
                tv = test_s.get(key)
                if cv is not None and tv is not None:
                    delta = tv - cv
                    lines.append(f"• {key}: ctrl={cv:.4f} test={tv:.4f} Δ={delta:+.4f}")
            SlackClient().send_message(channel=config.slack_channel, text="\n".join(lines))
        except Exception:
            logger.exception("Slack send failed for test %s", test_id)

    logger.info("Refresh complete for test %s, snapshot %s", test_id, snap_id)
