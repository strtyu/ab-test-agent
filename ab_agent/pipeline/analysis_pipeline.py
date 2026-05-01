from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

from ab_agent.bigquery.client import BQClient
from ab_agent.bigquery.query_builder import build_query, _strip_channel
from ab_agent.core.models import ABTestConfig, MetricResult
from ab_agent.db.repository import AnalysisRepo
from ab_agent.integrations.storage import ArtifactStore
from ab_agent.stats.engine import run_bootstrap_analysis, calc_metrics, serialize_metrics

logger = logging.getLogger(__name__)


def _determine_recommendation(results: list[MetricResult]) -> Literal["ship", "do_not_ship", "inconclusive"]:
    primary = {"cvr", "gain_per_view", "ttp_rate"}
    sig = [r for r in results if r.is_significant]
    if not sig:
        return "inconclusive"
    primary_sig = [r for r in sig if r.metric_key in primary]
    if not primary_sig:
        return "inconclusive"
    bad = [
        r for r in primary_sig
        if (r.higher_is_better and (r.test_value or 0) < (r.control_value or 0))
        or (not r.higher_is_better and (r.test_value or 0) > (r.control_value or 0))
    ]
    return "do_not_ship" if bad else "ship"


def run_analysis(test_id: str, config: ABTestConfig) -> str:
    """Run bootstrap analysis. Returns analysis_id."""
    store = ArtifactStore()
    store.ensure_dirs()
    analysis_id = str(uuid.uuid4())

    sql = build_query(config)
    df = BQClient().execute(sql, use_cache=False)

    if df.empty:
        raise ValueError("No data returned from BigQuery")

    ctrl_df = df[df["split"].isin([_strip_channel(v) for v in config.control.versions])].copy()
    test_df = df[df["split"].isin([_strip_channel(v) for v in config.test.versions])].copy()

    results = run_bootstrap_analysis(ctrl_df, test_df, config)
    recommendation = _determine_recommendation(results)

    ctrl_m = calc_metrics(ctrl_df)
    test_m = calc_metrics(test_df)

    png_path: Path | None = None
    try:
        from ab_agent.visualization.screenshot import render_summary_png
        p = store.screenshot_path(analysis_id, "analysis")
        render_summary_png(ctrl_m, test_m, config, p)
        png_path = p
    except Exception:
        logger.debug("Screenshot render skipped")

    narrative = ""
    try:
        from ab_agent.agents.narrative import NarrativeAgent
        from ab_agent.core.models import AnalysisResult, SliceResult
        result_obj = AnalysisResult(
            run_id=analysis_id,
            config=config,
            timestamp=datetime.utcnow(),
            overall=SliceResult(
                slice_key="overall",
                control_metrics=serialize_metrics(ctrl_m),
                test_metrics=serialize_metrics(test_m),
            ),
            metric_results=results,
            overall_recommendation=recommendation,
        )
        narrative = NarrativeAgent().generate(result_obj)
    except Exception:
        logger.exception("Narrative generation failed")

    results_json = json.dumps([r.model_dump() for r in results])
    AnalysisRepo().save(
        analysis_id=analysis_id,
        test_id=test_id,
        results_json=results_json,
        recommendation=recommendation,
        narrative=narrative,
        screenshot_path=str(png_path) if png_path else "",
    )

    if config.slack_channel:
        try:
            from ab_agent.integrations.slack import SlackClient
            rec_label = {"ship": "✅ SHIP", "do_not_ship": "❌ DO NOT SHIP", "inconclusive": "❓ INCONCLUSIVE"}
            slack = SlackClient()
            slack.send_message(
                channel=config.slack_channel,
                text=f"*{config.test_name}* — Analysis complete: {rec_label.get(recommendation, recommendation)}\n{narrative}",
            )
            if png_path and png_path.exists():
                slack.upload_file(
                    channel=config.slack_channel,
                    file_path=png_path,
                    title=f"Analysis: {config.test_name}",
                )
        except Exception:
            logger.exception("Slack send failed")

    return analysis_id
