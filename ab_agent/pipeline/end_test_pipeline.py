from __future__ import annotations

from ab_agent.core.models import ABTestConfig, AnalysisResult
from ab_agent.db.repository import ExperimentRepo
from ab_agent.integrations.slack import SlackClient
from ab_agent.integrations.storage import ArtifactStore
from ab_agent.pipeline.analysis_pipeline import AnalysisPipeline
from ab_agent.visualization.screenshot import render_summary_png


class EndTestPipeline:
    def run(self, config: ABTestConfig) -> AnalysisResult:
        result = AnalysisPipeline().run(config)

        # Upload infographic with conclusion text to Slack
        if config.slack_channel:
            png_path_str = result.artifact_paths.get("screenshot", "")
            rec = result.overall_recommendation.upper().replace("_", " ")
            conclusion = (
                f"*FINAL RESULT — {config.test_name}*\n"
                f"Recommendation: *{rec}*\n"
                f"{result.narrative_summary}"
            )
            try:
                slack = SlackClient()
                if png_path_str:
                    from pathlib import Path
                    p = Path(png_path_str)
                    if p.exists():
                        slack.upload_file(
                            channel=config.slack_channel,
                            file_path=p,
                            title=f"Final A/B Test Results: {config.test_name}",
                            initial_comment=conclusion,
                        )
                    else:
                        slack.send_message(
                            channel=config.slack_channel,
                            text=conclusion,
                        )
            except Exception:
                pass

        ExperimentRepo().mark_ended(result.run_id)
        return result
