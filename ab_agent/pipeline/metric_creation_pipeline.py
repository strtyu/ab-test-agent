from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml

from ab_agent.agents.sql_generator import SQLGeneratorAgent
from ab_agent.agents.stat_generator import StatGeneratorAgent
from ab_agent.agents.validator import ValidatorAgent
from ab_agent.agents.viz_generator import VizGeneratorAgent
from ab_agent.bigquery.client import BQClient
from ab_agent.bigquery.query_builder import QueryBuilder
from ab_agent.bigquery.schema_inspector import SchemaInspector
from ab_agent.core.exceptions import BQDryRunError, MetricValidationError
from ab_agent.core.metric_registry import get_registry
from ab_agent.core.models import ExperimentConfig, MetricDefinition, VizConfig
from ab_agent.db.repository import MetricRepo

_CUSTOM_DIR = Path(__file__).parent.parent.parent / "config" / "metrics" / "custom"
_MAX_SQL_RETRIES = 3
_MAX_VIZ_RETRIES = 3

_PLACEHOLDER_CONFIG = ExperimentConfig(
    experiment_id="validation_placeholder",
    variant_name="variant",
    control_name="control",
    start_date=datetime(2024, 1, 1).date(),
    end_date=datetime(2024, 1, 31).date(),
    metrics=["placeholder"],
    slack_channel="#test",
)


class MetricCreationPipeline:
    def __init__(self) -> None:
        self._sql_agent = SQLGeneratorAgent()
        self._viz_agent = VizGeneratorAgent()
        self._stat_agent = StatGeneratorAgent()
        self._validator = ValidatorAgent()
        self._bq = BQClient()
        self._query_builder = QueryBuilder()
        self._schema = SchemaInspector()
        self._registry = get_registry()
        self._metric_repo = MetricRepo()

    def run(self, name: str, description: str) -> MetricDefinition:
        # Validate name
        name = name.strip().lower().replace(" ", "_")
        if not name:
            raise MetricValidationError("Metric name cannot be empty")
        if name in self._registry.names():
            raise MetricValidationError(f"Metric '{name}' already exists")

        # Get BQ schema for AI context
        schema_context = self._schema.get_context_for_ai()

        # Step 1: Classify metric type
        stat_info = self._stat_agent.classify(name, description)
        metric_type = stat_info["metric_type"]
        stat_method = stat_info["stat_method"]

        # Step 2: Generate + validate SQL CTE (with retry loop)
        cte_sql = self._generate_sql_with_retries(name, description, schema_context)

        # Step 3: Validate SQL structure via LLM
        sql_report = self._validator.validate_sql_cte(cte_sql)
        if not sql_report.passed:
            raise MetricValidationError(
                f"SQL validation failed: {'; '.join(sql_report.errors)}"
            )

        # Step 4: Generate + validate visualization code
        viz_code = self._generate_viz_with_retries(name, description, metric_type)

        # Step 5: Build display name
        display_name = name.replace("_", " ").title()

        # Step 6: Construct MetricDefinition
        metric = MetricDefinition(
            name=name,
            display_name=display_name,
            description=description,
            metric_type=metric_type,
            stat_method=stat_method,
            is_custom=True,
            sql_template=cte_sql,
            viz_config=VizConfig(),
            custom_stat_code=None,
            created_at=datetime.utcnow(),
            created_by="ai",
        )

        # Step 7: Write YAML
        self._write_yaml(metric, viz_code)

        # Step 8: Hot-reload registry
        self._registry.reload()

        # Step 9: Save to DB
        self._metric_repo.save(metric)

        return metric

    def _generate_sql_with_retries(
        self, name: str, description: str, schema_context: str
    ) -> str:
        history = []
        error_feedback = ""
        last_cte = ""

        for attempt in range(_MAX_SQL_RETRIES):
            cte_sql = self._sql_agent.generate(
                metric_name=name,
                description=description,
                bq_schema_context=schema_context,
                error_feedback=error_feedback,
                history=history if attempt > 0 else None,
            )
            last_cte = cte_sql

            # Build validation query and dry-run
            try:
                validation_sql = self._query_builder.build_for_metric_validation(
                    cte_sql, name, _PLACEHOLDER_CONFIG
                )
                self._bq.dry_run(validation_sql)
                return cte_sql  # Success
            except BQDryRunError as e:
                error_feedback = str(e)
                # Add to conversation history so agent sees context
                history.append({"role": "assistant", "content": cte_sql})
                history.append({"role": "user", "content": f"BQ dry-run error: {error_feedback}"})

        raise MetricValidationError(
            f"SQL generation failed after {_MAX_SQL_RETRIES} attempts. "
            f"Last error: {error_feedback}"
        )

    def _generate_viz_with_retries(
        self, name: str, description: str, metric_type: str
    ) -> str:
        history = []
        error_feedback = ""

        for attempt in range(_MAX_VIZ_RETRIES):
            viz_code = self._viz_agent.generate(
                metric_name=name,
                description=description,
                metric_type=metric_type,
                error_feedback=error_feedback,
                history=history if attempt > 0 else None,
            )

            report = self._validator.validate_python_code(viz_code, "make_chart")
            if report.passed:
                return viz_code

            error_feedback = "; ".join(report.errors)
            history.append({"role": "assistant", "content": viz_code})
            history.append({"role": "user", "content": f"Validation error: {error_feedback}"})

        raise MetricValidationError(
            f"Viz code generation failed after {_MAX_VIZ_RETRIES} attempts. "
            f"Last error: {error_feedback}"
        )

    def _write_yaml(self, metric: MetricDefinition, viz_code: str) -> None:
        _CUSTOM_DIR.mkdir(parents=True, exist_ok=True)
        data = {
            "name": metric.name,
            "display_name": metric.display_name,
            "description": metric.description,
            "metric_type": metric.metric_type,
            "stat_method": metric.stat_method,
            "is_custom": True,
            "version": 1,
            "created_at": metric.created_at.isoformat(),
            "created_by": metric.created_by,
            "sql_template": metric.sql_template,
            "covariate_sql": None,
            "viz_config": metric.viz_config.model_dump(),
            "custom_viz_code": viz_code,
        }
        yaml_path = _CUSTOM_DIR / f"{metric.name}.yaml"
        with open(yaml_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
