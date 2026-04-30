from __future__ import annotations

from typing import Dict, List

from google.cloud import bigquery

from ab_agent.core.config_loader import get_settings


class SchemaInspector:
    def __init__(self) -> None:
        settings = get_settings()
        bq_cfg = settings["bigquery"]
        self.project = bq_cfg["project"]
        self.dataset = bq_cfg["dataset"]
        self.assignments_table = bq_cfg.get("assignments_table", "experiment_assignments")
        self.events_table = bq_cfg.get("events_table", "events")
        self._client = bigquery.Client(project=self.project)
        self._cache: Dict[str, List[Dict]] = {}

    def get_table_schema(self, table_name: str) -> List[Dict]:
        if table_name in self._cache:
            return self._cache[table_name]

        table_ref = f"{self.project}.{self.dataset}.{table_name}"
        try:
            table = self._client.get_table(table_ref)
            schema = [
                {"name": f.name, "type": f.field_type, "mode": f.mode}
                for f in table.schema
            ]
        except Exception:
            schema = []

        self._cache[table_name] = schema
        return schema

    def get_context_for_ai(self) -> str:
        assignments = self.get_table_schema(self.assignments_table)
        events = self.get_table_schema(self.events_table)

        lines = [
            f"BigQuery project: {self.project}, dataset: {self.dataset}",
            "",
            f"Table: {self.assignments_table}",
            "Columns:",
        ]
        for col in assignments:
            lines.append(f"  - {col['name']} ({col['type']})")

        lines += ["", f"Table: {self.events_table}", "Columns:"]
        for col in events:
            lines.append(f"  - {col['name']} ({col['type']})")

        return "\n".join(lines)
