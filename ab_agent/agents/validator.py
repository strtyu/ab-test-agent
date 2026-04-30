from __future__ import annotations

import json
import subprocess
import sys
import textwrap
import tempfile
from pathlib import Path

from ab_agent.agents.base import BaseAgent
from ab_agent.core.exceptions import AgentError, CodeExecutionError
from ab_agent.core.models import ValidationReport

_FORBIDDEN_PATTERNS = [
    "os.system", "subprocess", "eval(", "exec(", "__import__",
    "open(", "shutil", "pathlib.Path", "import os",
]


class ValidatorAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("validator_system.md")

    def validate_config_text(self, config_json: str) -> ValidationReport:
        prompt = f"Validate this experiment config JSON:\n\n{config_json}"
        return self._parse_report(self.call(prompt))

    def validate_sql_cte(self, cte_sql: str) -> ValidationReport:
        prompt = f"Validate this BigQuery SQL CTE:\n\n{cte_sql}"
        return self._parse_report(self.call(prompt))

    def validate_python_code(self, code: str, expected_function: str) -> ValidationReport:
        # Static check for forbidden patterns
        errors = []
        for pattern in _FORBIDDEN_PATTERNS:
            if pattern in code:
                errors.append(f"Forbidden pattern found: {pattern}")

        if errors:
            return ValidationReport(passed=False, errors=errors)

        # Ask LLM to review
        prompt = (
            f"Validate this Python function code.\n"
            f"Expected function name: {expected_function}\n\n{code}"
        )
        report = self._parse_report(self.call(prompt))
        if not report.passed:
            return report

        # Execute in subprocess sandbox with synthetic data
        try:
            self._sandbox_execute(code, expected_function)
        except CodeExecutionError as e:
            return ValidationReport(passed=False, errors=[str(e)])

        return ValidationReport(passed=True)

    def _sandbox_execute(self, code: str, function_name: str) -> None:
        test_code = textwrap.dedent(f"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go

{code}

# Test with synthetic data
df = pd.DataFrame({{
    "user_id": ["u1", "u2", "u3", "u4"],
    "variant": ["control", "control", "variant", "variant"],
    "value": [0.0, 1.0, 1.0, 0.0],
}})
result = {function_name}(df, "control", "variant")
assert isinstance(result, go.Figure), "Function must return a go.Figure"
print("OK")
""")
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(test_code)
            tmp_path = f.name

        try:
            proc = subprocess.run(
                [sys.executable, tmp_path],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if proc.returncode != 0:
                raise CodeExecutionError(
                    f"Code execution failed:\n{proc.stderr or proc.stdout}"
                )
        except subprocess.TimeoutExpired:
            raise CodeExecutionError("Code execution timed out (15s)")
        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def _parse_report(self, response: str) -> ValidationReport:
        text = response.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip().rstrip("```").strip()

        try:
            data = json.loads(text)
            return ValidationReport(
                passed=data.get("passed", False),
                errors=data.get("errors", []),
                warnings=data.get("warnings", []),
            )
        except json.JSONDecodeError:
            return ValidationReport(
                passed=False,
                errors=[f"Validator returned invalid JSON: {response[:200]}"],
            )
