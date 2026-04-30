from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import yaml

from ab_agent.core.exceptions import MetricNotFoundError, MetricValidationError
from ab_agent.core.models import MetricDefinition, VizConfig

_METRICS_DIR = Path(__file__).parent.parent.parent / "config" / "metrics"


def _load_yaml(path: Path) -> MetricDefinition:
    with open(path) as f:
        data = yaml.safe_load(f)
    try:
        viz_raw = data.pop("viz_config", {})
        data["viz_config"] = VizConfig(**viz_raw) if viz_raw else VizConfig()
        return MetricDefinition(**data)
    except Exception as e:
        raise MetricValidationError(f"Invalid metric file {path.name}: {e}") from e


class MetricRegistry:
    def __init__(self) -> None:
        self._metrics: Dict[str, MetricDefinition] = {}
        self.load_all()

    def load_all(self) -> None:
        self._metrics.clear()
        for path in sorted(_METRICS_DIR.rglob("*.yaml")):
            if path.stem.startswith("_"):
                continue
            m = _load_yaml(path)
            self._metrics[m.name] = m

    def reload(self) -> None:
        self.load_all()

    def get(self, name: str) -> MetricDefinition:
        if name not in self._metrics:
            raise MetricNotFoundError(name)
        return self._metrics[name]

    def resolve(self, names: List[str]) -> List[MetricDefinition]:
        return [self.get(n) for n in names]

    def all(self) -> List[MetricDefinition]:
        return list(self._metrics.values())

    def names(self) -> List[str]:
        return list(self._metrics.keys())


_registry: MetricRegistry | None = None


def get_registry() -> MetricRegistry:
    global _registry
    if _registry is None:
        _registry = MetricRegistry()
    return _registry
