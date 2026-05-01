from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, field_validator, model_validator

# Always exclude these by default
_ALWAYS_EXCLUDE_COUNTRIES = ["KZ"]
_ALWAYS_EXCLUDE_IPS = ["45.8.117.97"]


class OrderConfig(BaseModel):
    order_number: int
    rebill_counts: List[int]

    @field_validator("order_number")
    @classmethod
    def must_be_positive(cls, v: int) -> int:
        if v < 1:
            raise ValueError("order_number must be >= 1")
        return v

    @field_validator("rebill_counts")
    @classmethod
    def must_be_nonempty(cls, v: List[int]) -> List[int]:
        if not v:
            raise ValueError("rebill_counts cannot be empty")
        return v


class VersionGroup(BaseModel):
    versions: List[str]
    orders: List[OrderConfig]
    extra_filter: Optional[str] = None
    raw_orders: str = ""

    @field_validator("versions")
    @classmethod
    def versions_nonempty(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("versions cannot be empty")
        return v

    @field_validator("orders")
    @classmethod
    def orders_nonempty(cls, v: List[OrderConfig]) -> List[OrderConfig]:
        if not v:
            raise ValueError("orders cannot be empty")
        return v

    def all_rebill_counts(self) -> List[int]:
        counts: List[int] = []
        for order in self.orders:
            counts.extend(order.rebill_counts)
        return counts


class QueryFilters(BaseModel):
    exclude_countries: List[str] = []
    exclude_ips: List[str] = []
    extra_conditions: List[str] = []

    @model_validator(mode="after")
    def ensure_mandatory_exclusions(self) -> "QueryFilters":
        for c in _ALWAYS_EXCLUDE_COUNTRIES:
            if c not in self.exclude_countries:
                self.exclude_countries.append(c)
        for ip in _ALWAYS_EXCLUDE_IPS:
            if ip not in self.exclude_ips:
                self.exclude_ips.append(ip)
        return self


class ABTestConfig(BaseModel):
    test_name: str
    release_date: datetime
    end_date: Optional[datetime] = None
    control: VersionGroup
    test: VersionGroup
    filters: QueryFilters = QueryFilters()
    slack_channel: str = ""
    custom_sql: Optional[str] = None

    @model_validator(mode="after")
    def no_version_overlap(self) -> "ABTestConfig":
        overlap = set(self.control.versions) & set(self.test.versions)
        if overlap:
            raise ValueError(f"Same version in both control and test: {overlap}")
        return self

    def all_rebill_counts(self) -> List[int]:
        counts = set(self.control.all_rebill_counts())
        counts.update(self.test.all_rebill_counts())
        return sorted(counts)

    def all_version_names(self) -> List[str]:
        return self.control.versions + self.test.versions

    @property
    def control_label(self) -> str:
        return " + ".join(self.control.versions)

    @property
    def test_label(self) -> str:
        return " + ".join(self.test.versions)


class ValidationReport(BaseModel):
    passed: bool
    errors: List[str] = []
    warnings: List[str] = []


class MetricResult(BaseModel):
    metric_key: str
    label: str
    fmt: str
    higher_is_better: bool
    control_value: Optional[float]
    test_value: Optional[float]
    p_value: Optional[float] = None
    is_significant: bool = False
    delta_abs: Optional[float] = None
    delta_pct: Optional[float] = None


class SliceResult(BaseModel):
    slice_key: str
    control_metrics: Dict[str, Any]
    test_metrics: Dict[str, Any]


class AnalysisResult(BaseModel):
    run_id: str
    config: ABTestConfig
    timestamp: datetime = datetime.utcnow()
    overall: SliceResult
    slices: Dict[str, SliceResult] = {}
    metric_results: List[MetricResult] = []
    overall_recommendation: Literal["ship", "do_not_ship", "inconclusive"] = "inconclusive"
    narrative_summary: str = ""
    artifact_paths: Dict[str, str] = {}
