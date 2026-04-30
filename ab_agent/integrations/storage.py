from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ab_agent.core.config_loader import get_settings


class ArtifactStore:
    def __init__(self) -> None:
        settings = get_settings()
        self._base = Path(settings["artifacts"]["local_dir"])
        self._base.mkdir(parents=True, exist_ok=True)

    def screenshot_path(self, run_id: str, suffix: str = "dashboard") -> Path:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return self._base / "screenshots" / f"{run_id}_{suffix}_{ts}.png"

    def infographic_path(self, run_id: str) -> Path:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return self._base / "infographics" / f"{run_id}_{ts}.png"

    def html_path(self, run_id: str) -> Path:
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return self._base / "dashboards" / f"{run_id}_{ts}.html"

    def ensure_dirs(self) -> None:
        for sub in ("screenshots", "infographics", "dashboards"):
            (self._base / sub).mkdir(parents=True, exist_ok=True)
