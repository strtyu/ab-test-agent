from __future__ import annotations

from datetime import datetime
from pathlib import Path

from ab_agent.core.config_loader import get_settings


def _writable_base() -> Path:
    settings = get_settings()
    candidate = Path(settings["artifacts"]["local_dir"])
    try:
        candidate.mkdir(parents=True, exist_ok=True)
        test = candidate / ".write_test"
        test.touch()
        test.unlink()
        return candidate
    except (OSError, PermissionError):
        fallback = Path("/tmp/artifacts")
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


class ArtifactStore:
    def __init__(self) -> None:
        self._base = _writable_base()

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
