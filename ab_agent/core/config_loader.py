from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict

import yaml


_ROOT = Path(__file__).parent.parent.parent
_SETTINGS_PATH = _ROOT / "config" / "settings.yaml"

_env_pattern = re.compile(r"\$\{([^}:-]+)(?::-([^}]*))?\}")


def _resolve_env(value: str) -> str:
    def replacer(m: re.Match) -> str:
        var, default = m.group(1), m.group(2)
        return os.environ.get(var, default or "")

    return _env_pattern.sub(replacer, value)


def _resolve_dict(d: Any) -> Any:
    if isinstance(d, dict):
        return {k: _resolve_dict(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_resolve_dict(i) for i in d]
    if isinstance(d, str):
        return _resolve_env(d)
    return d


def load_settings(path: Path = _SETTINGS_PATH) -> Dict[str, Any]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return _resolve_dict(raw)


_settings: Dict[str, Any] | None = None


def get_settings() -> Dict[str, Any]:
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings
