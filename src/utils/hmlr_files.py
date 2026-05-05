"""Helpers for resolving HM Land Registry certificate files from env config."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def is_production_environment(app_env: str) -> bool:
    """Return True when the application is running in a production-like env."""
    return app_env.strip().lower() in {"prod", "production"}


def resolve_hmlr_file(
    path_value: str,
    *,
    content: Optional[str],
    app_env: str,
    label: str,
    content_env_name: str,
) -> Path:
    """
    Resolve an HMLR file path from env configuration.

    Resolution order:
    1. Use an existing file at the configured path.
    2. If file contents were supplied in an env var, write them to the path.
    3. In production, raise if the file is still missing.
    4. In development, raise and point the user at the configured path.
    """
    if not path_value or not path_value.strip():
        raise RuntimeError(f"{label} is not configured.")

    path = Path(path_value).expanduser()

    if path.exists():
        if not path.is_file():
            raise RuntimeError(f"{label} path exists but is not a file: {path}")
        if path.stat().st_size == 0:
            raise RuntimeError(f"{label} exists but is empty: {path}")
        return path

    if content and content.strip():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        logger.info("Wrote %s to %s from %s", label, path, content_env_name)
        return path

    if is_production_environment(app_env):
        raise RuntimeError(
            f"{label} is missing at {path} and {content_env_name} is not set. "
            "Provide the file contents in env vars or mount the file at the "
            "configured path."
        )

    raise RuntimeError(
        f"{label} is missing at {path}. In dev, create the file at that path "
        f"or provide {content_env_name}."
    )
