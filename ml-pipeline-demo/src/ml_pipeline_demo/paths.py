"""Resolve bundle source paths on Databricks workspace mounts."""

from __future__ import annotations

import sys
from pathlib import Path


def ensure_project_src_on_path(project_src: str) -> Path:
    """Add project_src (and /Workspace-prefixed variant) to sys.path; return resolved Path."""
    candidates = []
    raw = (project_src or "").rstrip("/")
    if raw:
        candidates.append(Path(raw))
        if not raw.startswith("/Workspace"):
            candidates.append(Path("/Workspace") / raw.lstrip("/"))

    for path in candidates:
        if path.exists():
            resolved = str(path.resolve())
            if resolved not in sys.path:
                sys.path.insert(0, resolved)
            return path

    raise FileNotFoundError(
        f"Could not resolve project_src={project_src!r}. Tried: {[str(c) for c in candidates]}"
    )
