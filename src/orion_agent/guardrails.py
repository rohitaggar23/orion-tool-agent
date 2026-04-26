from __future__ import annotations
from pathlib import Path
import re

class GuardrailError(ValueError):
    pass

def enforce_select_only(sql: str) -> None:
    compact = re.sub(r"\s+", " ", sql.strip().lower())
    if not compact.startswith("select"):
        raise GuardrailError("Only SELECT queries are allowed")
    banned = [" insert ", " update ", " delete ", " drop ", " alter ", " pragma ", " attach "]
    if any(b in f" {compact} " for b in banned):
        raise GuardrailError("Mutating or administrative SQL is not allowed")

def resolve_sandbox_path(root: str, requested: str) -> Path:
    base = Path(root).resolve()
    path = (base / requested).resolve()
    if base not in [path, *path.parents]:
        raise GuardrailError("Path escapes sandbox")
    return path
