from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List

@dataclass
class ToolCall:
    name: str
    args: Dict[str, Any]

@dataclass
class TraceStep:
    tool: str
    args: Dict[str, Any]
    observation: str

@dataclass
class AgentResult:
    answer: str
    trace: List[TraceStep] = field(default_factory=list)
    confidence: float = 0.0
