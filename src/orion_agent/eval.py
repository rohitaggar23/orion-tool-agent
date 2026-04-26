from __future__ import annotations
import json
from typing import List, Dict
from .planner import RulePlanner

class TaskEvaluator:
    def __init__(self, planner: RulePlanner):
        self.planner = planner

    def run_jsonl(self, path: str) -> Dict[str, float]:
        total = 0
        passed = 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                row = json.loads(line)
                total += 1
                result = self.planner.run(row["question"])
                if all(term.lower() in result.answer.lower() for term in row.get("expected_terms", [])):
                    passed += 1
        return {"total": total, "passed": passed, "success_rate": passed / max(1, total)}
