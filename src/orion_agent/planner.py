from __future__ import annotations
import re
from typing import Dict
from .messages import AgentResult, TraceStep
from .tools import Tool

class RulePlanner:
    """Deterministic planner for offline demos and CI."""
    def __init__(self, tools: Dict[str, Tool]):
        self.tools = tools

    def run(self, question: str) -> AgentResult:
        q = question.lower()
        trace = []
        if "ticket" in q and "sql" in self.tools:
            if "p0" in q and "open" in q:
                sql = "SELECT COUNT(*) AS open_p0 FROM tickets WHERE priority='P0' AND status='open'"
            elif "average" in q or "avg" in q:
                sql = "SELECT priority, AVG(age_hours) AS avg_age_hours FROM tickets GROUP BY priority"
            else:
                sql = "SELECT id, priority, status, summary FROM tickets LIMIT 5"
            obs = self.tools["sql"].run(query=sql)
            trace.append(TraceStep("sql", {"query": sql}, obs))
            return AgentResult(answer=f"Database result: {obs}", trace=trace, confidence=0.86)
        if any(term in q for term in ["policy", "rule", "escalation", "runbook", "sla"]):
            obs = self.tools["retriever"].run(query=question)
            trace.append(TraceStep("retriever", {"query": question}, obs))
            return AgentResult(answer=f"Based on the knowledge base: {obs}", trace=trace, confidence=0.78)
        if re.search(r"\d+\s*[+\-*/]\s*\d+", question):
            expr = re.findall(r"[0-9+\-*/(). ]+", question)[0].strip()
            obs = self.tools["calculator"].run(expression=expr)
            trace.append(TraceStep("calculator", {"expression": expr}, obs))
            return AgentResult(answer=f"The calculation result is {obs}.", trace=trace, confidence=0.99)
        obs = self.tools["retriever"].run(query=question)
        trace.append(TraceStep("retriever", {"query": question}, obs))
        return AgentResult(answer=f"I found this relevant information: {obs}", trace=trace, confidence=0.55)
