from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Protocol
import ast, operator, sqlite3, re
from pathlib import Path
from .guardrails import enforce_select_only, resolve_sandbox_path

class Tool(Protocol):
    name: str
    def run(self, **kwargs) -> str: ...

@dataclass
class CalculatorTool:
    name: str = "calculator"
    allowed = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul, ast.Div: operator.truediv, ast.Pow: operator.pow, ast.USub: operator.neg}

    def _eval(self, node):
        if isinstance(node, ast.Expression):
            return self._eval(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in self.allowed:
            return self.allowed[type(node.op)](self._eval(node.left), self._eval(node.right))
        if isinstance(node, ast.UnaryOp) and type(node.op) in self.allowed:
            return self.allowed[type(node.op)](self._eval(node.operand))
        raise ValueError("Unsupported expression")

    def run(self, expression: str) -> str:
        return str(self._eval(ast.parse(expression, mode="eval")))

@dataclass
class SQLTool:
    db_path: str
    name: str = "sql"
    def run(self, query: str) -> str:
        enforce_select_only(query)
        with sqlite3.connect(self.db_path) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(query).fetchall()
        return str([dict(r) for r in rows[:20]])

@dataclass
class FileTool:
    root: str
    name: str = "file_read"
    def run(self, path: str) -> str:
        p = resolve_sandbox_path(self.root, path)
        return p.read_text(encoding="utf-8")[:4000]

@dataclass
class RetrieverTool:
    docs: Dict[str, str]
    name: str = "retriever"

    @staticmethod
    def toks(text: str):
        return set(re.findall(r"[a-zA-Z0-9_]+", text.lower()))

    def run(self, query: str, k: int = 3) -> str:
        q = self.toks(query)
        scored = []
        for doc_id, text in self.docs.items():
            score = len(q & self.toks(text))
            scored.append((score, doc_id, text))
        top = [x for x in sorted(scored, reverse=True) if x[0] > 0][:k]
        return "\n\n".join(f"[{doc_id}] {text}" for _, doc_id, text in top) or "No relevant documents found."
