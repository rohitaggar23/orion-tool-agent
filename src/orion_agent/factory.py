from __future__ import annotations
import csv, os, sqlite3
from pathlib import Path
from .tools import CalculatorTool, SQLTool, RetrieverTool, FileTool
from .planner import RulePlanner

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"

def ensure_demo_db() -> str:
    db_path = DATA_DIR / "tickets.sqlite"
    DATA_DIR.mkdir(exist_ok=True)
    with sqlite3.connect(db_path) as con:
        con.execute("CREATE TABLE IF NOT EXISTS tickets (id TEXT, priority TEXT, status TEXT, age_hours REAL, summary TEXT)")
        count = con.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]
        if count == 0:
            con.executemany("INSERT INTO tickets VALUES (?, ?, ?, ?, ?)", [
                ("T-100", "P0", "open", 3.5, "checkout outage"),
                ("T-101", "P1", "open", 12.0, "PDF upload failing"),
                ("T-102", "P0", "closed", 8.0, "database failover"),
                ("T-103", "P2", "open", 48.0, "settings page typo"),
            ])
    return str(db_path)

def load_docs():
    docs_path = DATA_DIR / "company_knowledge.md"
    text = docs_path.read_text(encoding="utf-8")
    return {"company_knowledge.md": text}

def build_planner() -> RulePlanner:
    db = ensure_demo_db()
    tools = {
        "calculator": CalculatorTool(),
        "sql": SQLTool(db),
        "retriever": RetrieverTool(load_docs()),
        "file_read": FileTool(str(DATA_DIR)),
    }
    return RulePlanner(tools)
