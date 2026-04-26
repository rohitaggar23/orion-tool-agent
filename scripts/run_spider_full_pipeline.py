#!/usr/bin/env python3
"""Run the Spider parquet through Orion's offline analysis pipeline.

The repository contains the Spider train parquet, but not the original Spider
SQLite database files. This script therefore evaluates the full dataset through
the parts of Orion that can be run honestly offline: task loading, SQL safety
guardrails, deterministic SQL analysis, trace creation, aggregate metrics, and
figures.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from orion_agent.guardrails import GuardrailError, enforce_select_only
from orion_agent.messages import TraceStep


ROOT = Path(__file__).resolve().parents[1]
DATASET = ROOT / "data" / "train-00000-of-00001_spider.parquet"
OUT = ROOT / "outputs" / "spider_full_pipeline"
FIGURES = OUT / "figures"
METRICS = OUT / "metrics"
TRACES = OUT / "traces"

COLORS = {
    "ink": "#172033",
    "muted": "#5F6C7B",
    "grid": "#D7DEE8",
    "paper": "#F7F9FC",
    "green": "#2A9D8F",
    "blue": "#457B9D",
    "gold": "#E9A03B",
    "red": "#D45D5D",
    "violet": "#7B61A8",
    "teal": "#00A6A6",
}

AGG_FUNCS = ("count", "sum", "avg", "min", "max")
CLAUSES = ("where", "join", "group by", "order by", "having", "limit")
SET_OPS = ("intersect", "union", "except")


@dataclass
class AnalysisResult:
    row_id: int
    db_id: str
    question: str
    query: str
    route: str
    guardrail_passed: bool
    guardrail_error: str | None
    execution_status: str
    sql_tokens: int
    question_tokens: int
    tables: list[str]
    table_count: int
    join_count: int
    condition_count: int
    subquery_count: int
    aggregation_count: int
    set_operation_count: int
    has_where: bool
    has_group_by: bool
    has_order_by: bool
    has_having: bool
    has_limit: bool
    complexity_score: int
    complexity_tier: str
    trace: list[dict[str, Any]]


def normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip())


def tokens(text: str) -> list[str]:
    return re.findall(r"[A-Za-z0-9_]+|[<>=!]+|[(),.*+-/]", text)


def extract_tables(sql: str) -> list[str]:
    normalized = normalize_sql(sql)
    candidates = re.findall(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)", normalized, flags=re.IGNORECASE)
    return sorted(set(candidates), key=str.lower)


def count_subqueries(sql: str) -> int:
    return len(re.findall(r"\(\s*select\b", sql, flags=re.IGNORECASE))


def count_conditions(sql: str) -> int:
    where_bonus = 1 if re.search(r"\bwhere\b", sql, flags=re.IGNORECASE) else 0
    boolean_ops = len(re.findall(r"\b(?:and|or)\b", sql, flags=re.IGNORECASE))
    comparisons = len(re.findall(r"(?:=|!=|<>|>=|<=|>|<|\blike\b|\bin\b|\bbetween\b)", sql, flags=re.IGNORECASE))
    return where_bonus + boolean_ops + comparisons


def complexity_tier(score: int) -> str:
    if score <= 2:
        return "Simple"
    if score <= 5:
        return "Moderate"
    if score <= 9:
        return "Complex"
    return "Advanced"


def analyze_row(row_id: int, row: pd.Series) -> AnalysisResult:
    question = str(row["question"])
    query = normalize_sql(str(row["query"]))
    sql_lower = query.lower()
    trace: list[TraceStep] = []

    route = "sql_analysis"
    trace.append(
        TraceStep(
            "dataset_loader",
            {"row_id": row_id, "db_id": row["db_id"]},
            f"Loaded Spider question with {len(tokens(question))} question tokens.",
        )
    )

    guardrail_passed = True
    guardrail_error = None
    try:
        enforce_select_only(query)
        trace.append(TraceStep("sql_guardrail", {"policy": "select_only"}, "passed"))
    except GuardrailError as exc:
        guardrail_passed = False
        guardrail_error = str(exc)
        trace.append(TraceStep("sql_guardrail", {"policy": "select_only"}, f"blocked: {exc}"))

    join_count = len(re.findall(r"\bjoin\b", sql_lower))
    subquery_count = count_subqueries(query)
    aggregation_count = sum(len(re.findall(rf"\b{func}\s*\(", sql_lower)) for func in AGG_FUNCS)
    set_operation_count = sum(len(re.findall(rf"\b{op}\b", sql_lower)) for op in SET_OPS)
    condition_count = count_conditions(query)
    has_where = bool(re.search(r"\bwhere\b", sql_lower))
    has_group_by = "group by" in sql_lower
    has_order_by = "order by" in sql_lower
    has_having = bool(re.search(r"\bhaving\b", sql_lower))
    has_limit = bool(re.search(r"\blimit\b", sql_lower))
    tables = extract_tables(query)
    sql_token_count = len(tokens(query))
    question_token_count = len(tokens(question))

    score = (
        join_count * 2
        + subquery_count * 3
        + set_operation_count * 3
        + aggregation_count
        + condition_count
        + int(has_group_by)
        + int(has_order_by)
        + int(has_having) * 2
        + int(has_limit)
        + max(0, len(tables) - 1)
    )
    tier = complexity_tier(score)

    trace.append(
        TraceStep(
            "sql_analyzer",
            {"route": route},
            json.dumps(
                {
                    "tables": tables,
                    "complexity_tier": tier,
                    "sql_tokens": sql_token_count,
                    "execution_status": "not_run_missing_spider_db_files",
                },
                sort_keys=True,
            ),
        )
    )

    return AnalysisResult(
        row_id=row_id,
        db_id=str(row["db_id"]),
        question=question,
        query=query,
        route=route,
        guardrail_passed=guardrail_passed,
        guardrail_error=guardrail_error,
        execution_status="not_run_missing_spider_db_files",
        sql_tokens=sql_token_count,
        question_tokens=question_token_count,
        tables=tables,
        table_count=len(tables),
        join_count=join_count,
        condition_count=condition_count,
        subquery_count=subquery_count,
        aggregation_count=aggregation_count,
        set_operation_count=set_operation_count,
        has_where=has_where,
        has_group_by=has_group_by,
        has_order_by=has_order_by,
        has_having=has_having,
        has_limit=has_limit,
        complexity_score=score,
        complexity_tier=tier,
        trace=[asdict(step) for step in trace],
    )


def summarize(results: pd.DataFrame) -> dict[str, Any]:
    clause_rates = {
        "WHERE": float(results["has_where"].mean()),
        "JOIN": float((results["join_count"] > 0).mean()),
        "GROUP BY": float(results["has_group_by"].mean()),
        "ORDER BY": float(results["has_order_by"].mean()),
        "HAVING": float(results["has_having"].mean()),
        "LIMIT": float(results["has_limit"].mean()),
        "SUBQUERY": float((results["subquery_count"] > 0).mean()),
        "SET OP": float((results["set_operation_count"] > 0).mean()),
        "AGGREGATION": float((results["aggregation_count"] > 0).mean()),
    }
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset": str(DATASET.relative_to(ROOT)),
        "total_tasks": int(len(results)),
        "unique_databases": int(results["db_id"].nunique()),
        "execution_accuracy": None,
        "execution_accuracy_note": "Not computed because the Spider SQLite database files are not included in this repository.",
        "guardrail_passed": int(results["guardrail_passed"].sum()),
        "guardrail_pass_rate": float(results["guardrail_passed"].mean()),
        "avg_question_tokens": float(results["question_tokens"].mean()),
        "avg_sql_tokens": float(results["sql_tokens"].mean()),
        "median_sql_tokens": float(results["sql_tokens"].median()),
        "avg_tables_per_query": float(results["table_count"].mean()),
        "avg_complexity_score": float(results["complexity_score"].mean()),
        "complexity_tier_counts": {k: int(v) for k, v in results["complexity_tier"].value_counts().to_dict().items()},
        "clause_rates": clause_rates,
        "top_databases": {k: int(v) for k, v in results["db_id"].value_counts().head(15).to_dict().items()},
    }


def title(fig: plt.Figure, main: str, sub: str) -> None:
    fig.text(0.05, 0.96, main, fontsize=20, fontweight="bold", color=COLORS["ink"])
    fig.text(0.05, 0.925, sub, fontsize=10.5, color=COLORS["muted"])


def style_axis(ax: plt.Axes) -> None:
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color(COLORS["grid"])
    ax.tick_params(colors=COLORS["muted"])
    ax.yaxis.label.set_color(COLORS["muted"])
    ax.xaxis.label.set_color(COLORS["muted"])
    ax.grid(axis="y", color=COLORS["grid"], linewidth=0.8, alpha=0.7)


def save(fig: plt.Figure, name: str) -> None:
    path = FIGURES / name
    fig.savefig(path, dpi=220, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"saved {path.relative_to(ROOT)}")


def plot_scorecard(metrics: dict[str, Any], results: pd.DataFrame) -> None:
    fig = plt.figure(figsize=(13, 7))
    title(
        fig,
        "Orion Spider Full-Pipeline Run",
        "7,000 text-to-SQL tasks loaded from Spider parquet; guardrails, SQL analysis, traces, and metrics generated.",
    )
    cards = [
        ("Tasks Processed", f"{metrics['total_tasks']:,}", "Full parquet dataset"),
        ("Databases", f"{metrics['unique_databases']:,}", "Distinct Spider domains"),
        ("Guardrail Pass", f"{metrics['guardrail_pass_rate']:.1%}", "SELECT-only safety"),
        ("Avg SQL Tokens", f"{metrics['avg_sql_tokens']:.1f}", "Gold query length"),
        ("Avg Tables", f"{metrics['avg_tables_per_query']:.2f}", "Tables referenced/query"),
        ("Avg Complexity", f"{metrics['avg_complexity_score']:.1f}", "Static SQL analyzer score"),
    ]
    card_y = [0.58, 0.34]
    for idx, (label, value, note) in enumerate(cards):
        row, col = divmod(idx, 3)
        ax = fig.add_axes([0.06 + col * 0.31, card_y[row], 0.26, 0.2])
        ax.set_axis_off()
        ax.add_patch(plt.Rectangle((0, 0), 1, 1, transform=ax.transAxes, facecolor=COLORS["paper"], edgecolor=COLORS["grid"], linewidth=1.2))
        ax.text(0.06, 0.66, value, fontsize=25, fontweight="bold", color=COLORS["ink"], transform=ax.transAxes)
        ax.text(0.06, 0.42, label, fontsize=12, fontweight="bold", color=COLORS["blue"], transform=ax.transAxes)
        ax.text(0.06, 0.18, note, fontsize=9.5, color=COLORS["muted"], transform=ax.transAxes)

    ax = fig.add_axes([0.08, 0.08, 0.84, 0.18])
    counts = results["complexity_tier"].value_counts().reindex(["Simple", "Moderate", "Complex", "Advanced"], fill_value=0)
    bars = ax.barh(counts.index, counts.values, color=[COLORS["green"], COLORS["blue"], COLORS["gold"], COLORS["red"]])
    ax.set_xlabel("Tasks")
    ax.set_title("Complexity coverage", loc="left", fontsize=13, pad=12, fontweight="bold")
    style_axis(ax)
    for bar in bars:
        value = int(bar.get_width())
        ax.text(value + 35, bar.get_y() + bar.get_height() / 2, f"{value:,}", va="center", color=COLORS["ink"], fontweight="bold")
    save(fig, "01_spider_pipeline_scorecard.png")


def plot_clause_coverage(metrics: dict[str, Any]) -> None:
    rates = pd.Series(metrics["clause_rates"]).sort_values()
    fig, ax = plt.subplots(figsize=(11, 6.5))
    title(fig, "SQL Feature Coverage", "Static analysis highlights what the Orion pipeline saw across all gold SQL queries.")
    bars = ax.barh(rates.index, rates.values, color=COLORS["teal"])
    ax.set_xlim(0, 1)
    ax.set_xlabel("Share of tasks")
    style_axis(ax)
    for bar in bars:
        value = bar.get_width()
        ax.text(value + 0.015, bar.get_y() + bar.get_height() / 2, f"{value:.1%}", va="center", color=COLORS["ink"], fontweight="bold")
    fig.subplots_adjust(top=0.82, left=0.12, right=0.94, bottom=0.12)
    save(fig, "02_sql_feature_coverage.png")


def plot_database_distribution(results: pd.DataFrame) -> None:
    top = results["db_id"].value_counts().head(18).sort_values()
    fig, ax = plt.subplots(figsize=(12, 7))
    title(fig, "Spider Domain Distribution", "Top database domains by task count in the full parquet run.")
    bars = ax.barh(top.index, top.values, color=COLORS["blue"])
    ax.set_xlabel("Tasks")
    style_axis(ax)
    for bar in bars:
        value = int(bar.get_width())
        ax.text(value + 2, bar.get_y() + bar.get_height() / 2, f"{value}", va="center", color=COLORS["ink"], fontweight="bold")
    fig.subplots_adjust(top=0.82, left=0.25, right=0.95, bottom=0.12)
    save(fig, "03_database_distribution.png")


def plot_complexity_distribution(results: pd.DataFrame) -> None:
    order = ["Simple", "Moderate", "Complex", "Advanced"]
    fig, ax = plt.subplots(figsize=(11, 6.5))
    title(fig, "SQL Complexity Mix", "Tiering combines joins, filters, subqueries, set operations, aggregations, and clause usage.")
    counts = results["complexity_tier"].value_counts().reindex(order, fill_value=0)
    colors = [COLORS["green"], COLORS["blue"], COLORS["gold"], COLORS["red"]]
    bars = ax.bar(counts.index, counts.values, color=colors)
    ax.set_ylabel("Tasks")
    style_axis(ax)
    for bar in bars:
        value = int(bar.get_height())
        ax.text(bar.get_x() + bar.get_width() / 2, value + 35, f"{value:,}\n{value / len(results):.1%}", ha="center", color=COLORS["ink"], fontweight="bold")
    fig.subplots_adjust(top=0.82, left=0.08, right=0.96, bottom=0.12)
    save(fig, "04_complexity_mix.png")


def plot_length_relationship(results: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(11, 7))
    title(fig, "Question Length vs SQL Length", "Longer natural-language questions tend to map to broader SQL programs, with visible outliers.")
    hb = ax.hexbin(results["question_tokens"], results["sql_tokens"], gridsize=35, cmap="YlGnBu", mincnt=1)
    ax.set_xlabel("Question tokens")
    ax.set_ylabel("SQL tokens")
    style_axis(ax)
    cb = fig.colorbar(hb, ax=ax)
    cb.set_label("Task count")
    fig.subplots_adjust(top=0.82, left=0.08, right=0.94, bottom=0.12)
    save(fig, "05_question_vs_sql_length.png")


def plot_tooling_outcomes(results: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(11, 6.5))
    title(fig, "Pipeline Tooling Outcomes", "Every row receives a loader trace, SQL guardrail decision, and SQL analyzer trace.")
    values = {
        "Loaded": len(results),
        "Guardrail passed": int(results["guardrail_passed"].sum()),
        "Analyzed": len(results),
        "Execution skipped": int((results["execution_status"] == "not_run_missing_spider_db_files").sum()),
    }
    colors = [COLORS["blue"], COLORS["green"], COLORS["teal"], COLORS["gold"]]
    bars = ax.bar(values.keys(), values.values(), color=colors)
    ax.set_ylabel("Tasks")
    ax.set_ylim(0, len(results) * 1.15)
    style_axis(ax)
    for bar in bars:
        value = int(bar.get_height())
        ax.text(bar.get_x() + bar.get_width() / 2, value + 90, f"{value:,}", ha="center", color=COLORS["ink"], fontweight="bold")
    fig.subplots_adjust(top=0.82, left=0.08, right=0.96, bottom=0.15)
    save(fig, "06_pipeline_tooling_outcomes.png")


def write_report(metrics: dict[str, Any]) -> None:
    report = OUT / "README.md"
    report.write_text(
        "\n".join(
            [
                "# Orion Tool Agent: Spider Full-Pipeline Outputs",
                "",
                f"Generated from `{DATASET.relative_to(ROOT)}`.",
                "",
                "## Run Scope",
                "",
                "The full 7,000-row Spider parquet was processed through an offline Orion pipeline: dataset loading, SELECT-only SQL guardrails, SQL feature analysis, trace emission, aggregate metrics, and figure generation.",
                "",
                "Execution accuracy is not reported because this repository does not include the original Spider SQLite database files needed to execute each gold query.",
                "",
                "## Key Metrics",
                "",
                f"- Tasks processed: **{metrics['total_tasks']:,}**",
                f"- Unique databases: **{metrics['unique_databases']:,}**",
                f"- SELECT-only guardrail pass rate: **{metrics['guardrail_pass_rate']:.1%}**",
                f"- Average SQL length: **{metrics['avg_sql_tokens']:.1f} tokens**",
                f"- Average complexity score: **{metrics['avg_complexity_score']:.1f}**",
                "",
                "## Figures",
                "",
                "- `figures/01_spider_pipeline_scorecard.png`: Full-run scorecard.",
                "- `figures/02_sql_feature_coverage.png`: Clause and SQL-feature coverage.",
                "- `figures/03_database_distribution.png`: Top Spider database domains by task count.",
                "- `figures/04_complexity_mix.png`: Simple/moderate/complex/advanced query mix.",
                "- `figures/05_question_vs_sql_length.png`: Natural-language length vs SQL program length.",
                "- `figures/06_pipeline_tooling_outcomes.png`: Loader, guardrail, analyzer, and execution-status counts.",
                "",
                "## Artifacts",
                "",
                "- `metrics/spider_full_pipeline_metrics.json`",
                "- `spider_full_pipeline_results.csv`",
                "- `traces/spider_full_pipeline_traces.jsonl`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    print(f"saved {report.relative_to(ROOT)}")


def main() -> None:
    for folder in (OUT, FIGURES, METRICS, TRACES):
        folder.mkdir(parents=True, exist_ok=True)

    sns.set_theme(style="whitegrid", font="DejaVu Sans")
    plt.rcParams.update({"figure.facecolor": "white", "axes.titleweight": "bold"})

    source = pd.read_parquet(DATASET)
    print(f"loaded {len(source):,} rows from {DATASET.relative_to(ROOT)}")

    records = [asdict(analyze_row(idx, row)) for idx, row in source.iterrows()]
    results = pd.DataFrame(records)
    metrics = summarize(results)

    metrics_path = METRICS / "spider_full_pipeline_metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    print(f"saved {metrics_path.relative_to(ROOT)}")

    traces_path = TRACES / "spider_full_pipeline_traces.jsonl"
    with traces_path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True) + "\n")
    print(f"saved {traces_path.relative_to(ROOT)}")

    csv_path = OUT / "spider_full_pipeline_results.csv"
    flat = results.drop(columns=["trace", "tables"]).copy()
    flat.to_csv(csv_path, index=False)
    print(f"saved {csv_path.relative_to(ROOT)}")

    plot_scorecard(metrics, results)
    plot_clause_coverage(metrics)
    plot_database_distribution(results)
    plot_complexity_distribution(results)
    plot_length_relationship(results)
    plot_tooling_outcomes(results)
    write_report(metrics)


if __name__ == "__main__":
    main()
