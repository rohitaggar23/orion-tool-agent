# Orion Tool Agent: Spider Full-Pipeline Outputs

Generated from `data/train-00000-of-00001_spider.parquet`.

## Run Scope

The full 7,000-row Spider parquet was processed through the offline Orion pipeline: dataset loading, SELECT-only SQL guardrails, SQL feature analysis, trace emission, aggregate metrics, and figure generation.

Execution accuracy is not reported because this repository does not include the original Spider SQLite database files required to execute each gold query.

## Key Metrics

- Tasks processed: **7,000**
- Unique databases: **140**
- SELECT-only guardrail pass rate: **100.0%**
- Average SQL length: **21.9 tokens**
- Average complexity score: **5.3**

## Figures

- `figures/01_spider_pipeline_scorecard.png`: Full-run scorecard.
- `figures/02_sql_feature_coverage.png`: Clause and SQL-feature coverage.
- `figures/03_database_distribution.png`: Top database domains by task count.
- `figures/04_complexity_mix.png`: Simple, moderate, complex, and advanced query mix.
- `figures/05_question_vs_sql_length.png`: Natural-language length vs SQL program length.
- `figures/06_pipeline_tooling_outcomes.png`: Loader, guardrail, analyzer, and execution-status counts.

## Artifacts

- `metrics/spider_full_pipeline_metrics.json`
- `spider_full_pipeline_results.csv`
- `traces/spider_full_pipeline_traces.jsonl`
