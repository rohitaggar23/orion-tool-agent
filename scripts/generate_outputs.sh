#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHONPATH=src "${PYTHON:-python3}" -m orion_agent.cli "How many P0 tickets are open?" --json > outputs/traces/demo_task_01.json
PYTHONPATH=src "${PYTHON:-python3}" -m orion_agent.cli "What is the escalation rule for P0 incidents?" --json > outputs/traces/demo_task_02.json
PYTHONPATH=src "${PYTHON:-python3}" scripts/generate_outputs.py > outputs/logs/generate_outputs.log 2>&1
cat outputs/reports/EXPERIMENT_REPORT.md
