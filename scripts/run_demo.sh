#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHONPATH=src "${PYTHON:-python3}" -m orion_agent.cli "How many P0 tickets are open?"
PYTHONPATH=src "${PYTHON:-python3}" -m orion_agent.cli "What is the escalation rule for P0 incidents?"
