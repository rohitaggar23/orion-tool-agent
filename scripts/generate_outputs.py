from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime

from orion_agent.factory import build_planner
from orion_agent.eval import TaskEvaluator

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'data' / 'tasks.jsonl'
OUT = ROOT / 'outputs'


def main() -> None:
    planner = build_planner()

    evaluator = TaskEvaluator(planner)
    metrics = evaluator.run_jsonl(str(DATA))

    traces = []
    with open(DATA, 'r', encoding='utf-8') as f:
        for idx, line in enumerate(f):
            if not line.strip():
                continue
            row = json.loads(line)
            res = planner.run(row['question'])
            payload = {
                'question': row['question'],
                'answer': res.answer,
                'confidence': res.confidence,
                'trace': [s.__dict__ for s in res.trace],
                'expected_terms': row.get('expected_terms', []),
            }
            traces.append(payload)
            (OUT / 'traces' / f'task_{idx+1:02d}.json').write_text(json.dumps(payload, indent=2), encoding='utf-8')

    metrics_payload = {
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        **metrics,
    }
    (OUT / 'metrics' / 'eval_metrics.json').write_text(json.dumps(metrics_payload, indent=2), encoding='utf-8')

    md = []
    md.append('# Orion Tool Agent — Demo Report')
    md.append('')
    md.append('This report is included as evidence that the agent runs end-to-end, produces traces, and can be evaluated offline.')
    md.append('')
    md.append('## Summary')
    md.append(f"- Total tasks: **{metrics['total']}**")
    md.append(f"- Passed: **{metrics['passed']}**")
    md.append(f"- Success rate: **{metrics['success_rate']*100:.1f}%**")
    md.append('')
    md.append('## Tasks')
    for i, t in enumerate(traces, start=1):
        md.append(f"### Task {i}")
        md.append(f"**Question:** {t['question']}")
        md.append('')
        md.append(f"**Answer:** {t['answer']}")
        md.append('')
        md.append('**Trace:**')
        for step in t['trace']:
            obs = (step.get('observation') or '')
            obs_short = obs[:200].replace('\n', ' ')
            md.append(f"- `{step.get('tool')}` {step.get('args')} → {obs_short}")
        md.append('')

    (OUT / 'reports' / 'EXPERIMENT_REPORT.md').write_text('\n'.join(md) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
