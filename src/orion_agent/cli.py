from __future__ import annotations
import argparse, json
from .factory import build_planner

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("question")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    result = build_planner().run(args.question)
    if args.json:
        print(json.dumps({"answer": result.answer, "confidence": result.confidence, "trace": [s.__dict__ for s in result.trace]}, indent=2))
    else:
        print(result.answer)
        print("\nTrace:")
        for step in result.trace:
            print(f"- {step.tool}: {step.args} -> {step.observation[:160]}")

if __name__ == "__main__":
    main()
