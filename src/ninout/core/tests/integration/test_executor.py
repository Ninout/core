from __future__ import annotations

from ninout.core.engine.executor import run
from ninout.core.engine.models import Step


def test_executor_results_and_metrics() -> None:
    steps = {}

    def step_a():
        return ["a", "b", "c"]

    def step_b(results):
        return [item.upper() for item in results["a"]]

    steps["a"] = Step(name="a", func=step_a, deps=[])
    steps["b"] = Step(name="b", func=step_b, deps=["a"])

    results, status, outputs, timings, input_lines, output_lines = run(steps)
    assert status["a"] == "done"
    assert status["b"] == "done"
    assert results["b"] == ["A", "B", "C"]
    assert input_lines["b"] == 3
    assert output_lines["b"] == 3
