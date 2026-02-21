from __future__ import annotations

import pytest

from ninout.core.engine.models import Step
from ninout.core.engine.planner import compile_execution_plan


def test_compile_execution_plan_returns_order_and_disabled_sets() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {}, deps=[]),
        "b": Step(name="b", func=lambda: {}, deps=["a"]),
    }
    plan = compile_execution_plan(
        steps,
        disabled_edges={("a", "b")},
        disabled_steps={"b"},
    )
    assert plan.order[0] == "a"
    assert plan.disabled_edges == {("a", "b")}
    assert plan.disabled_steps == {"b"}


def test_compile_execution_plan_rejects_invalid_disabled_entries() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {}, deps=[]),
        "b": Step(name="b", func=lambda: {}, deps=["a"]),
    }
    with pytest.raises(ValueError):
        compile_execution_plan(steps, disabled_edges={("a", "missing")})
    with pytest.raises(ValueError):
        compile_execution_plan(steps, disabled_steps={"missing"})
