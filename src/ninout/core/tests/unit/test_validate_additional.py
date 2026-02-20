from __future__ import annotations

import pytest

from ninout.core.engine.models import Step
from ninout.core.engine.validate import levels, topological_order, validate_steps


def test_validate_condition_without_when_raises() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[], condition=True),
    }
    with pytest.raises(ValueError):
        validate_steps(steps)


def test_validate_when_without_condition_raises() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"], when="a", condition=None),
    }
    with pytest.raises(ValueError):
        validate_steps(steps)


def test_validate_invalid_condition_type_raises() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"], when="a", condition="true"),  # type: ignore[arg-type]
    }
    with pytest.raises(ValueError):
        validate_steps(steps)


def test_levels_are_computed_from_dependencies() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
        "c": Step(name="c", func=lambda: None, deps=["b"]),
        "d": Step(name="d", func=lambda: None, deps=["a"]),
    }
    order = topological_order(steps)
    lvls = levels(steps, order)
    assert lvls["a"] == 0
    assert lvls["b"] == 1
    assert lvls["c"] == 2
    assert lvls["d"] == 1


def test_topological_order_cycle_raises() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=["b"]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
    }
    with pytest.raises(ValueError):
        topological_order(steps)
