from __future__ import annotations

import pytest

from ninout.core.engine.models import Step
from ninout.core.engine.validate import topological_order, validate_steps


def test_validate_unknown_dep() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=["missing"]),
    }
    with pytest.raises(ValueError):
        validate_steps(steps)


def test_validate_cycle() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=["b"]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
    }
    with pytest.raises(ValueError):
        validate_steps(steps)


def test_topological_order() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
        "c": Step(name="c", func=lambda: None, deps=["a"]),
    }
    order = topological_order(steps)
    assert order.index("a") < order.index("b")
    assert order.index("a") < order.index("c")
