from __future__ import annotations

from ninout.core.ui.layout import layout_positions
from ninout.core.engine.models import Step


def test_layout_positions() -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
        "c": Step(name="c", func=lambda: None, deps=["a"]),
    }
    positions, width, height = layout_positions(steps)
    assert set(positions.keys()) == {"a", "b", "c"}
    assert width > 0
    assert height > 0
