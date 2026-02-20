from __future__ import annotations

from typing import Mapping

from ninout.core.engine.models import Step
from ninout.core.engine.validate import levels, topological_order


def layout_positions(
    steps: Mapping[str, Step],
) -> tuple[Mapping[str, tuple[int, int]], int, int]:
    order = topological_order(steps)
    level = levels(steps, order)
    grouped: dict[int, list[str]] = {}
    for name, lvl in level.items():
        grouped.setdefault(lvl, []).append(name)

    x_gap = 200
    y_gap = 120
    node_w = 140
    node_h = 48

    positions: dict[str, tuple[int, int]] = {}
    max_y = 0
    max_x = 0
    for lvl in sorted(grouped):
        nodes = grouped[lvl]
        for idx, name in enumerate(nodes):
            x = 40 + lvl * x_gap
            y = 40 + idx * y_gap
            positions[name] = (x, y)
            max_x = max(max_x, x + node_w + 40)
            max_y = max(max_y, y + node_h + 40)

    return positions, max_x, max_y
