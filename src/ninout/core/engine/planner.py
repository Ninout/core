from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from ninout.core.engine.models import Step
from ninout.core.engine.validate import topological_order, validate_steps


@dataclass(frozen=True)
class ExecutionPlan:
    order: list[str]
    disabled_edges: set[tuple[str, str]]
    disabled_steps: set[str]


def compile_execution_plan(
    steps: Mapping[str, Step],
    disabled_edges: set[tuple[str, str]] | None = None,
    disabled_steps: set[str] | None = None,
) -> ExecutionPlan:
    validate_steps(steps)
    disabled_edge_set = set(disabled_edges or set())
    disabled_step_set = set(disabled_steps or set())

    for step_name in disabled_step_set:
        if step_name not in steps:
            raise ValueError(f"Step desconhecido desabilitado: {step_name}")

    for source, target in disabled_edge_set:
        if source not in steps or target not in steps:
            raise ValueError(f"Hop desconhecido desabilitado: {source} -> {target}")
        if source not in steps[target].deps:
            raise ValueError(f"Hop nao existe no DAG: {source} -> {target}")

    order = topological_order(steps)
    return ExecutionPlan(
        order=order,
        disabled_edges=disabled_edge_set,
        disabled_steps=disabled_step_set,
    )
