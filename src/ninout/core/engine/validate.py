from __future__ import annotations

from typing import Mapping

from ninout.core.engine.models import Step


def validate_steps(steps: Mapping[str, Step]) -> None:
    for step in steps.values():
        if step.mode not in {"task", "row", "sql"}:
            raise ValueError(f"Step {step.name} tem mode invalido: {step.mode}")
        for dep in step.deps:
            if dep not in steps:
                raise ValueError(f"Dependencia desconhecida: {step.name} -> {dep}")
        if step.condition is not None and step.when is None:
            raise ValueError(
                f"Step {step.name} tem condition, mas nenhum when foi definido"
            )
        if step.when is not None and step.condition is None:
            raise ValueError(
                f"Step {step.name} tem when, mas condition nao foi definida"
            )
        if step.condition is not None and not isinstance(step.condition, bool):
            raise ValueError(f"Step {step.name} tem condition invalida: {step.condition}")

    temp: set[str] = set()
    perm: set[str] = set()

    def visit(node: str) -> None:
        if node in perm:
            return
        if node in temp:
            raise ValueError(f"Ciclo detectado envolvendo: {node}")
        temp.add(node)
        for dep in steps[node].deps:
            visit(dep)
        temp.remove(node)
        perm.add(node)

    for name in steps:
        visit(name)


def topological_order(steps: Mapping[str, Step]) -> list[str]:
    indegree: dict[str, int] = {name: 0 for name in steps}
    for step in steps.values():
        for _dep in step.deps:
            indegree[step.name] += 1

    queue = [name for name, deg in indegree.items() if deg == 0]
    order: list[str] = []

    while queue:
        node = queue.pop(0)
        order.append(node)
        for step in steps.values():
            if node in step.deps:
                indegree[step.name] -= 1
                if indegree[step.name] == 0:
                    queue.append(step.name)

    if len(order) != len(steps):
        raise ValueError("Ciclo detectado no grafo")

    return order


def levels(steps: Mapping[str, Step], order: list[str]) -> dict[str, int]:
    level: dict[str, int] = {name: 0 for name in steps}
    for node in order:
        deps = steps[node].deps
        if deps:
            level[node] = max(level[d] + 1 for d in deps)
    return level
