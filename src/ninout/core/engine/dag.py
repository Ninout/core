from __future__ import annotations

from typing import Callable, Iterable, MutableMapping
from datetime import datetime
import os

from ninout.core.engine.executor import run
from ninout.core.engine.models import Step
from ninout.core.ui.render import to_html_from_yaml
from ninout.core.ui.serialize import to_yaml
from ninout.core.engine.validate import validate_steps


class Dag:
    def __init__(self) -> None:
        self._steps: dict[str, Step] = {}
        self._disabled_edges: set[tuple[str, str]] = set()
        self._disabled_steps: set[str] = set()
        self._last_run: dict[str, dict[str, object]] | None = None

    @staticmethod
    def _ref_name(ref: Callable[..., object] | str) -> str:
        return ref.__name__ if callable(ref) else str(ref)

    def step(
        self,
        depends_on: Iterable[Callable[..., object] | str] | None = None,
        when: Callable[..., object] | str | None = None,
        condition: bool | None = None,
        is_branch: bool = False,
    ):
        def decorator(func: Callable[..., object]) -> Callable[..., object]:
            name = func.__name__
            deps = []
            for dep in depends_on or []:
                if callable(dep):
                    deps.append(dep.__name__)
                else:
                    deps.append(str(dep))
            when_name = None
            if when is not None:
                when_name = when.__name__ if callable(when) else str(when)
                if when_name not in deps:
                    deps.append(when_name)
                if condition is None:
                    cond_value = True
                else:
                    cond_value = condition
            else:
                cond_value = None
            self._steps[name] = Step(
                name=name,
                func=func,
                deps=deps,
                when=when_name,
                condition=cond_value,
                is_branch=is_branch,
            )
            return func

        return decorator

    def branch(self, depends_on: Iterable[Callable[..., object] | str] | None = None):
        return self.step(depends_on=depends_on, is_branch=True)

    def to_html(
        self,
        dag_name: str = "dag",
        logs_dir: str = "logs",
    ) -> tuple[str, str]:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = os.path.join(logs_dir, f"{dag_name}_{timestamp}")
        os.makedirs(run_dir, exist_ok=True)
        yaml_path = os.path.join(run_dir, "dag.yaml")
        html_path = os.path.join(run_dir, "dag.html")
        to_yaml(self._steps, path=yaml_path, run_data=self._last_run)
        to_html_from_yaml(yaml_path, html_path=html_path)
        return yaml_path, html_path

    def to_yaml(self, path: str = "dag.yaml") -> None:
        to_yaml(self._steps, path=path, run_data=self._last_run)

    def run(
        self,
        max_workers: int | None = None,
        raise_on_fail: bool = True,
        disabled_edges: Iterable[
            tuple[Callable[..., object] | str, Callable[..., object] | str]
        ]
        | None = None,
        disabled_steps: Iterable[Callable[..., object] | str] | None = None,
    ) -> tuple[MutableMapping[str, object], MutableMapping[str, str]]:
        all_disabled_edges = set(self._disabled_edges)
        for source, target in disabled_edges or []:
            source_name = self._ref_name(source)
            target_name = self._ref_name(target)
            all_disabled_edges.add((source_name, target_name))
        all_disabled_steps = set(self._disabled_steps)
        for step_ref in disabled_steps or []:
            all_disabled_steps.add(self._ref_name(step_ref))
        results, status, outputs, timings, input_lines_map, output_lines_map = run(
            self._steps,
            max_workers=max_workers,
            raise_on_fail=raise_on_fail,
            disabled_edges=all_disabled_edges,
            disabled_steps=all_disabled_steps,
        )
        self._last_run = {
            name: {
                "status": status[name],
                "output": outputs.get(name, ""),
                "duration_ms": round(timings.get(name, 0.0) * 1000.0, 3),
                "result": results.get(name),
                "input_lines": input_lines_map.get(name, 0),
                "output_lines": output_lines_map.get(name, 0),
                "disabled_deps": sorted(
                    source for source, target in all_disabled_edges if target == name
                ),
                "disabled_self": name in all_disabled_steps,
            }
            for name in status
        }
        return results, status

    def validate(self) -> None:
        validate_steps(self._steps)

    def __len__(self) -> int:
        return len(self._steps)

    def disable_edge(
        self,
        source: Callable[..., object] | str,
        target: Callable[..., object] | str,
    ) -> None:
        source_name = self._ref_name(source)
        target_name = self._ref_name(target)
        if source_name not in self._steps or target_name not in self._steps:
            raise ValueError(f"Steps desconhecidos para hop: {source_name} -> {target_name}")
        if source_name not in self._steps[target_name].deps:
            raise ValueError(f"Hop nao existe no DAG: {source_name} -> {target_name}")
        self._disabled_edges.add((source_name, target_name))

    def enable_edge(
        self,
        source: Callable[..., object] | str,
        target: Callable[..., object] | str,
    ) -> None:
        source_name = self._ref_name(source)
        target_name = self._ref_name(target)
        self._disabled_edges.discard((source_name, target_name))

    def list_disabled_edges(self) -> list[tuple[str, str]]:
        return sorted(self._disabled_edges)

    def disable_step(self, step: Callable[..., object] | str) -> None:
        step_name = self._ref_name(step)
        if step_name not in self._steps:
            raise ValueError(f"Step desconhecido para desabilitar: {step_name}")
        self._disabled_steps.add(step_name)

    def enable_step(self, step: Callable[..., object] | str) -> None:
        step_name = self._ref_name(step)
        self._disabled_steps.discard(step_name)

    def list_disabled_steps(self) -> list[str]:
        return sorted(self._disabled_steps)
