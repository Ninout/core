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
        self._last_run: dict[str, dict[str, object]] | None = None

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
    ) -> tuple[MutableMapping[str, object], MutableMapping[str, str]]:
        results, status, outputs, timings, input_lines_map, output_lines_map = run(
            self._steps, max_workers=max_workers, raise_on_fail=raise_on_fail
        )
        self._last_run = {
            name: {
                "status": status[name],
                "output": outputs.get(name, ""),
                "duration_ms": round(timings.get(name, 0.0) * 1000.0, 3),
                "result": results.get(name),
                "input_lines": input_lines_map.get(name, 0),
                "output_lines": output_lines_map.get(name, 0),
            }
            for name in status
        }
        return results, status

    def validate(self) -> None:
        validate_steps(self._steps)

    def __len__(self) -> int:
        return len(self._steps)
