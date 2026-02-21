from __future__ import annotations

from typing import Callable, Iterable, MutableMapping
from datetime import datetime
import os
import threading

from ninout.core.engine.executor import run
from ninout.core.engine.models import Step, StepMode, StepResult
from ninout.core.ui.persist_duckdb import DuckDBRunLogger
from ninout.core.ui.persist_sqlite import SQLiteRunLogger
from ninout.core.engine.validate import validate_steps


class Dag:
    def __init__(self) -> None:
        self._steps: dict[str, Step] = {}
        self._disabled_edges: set[tuple[str, str]] = set()
        self._disabled_steps: set[str] = set()
        self._last_run: dict[str, dict[str, object]] | None = None
        self._last_run_dir: str | None = None

    @staticmethod
    def _ref_name(ref: Callable[..., object] | str) -> str:
        return ref.__name__ if callable(ref) else str(ref)

    def step(
        self,
        depends_on: Iterable[Callable[..., object] | str] | None = None,
        when: Callable[..., StepResult] | str | None = None,
        condition: bool | None = None,
        is_branch: bool = False,
        mode: StepMode = "task",
    ):
        def decorator(func: Callable[..., StepResult]) -> Callable[..., StepResult]:
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
                mode=mode,
            )
            return func

        return decorator

    def branch(self, depends_on: Iterable[Callable[..., object] | str] | None = None):
        return self.step(depends_on=depends_on, is_branch=True)

    def to_html(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError(
            "HTML estatico foi removido. Use o dashboard dinamico via FastAPI em /dashboard."
        )

    def to_yaml(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError(
            "YAML estatico foi removido. Os dados de execucao ficam no run.duckdb."
        )

    def run(
        self,
        max_workers: int | None = None,
        raise_on_fail: bool = True,
        disabled_edges: Iterable[
            tuple[Callable[..., object] | str, Callable[..., object] | str]
        ]
        | None = None,
        disabled_steps: Iterable[Callable[..., object] | str] | None = None,
        dag_name: str = "dag",
        logs_dir: str = "logs",
        persist_duckdb: bool = True,
        duckdb_file_name: str = "run.duckdb",
    ) -> tuple[MutableMapping[str, object], MutableMapping[str, str]]:
        all_disabled_edges = set(self._disabled_edges)
        for source, target in disabled_edges or []:
            source_name = self._ref_name(source)
            target_name = self._ref_name(target)
            all_disabled_edges.add((source_name, target_name))
        all_disabled_steps = set(self._disabled_steps)
        for step_ref in disabled_steps or []:
            all_disabled_steps.add(self._ref_name(step_ref))
        if not persist_duckdb:
            raise RuntimeError(
                "persist_duckdb=False nao e suportado. DuckDB e obrigatorio neste runtime."
            )
        loggers: list[object] = []
        self._last_run_dir = None
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = os.path.join(logs_dir, f"{dag_name}_{timestamp}")
        run_name = os.path.basename(run_dir)
        os.makedirs(run_dir, exist_ok=True)
        self._last_run_dir = run_dir
        db_path = os.path.join(run_dir, duckdb_file_name)
        duckdb_logger = DuckDBRunLogger(
            db_path=db_path,
            dag_name=dag_name,
            steps=self._steps,
            disabled_edges=all_disabled_edges,
            disabled_steps=all_disabled_steps,
        )
        loggers.append(duckdb_logger)
        sqlite_path = os.path.join(logs_dir, "runs.sqlite")
        sqlite_logger = SQLiteRunLogger(
            db_path=sqlite_path,
            run_name=run_name,
            dag_name=dag_name,
            steps=self._steps,
            disabled_edges=all_disabled_edges,
            disabled_steps=all_disabled_steps,
        )
        loggers.append(sqlite_logger)
        logger_lock = threading.Lock()

        def _on_step_update(
            step_name: str,
            step_status: str,
            step_result: object | None,
            step_output: str,
            duration_s: float,
            input_lines: int,
            output_lines: int,
        ) -> None:
            throughput_in_lps = 0.0 if duration_s <= 0 else input_lines / duration_s
            throughput_out_lps = 0.0 if duration_s <= 0 else output_lines / duration_s
            meta = {
                "status": step_status,
                "output": step_output,
                "duration_ms": round(duration_s * 1000.0, 3),
                "result": step_result,
                "input_lines": input_lines,
                "output_lines": output_lines,
                "throughput_in_lps": round(throughput_in_lps, 3),
                "throughput_out_lps": round(throughput_out_lps, 3),
                "disabled_deps": sorted(
                    source
                    for source, target in all_disabled_edges
                    if target == step_name
                ),
                "disabled_self": step_name in all_disabled_steps,
            }
            with logger_lock:
                for logger in loggers:
                    logger.log_step(step_name, meta)
        try:
            results, status, outputs, timings, input_lines_map, output_lines_map = run(
                self._steps,
                max_workers=max_workers,
                raise_on_fail=raise_on_fail,
                disabled_edges=all_disabled_edges,
                disabled_steps=all_disabled_steps,
                on_step_update=_on_step_update,
            )
            self._last_run = {
                name: {
                    "status": status[name],
                    "output": outputs.get(name, ""),
                    "duration_ms": round(timings.get(name, 0.0) * 1000.0, 3),
                    "result": results.get(name),
                    "input_lines": input_lines_map.get(name, 0),
                    "output_lines": output_lines_map.get(name, 0),
                    "throughput_in_lps": round(
                        (
                            input_lines_map.get(name, 0)
                            / timings.get(name, 0.0)
                        )
                        if timings.get(name, 0.0) > 0
                        else 0.0,
                        3,
                    ),
                    "throughput_out_lps": round(
                        (
                            output_lines_map.get(name, 0)
                            / timings.get(name, 0.0)
                        )
                        if timings.get(name, 0.0) > 0
                        else 0.0,
                        3,
                    ),
                    "disabled_deps": sorted(
                        source for source, target in all_disabled_edges if target == name
                    ),
                    "disabled_self": name in all_disabled_steps,
                }
                for name in status
            }
            return results, status
        finally:
            for logger in loggers:
                logger.close()

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
