from __future__ import annotations

import io
from queue import Queue
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Callable, Iterable, Mapping, MutableMapping

from ninout.core.engine.models import Step
from ninout.core.engine.planner import compile_execution_plan


def run(
    steps: Mapping[str, Step],
    max_workers: int | None = None,
    raise_on_fail: bool = True,
    disabled_edges: set[tuple[str, str]] | None = None,
    disabled_steps: set[str] | None = None,
    on_step_update: (
        Callable[[str, str, object | None, str, float, int, int], None] | None
    ) = None,
) -> tuple[MutableMapping[str, object], MutableMapping[str, str], MutableMapping[str, str]]:
    progress_emit_interval_s = 0.2
    plan = compile_execution_plan(
        steps,
        disabled_edges=set(disabled_edges or set()),
        disabled_steps=set(disabled_steps or set()),
    )
    disabled = set(plan.disabled_edges)
    disabled_nodes = set(plan.disabled_steps)
    order = plan.order
    pending = set(order)
    running: dict[str, object] = {}
    results: MutableMapping[str, object] = {}
    status: MutableMapping[str, str] = {name: "pending" for name in order}
    timings: MutableMapping[str, float] = {}

    def _set_status(
        name: str,
        new_status: str,
        allowed_from: set[str],
    ) -> None:
        current = status[name]
        if current not in allowed_from:
            raise RuntimeError(
                f"Transicao de status invalida para {name}: {current} -> {new_status}"
            )
        status[name] = new_status

    def is_ready(name: str) -> bool:
        step = steps[name]
        return all(status[dep] == "done" for dep in step.deps)

    def should_skip(name: str) -> bool:
        if name in disabled_nodes:
            return True
        step = steps[name]
        for dep in step.deps:
            if (dep, name) in disabled:
                return True
            if status[dep] in {"failed", "skipped"}:
                return True
        return False

    def branch_matches(name: str) -> bool:
        step = steps[name]
        if step.when is None:
            return True
        cond_value = results[step.when]
        if not isinstance(cond_value, bool):
            raise ValueError(f"Branch {step.when} deve retornar bool, recebeu {cond_value}")
        return cond_value is step.condition

    stdout = sys.stdout
    stderr = sys.stderr
    thread_local = threading.local()

    class _ThreadLocalIO:
        def write(self, text: str) -> int:
            buffer = getattr(thread_local, "buffer", None)
            if buffer is None:
                return stdout.write(text)
            return buffer.write(text)

        def flush(self) -> None:
            buffer = getattr(thread_local, "buffer", None)
            if buffer is None:
                stdout.flush()
            else:
                buffer.flush()

    def _count_lines(value: object) -> int:
        if value is None:
            return 0
        if isinstance(value, list):
            return len(value)
        if isinstance(value, dict):
            return len(value)
        if isinstance(value, str):
            return max(1, value.count("\n") + 1) if value else 0
        return 1

    def _is_step_payload(value: object) -> bool:
        if isinstance(value, dict):
            return all(isinstance(k, str) for k in value)
        if isinstance(value, list):
            return all(isinstance(item, dict) for item in value)
        return False

    def _normalize_payload(value: object, step_name: str) -> object:
        if _is_step_payload(value):
            return value
        if isinstance(value, tuple):
            mapped = [{"value": item} for item in value]
            return mapped
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict, list)):
            collected = list(value)
            if all(isinstance(item, dict) for item in collected):
                return collected
        raise TypeError(
            f"Step {step_name} deve retornar dict ou list[dict], recebeu {type(value).__name__}"
        )

    duckdb_connection = None
    if any(step.mode == "sql" for step in steps.values()):
        try:
            import duckdb  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "duckdb nao esta instalado. Instale com `uv add duckdb` para executar steps mode='sql'."
            ) from exc
        duckdb_connection = duckdb.connect(":memory:")

    def _run_step(step: Step) -> tuple[bool, object, str, float, int, int]:
        buffer = io.StringIO()
        thread_local.buffer = buffer
        start = time.perf_counter()
        input_lines = 0
        for dep in step.deps:
            if dep in results:
                input_lines += _count_lines(results[dep])
        try:
            if step.mode == "row":
                input_rows: list[dict[str, object]] = []
                for dep in step.deps:
                    dep_value = results.get(dep)
                    if isinstance(dep_value, dict):
                        input_rows.append(dep_value)
                    elif isinstance(dep_value, list):
                        input_rows.extend(dep_value)

                eof = object()
                in_q: Queue[object] = Queue(maxsize=1024)
                out_q: Queue[object] = Queue(maxsize=1024)
                worker_error: dict[str, Exception] = {}

                def _producer() -> None:
                    for row in input_rows:
                        in_q.put(row)
                    in_q.put(eof)

                def _worker() -> None:
                    while True:
                        item = in_q.get()
                        if item is eof:
                            out_q.put(eof)
                            return
                        row = item
                        try:
                            try:
                                row_result = step.func(row)
                            except TypeError:
                                row_result = step.func([row])
                            if row_result is None:
                                continue
                            if isinstance(row_result, dict):
                                out_q.put(row_result)
                            elif isinstance(row_result, list):
                                for r in row_result:
                                    out_q.put(r)
                            else:
                                raise TypeError(
                                    f"Step {step.name} (mode=row) deve retornar dict, list[dict] ou None por linha."
                                )
                        except Exception as exc:  # noqa: BLE001
                            worker_error["error"] = exc
                            out_q.put(eof)
                            return

                producer_thread = threading.Thread(target=_producer)
                worker_thread = threading.Thread(target=_worker)
                producer_thread.start()
                worker_thread.start()

                collected_rows: list[dict[str, object]] = []
                last_emit = time.perf_counter()
                while True:
                    out_item = out_q.get()
                    if out_item is eof:
                        break
                    collected_rows.append(out_item)
                    now = time.perf_counter()
                    if (
                        on_step_update is not None
                        and now - last_emit >= progress_emit_interval_s
                    ):
                        elapsed = now - start
                        on_step_update(
                            step.name,
                            "running",
                            None,
                            buffer.getvalue(),
                            elapsed,
                            input_lines,
                            len(collected_rows),
                        )
                        last_emit = now

                producer_thread.join()
                worker_thread.join()
                if "error" in worker_error:
                    raise worker_error["error"]
                result = collected_rows
            elif step.mode == "sql":
                try:
                    query = step.func(results)
                except TypeError:
                    query = step.func()
                if not isinstance(query, str):
                    raise TypeError(
                        f"Step {step.name} com mode='sql' deve retornar query SQL (str)."
                    )
                if duckdb_connection is None:
                    raise RuntimeError("Conexao DuckDB indisponivel para mode='sql'.")
                sql_result = duckdb_connection.execute(query).fetchall()
                columns = [desc[0] for desc in duckdb_connection.description]
                result = [
                    {str(col): row[idx] for idx, col in enumerate(columns)}
                    for row in sql_result
                ]
            else:
                try:
                    result = step.func(results)
                except TypeError:
                    result = step.func()
            if step.is_branch:
                if not isinstance(result, bool):
                    raise ValueError(f"Branch {step.name} deve retornar bool, recebeu {result}")
            else:
                result = _normalize_payload(result, step.name)
            output_lines = _count_lines(result)
            return (
                True,
                result,
                buffer.getvalue(),
                time.perf_counter() - start,
                input_lines,
                output_lines,
            )
        except Exception as exc:  # noqa: BLE001
            return (
                False,
                exc,
                buffer.getvalue(),
                time.perf_counter() - start,
                input_lines,
                0,
            )
        finally:
            thread_local.buffer = None

    outputs: MutableMapping[str, str] = {}
    input_lines_map: MutableMapping[str, int] = {}
    output_lines_map: MutableMapping[str, int] = {}

    try:
        sys.stdout = _ThreadLocalIO()
        sys.stderr = sys.stdout
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while pending or running:
                progressed = False
                for name in list(pending):
                    if should_skip(name):
                        _set_status(name, "skipped", {"pending"})
                        outputs[name] = ""
                        timings[name] = 0.0
                        input_lines_map[name] = 0
                        output_lines_map[name] = 0
                        if on_step_update is not None:
                            on_step_update(name, "skipped", None, "", 0.0, 0, 0)
                        pending.remove(name)
                        progressed = True
                        continue
                    if not is_ready(name):
                        continue
                    if not branch_matches(name):
                        _set_status(name, "skipped", {"pending"})
                        outputs[name] = ""
                        timings[name] = 0.0
                        input_lines_map[name] = 0
                        output_lines_map[name] = 0
                        if on_step_update is not None:
                            on_step_update(name, "skipped", None, "", 0.0, 0, 0)
                        pending.remove(name)
                        progressed = True
                        continue
                    step = steps[name]
                    future = executor.submit(_run_step, step)
                    running[name] = future
                    _set_status(name, "running", {"pending"})
                    pending.remove(name)
                    progressed = True

                if running:
                    done_futures, _ = wait(running.values(), return_when=FIRST_COMPLETED)
                    for future in done_futures:
                        finished = None
                        for name, f in list(running.items()):
                            if f is future:
                                finished = name
                                break
                        if finished is None:
                            continue
                        ok, payload, output, duration, input_lines, output_lines = (
                            future.result()
                        )
                        if output_lines == 0 and output:
                            output_lines = _count_lines(output)
                        outputs[finished] = output
                        timings[finished] = duration
                        input_lines_map[finished] = input_lines
                        output_lines_map[finished] = output_lines
                        if ok:
                            results[finished] = payload
                            _set_status(finished, "done", {"running"})
                            if on_step_update is not None:
                                on_step_update(
                                    finished,
                                    "done",
                                    payload,
                                    output,
                                    duration,
                                    input_lines,
                                    output_lines,
                                )
                        else:
                            results[finished] = payload
                            _set_status(finished, "failed", {"running"})
                            if on_step_update is not None:
                                on_step_update(
                                    finished,
                                    "failed",
                                    payload,
                                    output,
                                    duration,
                                    input_lines,
                                    output_lines,
                                )
                        del running[finished]
                    progressed = True

                if not progressed and not running and pending:
                    raise RuntimeError("Deadlock ao executar o DAG")
    finally:
        if duckdb_connection is not None:
            duckdb_connection.close()
        sys.stdout = stdout
        sys.stderr = stderr

    failed = [name for name, st in status.items() if st == "failed"]
    if failed and raise_on_fail:
        raise RuntimeError(f"Steps com falha: {', '.join(failed)}")
    return results, status, outputs, timings, input_lines_map, output_lines_map
