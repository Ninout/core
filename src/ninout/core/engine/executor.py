from __future__ import annotations

import io
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Mapping, MutableMapping

from ninout.core.engine.models import Step
from ninout.core.engine.validate import topological_order, validate_steps


def run(
    steps: Mapping[str, Step],
    max_workers: int | None = None,
    raise_on_fail: bool = True,
) -> tuple[MutableMapping[str, object], MutableMapping[str, str], MutableMapping[str, str]]:
    validate_steps(steps)
    order = topological_order(steps)
    pending = set(order)
    running: dict[str, object] = {}
    results: MutableMapping[str, object] = {}
    status: MutableMapping[str, str] = {name: "pending" for name in order}
    timings: MutableMapping[str, float] = {}

    def can_run(name: str) -> bool:
        step = steps[name]
        for dep in step.deps:
            if status[dep] == "failed":
                return False
            if status[dep] == "skipped":
                return False
            if status[dep] != "done":
                return False
        if step.when is not None:
            cond_value = results[step.when]
            if not isinstance(cond_value, bool):
                raise ValueError(
                    f"Branch {step.when} deve retornar bool, recebeu {cond_value}"
                )
            return cond_value is step.condition
        return True

    def should_skip(name: str) -> bool:
        step = steps[name]
        for dep in step.deps:
            if status[dep] in {"failed", "skipped"}:
                return True
        if step.when is not None and status[step.when] == "done":
            cond_value = results[step.when]
            if not isinstance(cond_value, bool):
                raise ValueError(
                    f"Branch {step.when} deve retornar bool, recebeu {cond_value}"
                )
            if cond_value is not step.condition:
                return True
        return False

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

    def _run_step(step: Step) -> tuple[bool, object, str, float, int, int]:
        buffer = io.StringIO()
        thread_local.buffer = buffer
        start = time.perf_counter()
        input_lines = 0
        for dep in step.deps:
            if dep in results:
                input_lines += _count_lines(results[dep])
        try:
            try:
                result = step.func(results)
            except TypeError:
                result = step.func()
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
                        status[name] = "skipped"
                        outputs[name] = ""
                        timings[name] = 0.0
                        input_lines_map[name] = 0
                        output_lines_map[name] = 0
                        pending.remove(name)
                        progressed = True
                        continue
                    if can_run(name):
                        step = steps[name]
                        future = executor.submit(_run_step, step)
                        running[name] = future
                        status[name] = "running"
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
                            status[finished] = "done"
                        else:
                            results[finished] = payload
                            status[finished] = "failed"
                        del running[finished]
                    progressed = True

                if not progressed and not running and pending:
                    raise RuntimeError("Deadlock ao executar o DAG")
    finally:
        sys.stdout = stdout
        sys.stderr = stderr

    failed = [name for name, st in status.items() if st == "failed"]
    if failed and raise_on_fail:
        raise RuntimeError(f"Steps com falha: {', '.join(failed)}")
    return results, status, outputs, timings, input_lines_map, output_lines_map
