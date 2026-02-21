from __future__ import annotations

from concurrent.futures import Future
import threading
import sys

import pytest

import ninout.core.engine.executor as executor_module
from ninout.core.engine.executor import run
from ninout.core.engine.models import Step


def test_executor_skips_false_branch_path() -> None:
    steps = {}

    def start():
        return {"value": "seed"}

    def decision():
        return False

    def on_true(results):
        return results["start"]

    def on_false():
        return {"value": "fallback"}

    steps["start"] = Step(name="start", func=start, deps=[])
    steps["decision"] = Step(name="decision", func=decision, deps=["start"], is_branch=True)
    steps["on_true"] = Step(
        name="on_true",
        func=on_true,
        deps=["start", "decision"],
        when="decision",
        condition=True,
    )
    steps["on_false"] = Step(
        name="on_false",
        func=on_false,
        deps=["start", "decision"],
        when="decision",
        condition=False,
    )

    results, status, _outputs, _timings, _in_lines, _out_lines = run(steps)
    assert status["on_true"] == "skipped"
    assert status["on_false"] == "done"
    assert results["on_false"] == {"value": "fallback"}


def test_executor_returns_failed_when_raise_on_fail_false() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {"value": 1}, deps=[]),
        "boom": Step(name="boom", func=lambda: 1 / 0, deps=["a"]),
    }
    _results, status, _outputs, _timings, _in_lines, _out_lines = run(
        steps, raise_on_fail=False
    )
    assert status["a"] == "done"
    assert status["boom"] == "failed"


def test_executor_raises_when_raise_on_fail_true() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {"value": 1}, deps=[]),
        "boom": Step(name="boom", func=lambda: 1 / 0, deps=["a"]),
    }
    with pytest.raises(RuntimeError):
        run(steps, raise_on_fail=True)


def test_executor_branch_must_return_bool() -> None:
    steps = {}

    def start():
        return {"value": "x"}

    def bad_branch():
        return "yes"

    def conditional():
        return {"value": "nope"}

    steps["start"] = Step(name="start", func=start, deps=[])
    steps["bad_branch"] = Step(
        name="bad_branch", func=bad_branch, deps=["start"], is_branch=True
    )
    steps["conditional"] = Step(
        name="conditional",
        func=conditional,
        deps=["start", "bad_branch"],
        when="bad_branch",
        condition=True,
    )

    with pytest.raises(RuntimeError):
        run(steps)


def test_executor_supports_steps_without_results_arg_and_counts_stdout_lines() -> None:
    steps = {}

    def no_args_step():
        print("line-1")
        print("line-2")
        return {}

    steps["no_args"] = Step(name="no_args", func=no_args_step, deps=[])
    _results, status, outputs, _timings, in_lines, out_lines = run(steps)
    assert status["no_args"] == "done"
    assert in_lines["no_args"] == 0
    assert out_lines["no_args"] == 3
    assert "line-1" in outputs["no_args"]


def test_executor_skips_dependents_when_parent_fails() -> None:
    steps = {
        "fail": Step(name="fail", func=lambda: (_ for _ in ()).throw(ValueError("x")), deps=[]),
        "downstream": Step(name="downstream", func=lambda: {"value": 1}, deps=["fail"]),
    }
    _results, status, _outputs, _timings, _in_lines, _out_lines = run(
        steps, raise_on_fail=False
    )
    assert status["fail"] == "failed"
    assert status["downstream"] == "skipped"


def test_executor_threadlocal_io_none_and_buffer_flush_paths() -> None:
    steps = {}

    def io_step():
        def in_other_thread():
            print("outside-buffer")
            sys.stdout.flush()

        worker = threading.Thread(target=in_other_thread)
        worker.start()
        worker.join()

        print("inside-buffer")
        sys.stdout.flush()
        return {"value": "ok"}

    steps["io"] = Step(name="io", func=io_step, deps=[])
    _results, status, outputs, _timings, _in_lines, _out_lines = run(steps)
    assert status["io"] == "done"
    assert "inside-buffer" in outputs["io"]


def test_executor_handles_done_future_not_in_running_map(monkeypatch) -> None:
    original_wait = executor_module.wait

    def patched_wait(futures, return_when):
        done, not_done = original_wait(futures, return_when=return_when)
        ghost = Future()
        ghost.set_result((True, None, "", 0.0, 0, 0))
        return done | {ghost}, not_done

    monkeypatch.setattr(executor_module, "wait", patched_wait)
    steps = {"a": Step(name="a", func=lambda: {"value": "ok"}, deps=[])}
    results, status, _outputs, _timings, _in_lines, _out_lines = run(steps)
    assert status["a"] == "done"
    assert results["a"] == {"value": "ok"}


def test_executor_deadlock_path_raises_runtime_error(monkeypatch) -> None:
    monkeypatch.setattr(executor_module, "validate_steps", lambda _steps: None)
    monkeypatch.setattr(executor_module, "topological_order", lambda _steps: ["a"])

    steps = {"a": Step(name="a", func=lambda: {"value": "ok"}, deps=["a"])}
    with pytest.raises(RuntimeError, match="Deadlock"):
        run(steps)


def test_executor_skips_target_when_hop_is_disabled() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {"value": "a"}, deps=[]),
        "b": Step(name="b", func=lambda: {"value": "b"}, deps=["a"]),
        "c": Step(name="c", func=lambda: {"value": "c"}, deps=["b"]),
    }
    _results, status, _outputs, _timings, _in_lines, _out_lines = run(
        steps, raise_on_fail=False, disabled_edges={("a", "b")}
    )
    assert status["a"] == "done"
    assert status["b"] == "skipped"
    assert status["c"] == "skipped"


def test_executor_rejects_invalid_disabled_hop() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {"value": "a"}, deps=[]),
        "b": Step(name="b", func=lambda: {"value": "b"}, deps=["a"]),
    }
    with pytest.raises(ValueError):
        run(steps, disabled_edges={("a", "missing")})


def test_executor_skips_step_when_disabled_steps_is_set() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {"value": "a"}, deps=[]),
        "b": Step(name="b", func=lambda: {"value": "b"}, deps=["a"]),
        "c": Step(name="c", func=lambda: {"value": "c"}, deps=["b"]),
    }
    _results, status, _outputs, _timings, _in_lines, _out_lines = run(
        steps, raise_on_fail=False, disabled_steps={"b"}
    )
    assert status["a"] == "done"
    assert status["b"] == "skipped"
    assert status["c"] == "skipped"


def test_executor_rejects_invalid_disabled_step() -> None:
    steps = {
        "a": Step(name="a", func=lambda: {"value": "a"}, deps=[]),
    }
    with pytest.raises(ValueError):
        run(steps, disabled_steps={"missing"})


def test_executor_rejects_non_standard_step_output_type() -> None:
    steps = {
        "a": Step(name="a", func=lambda: "not-allowed", deps=[]),
    }
    with pytest.raises(RuntimeError):
        run(steps, raise_on_fail=True)
