from __future__ import annotations

import builtins

import pytest

import ninout.core.engine.dag as dag_module
from ninout import Dag
from ninout.core.ui.persist_duckdb import (
    _rows_for_result,
    _table_name_for_step,
    persist_run_to_duckdb,
)


def test_table_name_for_step_sanitizes_values() -> None:
    assert _table_name_for_step("My-Step") == "step_my_step"
    assert _table_name_for_step("123") == "step_s_123"
    assert _table_name_for_step("___") == "step_step"


def test_rows_for_result_handles_none_list_and_scalar() -> None:
    assert _rows_for_result(None) == []
    assert _rows_for_result([{"id": 1}, {"id": 2}])[0][0] == 1
    assert _rows_for_result("ok") == [(1, '"ok"')]


def test_persist_run_to_duckdb_raises_when_duckdb_is_missing(monkeypatch, tmp_path) -> None:
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "duckdb":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(RuntimeError, match="duckdb nao esta instalado"):
        persist_run_to_duckdb(
            db_path=str(tmp_path / "run.duckdb"),
            dag_name="x",
            steps={},
            run_data={},
        )


def test_dag_to_html_calls_duckdb_persistence_when_enabled(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def fake_persist(db_path: str, dag_name: str, steps, run_data):
        captured["db_path"] = db_path
        captured["dag_name"] = dag_name
        captured["steps"] = steps
        captured["run_data"] = run_data
        return {}

    monkeypatch.setattr(dag_module, "persist_run_to_duckdb", fake_persist)

    dag = Dag()

    @dag.step()
    def only():
        return {"id": 1}

    dag.run()
    _yaml_path, _html_path = dag.to_html(
        dag_name="duck",
        logs_dir=str(tmp_path),
        persist_duckdb=True,
    )

    assert captured["dag_name"] == "duck"
    assert str(captured["db_path"]).endswith("run.duckdb")
    assert "only" in captured["steps"]
    assert "only" in captured["run_data"]


def test_dag_run_persists_step_updates_to_duckdb_logger(monkeypatch, tmp_path) -> None:
    events: list[tuple[str, str]] = []
    closed = {"value": False}

    class FakeLogger:
        def __init__(
            self,
            db_path: str,
            dag_name: str,
            steps,
            disabled_edges=None,
            disabled_steps=None,
        ) -> None:
            self.db_path = db_path
            self.dag_name = dag_name
            self.steps = steps

        def log_step(self, step_name: str, meta) -> None:
            events.append((step_name, str(meta.get("status", ""))))

        def close(self) -> None:
            closed["value"] = True

    monkeypatch.setattr(dag_module, "DuckDBRunLogger", FakeLogger)

    dag = Dag()

    @dag.step()
    def a():
        return ["x", "y"]

    @dag.step(depends_on=[a])
    def b(results):
        return len(results["a"])

    _results, status = dag.run(
        dag_name="live",
        logs_dir=str(tmp_path),
        persist_duckdb=True,
        duckdb_file_name="live.duckdb",
    )
    assert status["a"] == "done"
    assert status["b"] == "done"
    assert ("a", "done") in events
    assert ("b", "done") in events
    assert closed["value"] is True
    assert dag._last_run_dir is not None
