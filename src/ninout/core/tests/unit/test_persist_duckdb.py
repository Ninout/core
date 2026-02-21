from __future__ import annotations

import builtins
import sqlite3
import time

import pytest

import ninout.core.engine.dag as dag_module
from ninout import Dag
from ninout.core.ui.persist_duckdb import _rows_for_result, _table_name_for_step, persist_run_to_duckdb
from ninout.core.ui.persist_sqlite import SQLiteRunLogger


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
        return [{"value": "x"}, {"value": "y"}]

    @dag.step(depends_on=[a])
    def b(results):
        return {"count": len(results["a"])}

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


def test_dag_run_requires_duckdb_persistence() -> None:
    dag = Dag()

    @dag.step()
    def a():
        return {"id": 1}

    with pytest.raises(RuntimeError):
        dag.run(persist_duckdb=False)


def test_sqlite_logger_persists_runtime_and_rows(tmp_path) -> None:
    db_path = tmp_path / "runs.sqlite"
    logger = SQLiteRunLogger(
        db_path=str(db_path),
        run_name="run_x",
        dag_name="dag_x",
        steps={"a": dag_module.Step(name="a", func=lambda: {"id": 1}, deps=[])},
    )
    logger.log_step(
        "a",
        {
            "status": "done",
            "duration_ms": 12.0,
            "input_lines": 0,
            "output_lines": 1,
            "throughput_in_lps": 0.0,
            "throughput_out_lps": 83.3,
            "output": "ok",
            "result": {"id": 1},
        },
    )
    logger.close()

    con = sqlite3.connect(db_path)
    try:
        runtime = con.execute(
            "SELECT status, output_lines FROM step_runtime WHERE run_name = ? AND step_name = ?",
            ["run_x", "a"],
        ).fetchone()
        assert runtime == ("done", 1)
        row = con.execute(
            "SELECT row_id, payload_json FROM step_rows WHERE run_name = ? AND step_name = ?",
            ["run_x", "a"],
        ).fetchone()
        assert row is not None
        assert row[0] == 1
        assert '"id": 1' in row[1]
    finally:
        con.close()


def test_dag_run_persists_step_updates_to_sqlite_logger(monkeypatch, tmp_path) -> None:
    events: list[tuple[str, str]] = []
    closed = {"value": False}

    class FakeSQLiteLogger:
        def __init__(
            self,
            db_path: str,
            run_name: str,
            dag_name: str,
            steps,
            disabled_edges=None,
            disabled_steps=None,
        ) -> None:
            self.db_path = db_path
            self.run_name = run_name
            self.dag_name = dag_name
            self.steps = steps

        def log_step(self, step_name: str, meta) -> None:
            events.append((step_name, str(meta.get("status", ""))))

        def close(self) -> None:
            closed["value"] = True

    monkeypatch.setattr(dag_module, "SQLiteRunLogger", FakeSQLiteLogger)

    dag = Dag()

    @dag.step()
    def a():
        return {"id": 1}

    _results, status = dag.run(dag_name="sqlite_live", logs_dir=str(tmp_path))
    assert status["a"] == "done"
    assert ("a", "done") in events
    assert closed["value"] is True


def test_dag_run_creates_central_sqlite_automatically(tmp_path) -> None:
    dag = Dag()

    @dag.step()
    def a():
        return {"id": 1}

    _results, status = dag.run(dag_name="sqlite_auto", logs_dir=str(tmp_path))
    assert status["a"] == "done"
    sqlite_path = tmp_path / "runs.sqlite"
    assert sqlite_path.exists()


def test_sqlite_logger_handles_row_mode_running_updates_from_worker_thread(tmp_path) -> None:
    dag = Dag()

    @dag.step()
    def extract():
        return [{"id": 1}]

    @dag.step(depends_on=[extract], mode="row")
    def slow_row(row):
        time.sleep(0.25)
        return {"id": row["id"], "v": 10}

    _results, status = dag.run(dag_name="sqlite_threadsafe", logs_dir=str(tmp_path))
    assert status["extract"] == "done"
    assert status["slow_row"] == "done"
