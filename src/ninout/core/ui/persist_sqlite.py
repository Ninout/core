from __future__ import annotations

from datetime import datetime, timezone
import inspect
import json
import os
import sqlite3
import threading
from typing import Mapping

from ninout.core.engine.models import Step


def _to_payload(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def _safe_source(func: object) -> str:
    try:
        return inspect.getsource(func).rstrip()
    except OSError:
        return "# Codigo fonte indisponivel"


def _rows_for_result(value: object) -> list[tuple[int, str]]:
    if value is None:
        return []
    if isinstance(value, list):
        return [(idx, _to_payload(item)) for idx, item in enumerate(value, start=1)]
    return [(1, _to_payload(value))]


def _result_kind(value: object) -> str:
    if value is None:
        return "none"
    if isinstance(value, list):
        return "list"
    return "scalar"


class SQLiteRunLogger:
    def __init__(
        self,
        db_path: str,
        run_name: str,
        dag_name: str,
        steps: Mapping[str, Step],
        disabled_edges: set[tuple[str, str]] | None = None,
        disabled_steps: set[str] | None = None,
    ) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self._con = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._con.execute("PRAGMA journal_mode=WAL")
            self._con.execute("PRAGMA synchronous=NORMAL")
        self.run_name = run_name
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        created_at = datetime.now(timezone.utc).isoformat()
        disabled_edge_set = set(disabled_edges or set())
        disabled_step_set = set(disabled_steps or set())

        with self._lock:
            self._con.execute(
                """
                CREATE TABLE IF NOT EXISTS run_metadata (
                    run_name TEXT,
                    run_id TEXT,
                    dag_name TEXT,
                    created_at_utc TEXT,
                    step_count INTEGER,
                    PRIMARY KEY (run_name, run_id)
                )
                """
            )
            self._con.execute(
                """
                CREATE TABLE IF NOT EXISTS step_definition (
                    run_name TEXT,
                    run_id TEXT,
                    step_name TEXT,
                    deps_json TEXT,
                    when_name TEXT,
                    condition_bool INTEGER,
                    is_branch INTEGER,
                    code_text TEXT,
                    disabled_deps_json TEXT,
                    disabled_self INTEGER,
                    PRIMARY KEY (run_name, run_id, step_name)
                )
                """
            )
            self._con.execute(
                """
                CREATE TABLE IF NOT EXISTS step_runtime (
                    run_name TEXT,
                    run_id TEXT,
                    step_name TEXT,
                    status TEXT,
                    duration_ms REAL,
                    input_lines INTEGER,
                    output_lines INTEGER,
                    throughput_in_lps REAL,
                    throughput_out_lps REAL,
                    output_text TEXT,
                    result_kind TEXT,
                    updated_at_utc TEXT,
                    PRIMARY KEY (run_name, run_id, step_name)
                )
                """
            )
            self._con.execute(
                """
                CREATE TABLE IF NOT EXISTS step_rows (
                    run_name TEXT,
                    run_id TEXT,
                    step_name TEXT,
                    row_id INTEGER,
                    payload_json TEXT,
                    PRIMARY KEY (run_name, run_id, step_name, row_id)
                )
                """
            )
            self._con.execute(
                """
                INSERT OR REPLACE INTO run_metadata (
                    run_name, run_id, dag_name, created_at_utc, step_count
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [self.run_name, self.run_id, dag_name, created_at, len(steps)],
            )

            for step_name, step in steps.items():
                disabled_deps = sorted(
                    source for source, target in disabled_edge_set if target == step_name
                )
                self._con.execute(
                    """
                    INSERT OR REPLACE INTO step_definition (
                        run_name, run_id, step_name, deps_json, when_name,
                        condition_bool, is_branch, code_text, disabled_deps_json, disabled_self
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        self.run_name,
                        self.run_id,
                        step_name,
                        _to_payload(step.deps),
                        step.when,
                        step.condition,
                        int(step.is_branch),
                        _safe_source(step.func),
                        _to_payload(disabled_deps),
                        int(step_name in disabled_step_set),
                    ],
                )
                self._con.execute(
                    """
                    INSERT OR REPLACE INTO step_runtime (
                        run_name, run_id, step_name, status, duration_ms,
                        input_lines, output_lines, throughput_in_lps, throughput_out_lps,
                        output_text, result_kind, updated_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        self.run_name,
                        self.run_id,
                        step_name,
                        "pending",
                        None,
                        None,
                        None,
                        None,
                        None,
                        "",
                        "none",
                        created_at,
                    ],
                )
            self._con.commit()

    def log_step(self, step_name: str, meta: Mapping[str, object]) -> None:
        with self._lock:
            result = meta.get("result")
            status_value = str(meta.get("status", ""))
            if status_value in {"done", "failed"} or result is not None:
                rows = _rows_for_result(result)
                self._con.execute(
                    "DELETE FROM step_rows WHERE run_name = ? AND run_id = ? AND step_name = ?",
                    [self.run_name, self.run_id, step_name],
                )
                if rows:
                    self._con.executemany(
                        """
                        INSERT OR REPLACE INTO step_rows (
                            run_name, run_id, step_name, row_id, payload_json
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        [
                            (self.run_name, self.run_id, step_name, row_id, payload_json)
                            for row_id, payload_json in rows
                        ],
                    )

            updated_at = datetime.now(timezone.utc).isoformat()
            self._con.execute(
                """
                INSERT OR REPLACE INTO step_runtime (
                    run_name, run_id, step_name, status, duration_ms,
                    input_lines, output_lines, throughput_in_lps, throughput_out_lps,
                    output_text, result_kind, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self.run_name,
                    self.run_id,
                    step_name,
                    status_value,
                    meta.get("duration_ms")
                    if isinstance(meta.get("duration_ms"), (int, float))
                    else None,
                    meta.get("input_lines")
                    if isinstance(meta.get("input_lines"), int)
                    else None,
                    meta.get("output_lines")
                    if isinstance(meta.get("output_lines"), int)
                    else None,
                    meta.get("throughput_in_lps")
                    if isinstance(meta.get("throughput_in_lps"), (int, float))
                    else None,
                    meta.get("throughput_out_lps")
                    if isinstance(meta.get("throughput_out_lps"), (int, float))
                    else None,
                    str(meta.get("output", "")),
                    _result_kind(result),
                    updated_at,
                ],
            )
            self._con.commit()

    def close(self) -> None:
        with self._lock:
            self._con.close()
