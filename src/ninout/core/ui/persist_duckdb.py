from __future__ import annotations

from datetime import datetime, timezone
import inspect
import json
import re
from typing import Mapping

from ninout.core.engine.models import Step


def _table_name_for_step(step_name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]", "_", step_name).strip("_").lower()
    if not normalized:
        normalized = "step"
    if normalized[0].isdigit():
        normalized = f"s_{normalized}"
    return f"step_{normalized}"


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


class DuckDBRunLogger:
    def __init__(
        self,
        db_path: str,
        dag_name: str,
        steps: Mapping[str, Step],
        disabled_edges: set[tuple[str, str]] | None = None,
        disabled_steps: set[str] | None = None,
    ) -> None:
        try:
            import duckdb  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "duckdb nao esta instalado. Instale com `uv add duckdb` para persistir resultados."
            ) from exc

        self._con = duckdb.connect(db_path)
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        disabled_edge_set = set(disabled_edges or set())
        disabled_step_set = set(disabled_steps or set())

        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS run_metadata (
                run_id VARCHAR,
                dag_name VARCHAR,
                created_at_utc TIMESTAMP,
                step_count INTEGER
            )
            """
        )
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS step_definition (
                run_id VARCHAR,
                step_name VARCHAR,
                table_name VARCHAR,
                deps_json VARCHAR,
                when_name VARCHAR,
                condition_bool BOOLEAN,
                is_branch BOOLEAN,
                code_text VARCHAR,
                disabled_deps_json VARCHAR,
                disabled_self BOOLEAN
            )
            """
        )
        self._con.execute(
            """
            CREATE TABLE IF NOT EXISTS step_runtime (
                run_id VARCHAR,
                step_name VARCHAR,
                status VARCHAR,
                duration_ms DOUBLE,
                input_lines INTEGER,
                output_lines INTEGER,
                throughput_in_lps DOUBLE,
                throughput_out_lps DOUBLE,
                output_text VARCHAR,
                result_kind VARCHAR,
                updated_at_utc TIMESTAMP
            )
            """
        )
        self._con.execute(
            "ALTER TABLE step_runtime ADD COLUMN IF NOT EXISTS throughput_in_lps DOUBLE"
        )
        self._con.execute(
            "ALTER TABLE step_runtime ADD COLUMN IF NOT EXISTS throughput_out_lps DOUBLE"
        )
        self._con.execute(
            "INSERT INTO run_metadata VALUES (?, ?, ?, ?)",
            [self.run_id, dag_name, created_at, len(steps)],
        )

        self.table_map: dict[str, str] = {}
        for step_name, step in steps.items():
            table_name = _table_name_for_step(step_name)
            self.table_map[step_name] = table_name
            self._con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    row_id BIGINT,
                    payload_json VARCHAR
                )
                """
            )
            disabled_deps = sorted(
                source for source, target in disabled_edge_set if target == step_name
            )
            self._con.execute(
                """
                INSERT INTO step_definition VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self.run_id,
                    step_name,
                    table_name,
                    _to_payload(step.deps),
                    step.when,
                    step.condition,
                    step.is_branch,
                    _safe_source(step.func),
                    _to_payload(disabled_deps),
                    step_name in disabled_step_set,
                ],
            )
            self._con.execute(
                """
                INSERT INTO step_runtime (
                    run_id, step_name, status, duration_ms, input_lines, output_lines,
                    throughput_in_lps, throughput_out_lps, output_text, result_kind, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
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

    def log_step(self, step_name: str, meta: Mapping[str, object]) -> None:
        table_name = self.table_map[step_name]
        result = meta.get("result")
        status_value = str(meta.get("status", ""))
        if status_value in {"done", "failed"} or result is not None:
            rows = _rows_for_result(result)
            self._con.execute(f"DELETE FROM {table_name}")
            if rows:
                self._con.executemany(
                    f"INSERT INTO {table_name} (row_id, payload_json) VALUES (?, ?)",
                    rows,
                )

        updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self._con.execute(
            "DELETE FROM step_runtime WHERE run_id = ? AND step_name = ?",
            [self.run_id, step_name],
        )
        self._con.execute(
            """
            INSERT INTO step_runtime (
                run_id, step_name, status, duration_ms, input_lines, output_lines,
                throughput_in_lps, throughput_out_lps, output_text, result_kind, updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
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

    def close(self) -> None:
        self._con.close()


def load_steps_from_duckdb(db_path: str) -> dict[str, Step]:
    try:
        import duckdb  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "duckdb nao esta instalado. Instale com `uv add duckdb` para carregar resultados."
        ) from exc

    con = duckdb.connect(db_path, read_only=True)
    try:
        run_id_row = con.execute(
            "SELECT run_id FROM run_metadata ORDER BY created_at_utc DESC LIMIT 1"
        ).fetchone()
        if not run_id_row:
            return {}
        run_id = run_id_row[0]
        rows = con.execute(
            """
            SELECT
                d.step_name,
                d.table_name,
                d.deps_json,
                d.when_name,
                d.condition_bool,
                d.is_branch,
                d.code_text,
                d.disabled_deps_json,
                d.disabled_self,
                r.status,
                r.duration_ms,
                r.input_lines,
                r.output_lines,
                r.throughput_in_lps,
                r.throughput_out_lps,
                r.output_text,
                r.result_kind
            FROM step_definition d
            JOIN step_runtime r
              ON d.run_id = r.run_id AND d.step_name = r.step_name
            WHERE d.run_id = ?
            """,
            [run_id],
        ).fetchall()

        steps: dict[str, Step] = {}
        for row in rows:
            (
                step_name,
                table_name,
                deps_json,
                when_name,
                condition_bool,
                is_branch,
                code_text,
                disabled_deps_json,
                disabled_self,
                status,
                duration_ms,
                input_lines,
                output_lines,
                throughput_in_lps,
                throughput_out_lps,
                output_text,
                result_kind,
            ) = row

            payload_rows = con.execute(
                f"SELECT payload_json FROM {table_name} ORDER BY row_id"
            ).fetchall()
            parsed_rows = [json.loads(item[0]) for item in payload_rows]
            if result_kind == "none":
                result_value = ""
            elif result_kind == "scalar":
                result_value = parsed_rows[0] if parsed_rows else ""
            else:
                result_value = parsed_rows

            def _noop() -> None:
                return None

            steps[step_name] = Step(
                name=step_name,
                func=_noop,
                deps=[str(dep) for dep in json.loads(deps_json or "[]")],
                when=when_name if isinstance(when_name, str) and when_name else None,
                condition=condition_bool if isinstance(condition_bool, bool) else None,
                is_branch=bool(is_branch),
                code=code_text if isinstance(code_text, str) else None,
                output=output_text if isinstance(output_text, str) else None,
                result=_to_payload(result_value) if result_value != "" else "",
                status=status if isinstance(status, str) else None,
                duration_ms=float(duration_ms)
                if isinstance(duration_ms, (int, float))
                else None,
                input_lines=int(input_lines) if isinstance(input_lines, int) else None,
                output_lines=int(output_lines) if isinstance(output_lines, int) else None,
                throughput_in_lps=float(throughput_in_lps)
                if isinstance(throughput_in_lps, (int, float))
                else None,
                throughput_out_lps=float(throughput_out_lps)
                if isinstance(throughput_out_lps, (int, float))
                else None,
                disabled_deps=[
                    str(dep) for dep in json.loads(disabled_deps_json or "[]")
                ],
                disabled_self=bool(disabled_self),
            )
        return steps
    finally:
        con.close()


def persist_run_to_duckdb(
    db_path: str,
    dag_name: str,
    steps: Mapping[str, Step],
    run_data: Mapping[str, Mapping[str, object]],
) -> dict[str, str]:
    logger = DuckDBRunLogger(db_path=db_path, dag_name=dag_name, steps=steps)
    try:
        for step_name, meta in run_data.items():
            logger.log_step(step_name, meta)
        return logger.table_map
    finally:
        logger.close()
