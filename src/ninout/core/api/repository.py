from __future__ import annotations

import json
import os
import re

from ninout.core.api.schemas import (
    GraphEdge,
    GraphNode,
    RunDetails,
    RunGraph,
    RunSummary,
    StepRowsPage,
    StepSummary,
)


def _logs_dir() -> str:
    return os.environ.get("NINOUT_LOGS_DIR", "logs")


def _run_db_path(run_name: str) -> str:
    return os.path.join(_logs_dir(), run_name, "run.duckdb")


def _connect(db_path: str):
    import duckdb  # type: ignore[import-not-found]

    return duckdb.connect(db_path, read_only=True)


def _ensure_table_name(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", name):
        raise ValueError("Invalid table name")
    return name


def list_runs() -> list[RunSummary]:
    logs_dir = _logs_dir()
    if not os.path.isdir(logs_dir):
        return []
    items: list[RunSummary] = []
    for run_name in sorted(os.listdir(logs_dir), reverse=True):
        db_path = _run_db_path(run_name)
        if not os.path.isfile(db_path):
            continue
        con = _connect(db_path)
        try:
            row = con.execute(
                "SELECT run_id, dag_name, created_at_utc, step_count FROM run_metadata ORDER BY created_at_utc DESC LIMIT 1"
            ).fetchone()
            if not row:
                continue
            runtime_rows = con.execute(
                "SELECT status, count(*) FROM step_runtime WHERE run_id = ? GROUP BY status",
                [row[0]],
            ).fetchall()
            status_summary = {str(status): int(count) for status, count in runtime_rows}
            items.append(
                RunSummary(
                    run_name=run_name,
                    run_id=str(row[0]),
                    dag_name=str(row[1]),
                    created_at_utc=str(row[2]),
                    step_count=int(row[3]),
                    status_summary=status_summary,
                )
            )
        finally:
            con.close()
    return items


def get_run_details(run_name: str) -> RunDetails:
    db_path = _run_db_path(run_name)
    if not os.path.isfile(db_path):
        raise FileNotFoundError(run_name)
    con = _connect(db_path)
    try:
        run_row = con.execute(
            "SELECT run_id, dag_name, created_at_utc, step_count FROM run_metadata ORDER BY created_at_utc DESC LIMIT 1"
        ).fetchone()
        if not run_row:
            raise FileNotFoundError(run_name)
        run_id = str(run_row[0])
        rows = con.execute(
            """
            SELECT
                d.step_name,
                d.table_name,
                r.status,
                r.duration_ms,
                r.input_lines,
                r.output_lines,
                r.throughput_in_lps,
                r.throughput_out_lps,
                d.when_name,
                d.condition_bool,
                d.is_branch,
                d.disabled_self,
                d.disabled_deps_json,
                d.deps_json,
                r.output_text
            FROM step_definition d
            JOIN step_runtime r
              ON d.run_id = r.run_id AND d.step_name = r.step_name
            WHERE d.run_id = ?
            ORDER BY d.step_name
            """,
            [run_id],
        ).fetchall()
        steps: list[StepSummary] = []
        for row in rows:
            steps.append(
                StepSummary(
                    step_name=str(row[0]),
                    table_name=str(row[1]),
                    status=str(row[2]),
                    duration_ms=float(row[3]) if isinstance(row[3], (int, float)) else None,
                    input_lines=int(row[4]) if isinstance(row[4], int) else None,
                    output_lines=int(row[5]) if isinstance(row[5], int) else None,
                    throughput_in_lps=float(row[6])
                    if isinstance(row[6], (int, float))
                    else None,
                    throughput_out_lps=float(row[7])
                    if isinstance(row[7], (int, float))
                    else None,
                    when_name=str(row[8]) if isinstance(row[8], str) and row[8] else None,
                    condition_bool=row[9] if isinstance(row[9], bool) else None,
                    is_branch=bool(row[10]),
                    disabled_self=bool(row[11]),
                    disabled_deps=list(json.loads(row[12] or "[]")),
                    deps=list(json.loads(row[13] or "[]")),
                    output_text=str(row[14]) if row[14] is not None else "",
                )
            )
        return RunDetails(
            run_name=run_name,
            run_id=run_id,
            dag_name=str(run_row[1]),
            created_at_utc=str(run_row[2]),
            step_count=int(run_row[3]),
            steps=steps,
        )
    finally:
        con.close()


def get_step_rows(run_name: str, step_name: str, limit: int = 100, offset: int = 0) -> StepRowsPage:
    details = get_run_details(run_name)
    step = next((s for s in details.steps if s.step_name == step_name), None)
    if step is None:
        raise FileNotFoundError(step_name)

    db_path = _run_db_path(run_name)
    con = _connect(db_path)
    try:
        table_name = _ensure_table_name(step.table_name)
        total = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        rows = con.execute(
            f"SELECT row_id, payload_json FROM {table_name} ORDER BY row_id LIMIT ? OFFSET ?",
            [limit, offset],
        ).fetchall()
        payload = [{"row_id": int(r[0]), "payload": json.loads(r[1])} for r in rows]
        return StepRowsPage(
            run_name=run_name,
            step_name=step_name,
            total_rows=int(total),
            offset=offset,
            limit=limit,
            rows=payload,
        )
    finally:
        con.close()


def get_run_graph(run_name: str) -> RunGraph:
    details = get_run_details(run_name)
    nodes = [
        GraphNode(
            step_name=step.step_name,
            status=step.status,
            deps=step.deps,
            is_branch=step.is_branch,
            when_name=step.when_name,
            condition_bool=step.condition_bool,
            disabled_self=step.disabled_self,
            disabled_deps=step.disabled_deps,
        )
        for step in details.steps
    ]
    edges: list[GraphEdge] = []
    for step in details.steps:
        for dep in step.deps:
            is_conditional = step.when_name == dep
            edges.append(
                GraphEdge(
                    source=dep,
                    target=step.step_name,
                    is_conditional=is_conditional,
                    condition_bool=step.condition_bool if is_conditional else None,
                    disabled=dep in step.disabled_deps,
                )
            )
    return RunGraph(
        run_name=details.run_name,
        run_id=details.run_id,
        dag_name=details.dag_name,
        created_at_utc=details.created_at_utc,
        nodes=nodes,
        edges=edges,
    )
