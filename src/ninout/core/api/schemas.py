from __future__ import annotations

from pydantic import BaseModel


class RunSummary(BaseModel):
    run_name: str
    run_id: str
    dag_name: str
    created_at_utc: str
    step_count: int
    status_summary: dict[str, int]


class StepSummary(BaseModel):
    step_name: str
    table_name: str
    status: str
    duration_ms: float | None
    input_lines: int | None
    output_lines: int | None
    throughput_in_lps: float | None
    throughput_out_lps: float | None
    when_name: str | None
    condition_bool: bool | None
    is_branch: bool
    disabled_self: bool
    disabled_deps: list[str]
    deps: list[str]
    output_text: str


class RunDetails(BaseModel):
    run_name: str
    run_id: str
    dag_name: str
    created_at_utc: str
    step_count: int
    steps: list[StepSummary]


class StepRowsPage(BaseModel):
    run_name: str
    step_name: str
    total_rows: int
    offset: int
    limit: int
    rows: list[dict[str, object]]


class GraphNode(BaseModel):
    step_name: str
    status: str
    deps: list[str]
    is_branch: bool
    when_name: str | None
    condition_bool: bool | None
    disabled_self: bool
    disabled_deps: list[str]


class GraphEdge(BaseModel):
    source: str
    target: str
    is_conditional: bool
    condition_bool: bool | None
    disabled: bool


class RunGraph(BaseModel):
    run_name: str
    run_id: str
    dag_name: str
    created_at_utc: str
    nodes: list[GraphNode]
    edges: list[GraphEdge]
