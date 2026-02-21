from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from ninout.core.api.repository import get_run_details, get_run_graph, get_step_rows, list_runs
from ninout.core.api.schemas import RunDetails, RunGraph, RunSummary, StepRowsPage

router = APIRouter(prefix="/api", tags=["runs"])


@router.get("/runs", response_model=list[RunSummary])
def list_runs_endpoint() -> list[RunSummary]:
    return list_runs()


@router.get("/runs/{run_name}", response_model=RunDetails)
def run_details_endpoint(run_name: str) -> RunDetails:
    try:
        return get_run_details(run_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Run not found") from exc


@router.get("/runs/{run_name}/graph", response_model=RunGraph)
def run_graph_endpoint(run_name: str) -> RunGraph:
    try:
        return get_run_graph(run_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Run not found") from exc


@router.get("/runs/{run_name}/steps/{step_name}/rows", response_model=StepRowsPage)
def run_step_rows_endpoint(
    run_name: str,
    step_name: str,
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> StepRowsPage:
    try:
        return get_step_rows(run_name, step_name, limit=limit, offset=offset)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Run or step not found") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
