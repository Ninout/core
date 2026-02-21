# Overview

## What ninout is

`ninout` is a Python framework to model and execute DAG pipelines.

Each decorated function becomes a step, with:
- explicit dependencies,
- conditional branch support (`when` + `condition`),
- execution modes (`task`, `row`, `sql`),
- parallel scheduling when dependencies allow,
- runtime observability persisted in DuckDB.

## Practical goal

Provide a lightweight pipeline orchestrator that keeps:
- simple authoring in code,
- graph validation before execution,
- per-step traceability and metrics during execution,
- dynamic run inspection via API and dashboard.

## Usage flow

1. Create `Dag()`.
2. Declare steps with `@dag.step(...)` and `@dag.branch(...)`.
3. Execute with `dag.run(...)`.
4. Inspect logs in DuckDB using the FastAPI dashboard.

## Core concepts

- `step`: unit of work.
- `deps`: upstream steps required before run.
- `branch`: step that must return `bool`.
- `when` + `condition`: conditional path control.
- `results`: dictionary with completed outputs (for task mode when function takes one argument).
- `run.duckdb`: single source of truth for execution metadata and payload rows.
