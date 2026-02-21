# Architecture

## Folder structure

- `src/ninout/core/engine/`: DAG API, planner, executor, validation, models.
- `src/ninout/core/ui/`: DuckDB persistence helpers and dashboard assets.
- `src/ninout/core/api/`: FastAPI endpoints and DuckDB repository.
- `transformations/`: runnable examples.
- `logs/`: run outputs (`run.duckdb` per execution).
- `src/ninout/core/tests/`: unit/integration tests.

## Main modules

### `src/ninout/core/engine/dag.py`

`Dag` authoring API:
- register steps and branches,
- maintain disabled edges/steps,
- run execution,
- persist `_last_run` metadata and run directory.

### `src/ninout/core/engine/planner.py`

Compiles runtime plan from graph:
- execution order,
- upstream/downstream maps,
- disabled edge/step effects.

### `src/ninout/core/engine/executor.py`

Executes plan with thread pool scheduler:
- transitions: `pending -> running -> done/failed/skipped`,
- dependency/branch gating,
- mode-aware execution (`task`, `row`, `sql`),
- incremental metrics callbacks.

### `src/ninout/core/ui/persist_duckdb.py`

DuckDB persistence layer:
- `run_metadata`,
- `step_definition`,
- `step_runtime`,
- one payload table per step (`step_<name>`).

### `src/ninout/core/api/*`

API and data access:
- list runs,
- run detail with steps and metrics,
- paginated rows for a step.

### `src/ninout/core/ui/dashboard/*`

Single-page frontend:
- lists available runs,
- shows per-run graph/runtime information,
- queries data through FastAPI endpoints.

## Execution cycle

1. `Dag.run()` validates and builds execution plan.
2. Executor schedules ready steps and updates status/metrics.
3. Step updates are persisted incrementally to DuckDB.
4. Dashboard/API read only from DuckDB.
