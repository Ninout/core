# Execution and Logs

## Running a DAG

```bash
uv run transformations/example_dag.py
```

## Output generated per run

Each run creates a timestamped folder under `logs/`:

```text
logs/<dag_name>_<YYYYMMDD_HHMMSS>/
```

Files:
- `run.duckdb`: required execution artifact.

## DuckDB schema

Core tables:
- `run_metadata`: run identity and creation timestamp.
- `step_definition`: static graph metadata (deps, branch config, code, disabled info).
- `step_runtime`: dynamic status/metrics updates.
- `step_<name>`: payload rows for each step.

## Runtime behavior

- Executor updates `step_runtime` incrementally while processing.
- `row` mode emits running updates during row consumption.
- final state (`done`/`failed`/`skipped`) overwrites latest runtime row for the step.
- logs are queryable immediately by API/dashboard.

## Error behavior

- failed step: `status=failed`.
- dependent steps: usually `status=skipped`.
- with `raise_on_fail=True`, the run raises `RuntimeError`.
