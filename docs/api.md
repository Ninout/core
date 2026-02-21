# API

## Main import

```python
from ninout import Dag
```

## `Dag` class

### `Dag.step(...)`

Registers a step.

Parameters:
- `depends_on`: iterable of functions or step names.
- `when`: branch function/name used as gate.
- `condition`: expected branch value (`True`/`False`).
- `mode`: `"task"`, `"row"`, or `"sql"`.
- `is_branch`: internal use; prefer `dag.branch(...)`.

Rules:
- if `when` is provided and `condition` is omitted, `condition=True`.
- branch steps must return `bool`.
- non-branch outputs are normalized for logging/runtime as dict/list-of-dict style payloads.

### `Dag.branch(depends_on=None)`

Shortcut to create branch steps (`is_branch=True`).

### `Dag.run(...)`

Executes the DAG and persists runtime data in DuckDB.

Main parameters:
- `max_workers`
- `raise_on_fail`
- `disabled_edges`
- `disabled_steps`
- `dag_name`
- `logs_dir`
- `persist_duckdb` (must be `True`)
- `duckdb_file_name` (default `run.duckdb`)

Returns:
- `results`: map of step results.
- `status`: map of step statuses.

Status values:
- `pending`, `running`, `done`, `failed`, `skipped`.

### `Dag.to_html(...)` / `Dag.to_yaml(...)`

Deprecated and removed from runtime behavior.
Both methods raise `RuntimeError`.
Use dashboard/API backed by DuckDB.

### `Dag.validate()`

Runs graph validation without executing.

### `Dag.disable_edge(source, target)` / `Dag.enable_edge(source, target)`

Disable or re-enable a directed dependency edge.

### `Dag.disable_step(step)` / `Dag.enable_step(step)`

Disable or re-enable a full step.

### `Dag.list_disabled_edges()` / `Dag.list_disabled_steps()`

Return currently disabled execution elements.
