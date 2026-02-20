# API

## Main import

```python
from ninout import Dag
```

## `Dag` class

### `Dag.step(...)`

Decorator used to register a step.

Parameters:
- `depends_on`: iterable of functions or step names.
- `when`: branch name/function used as a condition.
- `condition`: `True` or `False` to indicate which path should run.
- `is_branch`: internal usage; prefer `dag.branch(...)`.

Default behavior:
- if `when` is provided and `condition` is omitted, it defaults to `True`.

Important rule:
- If the step function accepts one parameter, the executor tries to pass `results`.
- If the function accepts no parameters, it runs without arguments.

### `Dag.branch(depends_on=None)`

Shortcut to create a branch step (`is_branch=True`).

Contract:
- return value must be `bool`;
- this value is compared with `condition` in conditional steps.

### `Dag.run(max_workers=None, raise_on_fail=True)`

Executes the DAG.

Returns:
- `results`: dict with each step return value.
- `status`: dict with each step status.

Behavior:
- parallelizes independent steps;
- marks step as `skipped` if dependency fails/is skipped or branch condition does not match;
- raises `RuntimeError` on failures when `raise_on_fail=True`.
- supports disabling specific hops (edges) for the run via `disabled_edges=[("a", "b")]`.
- supports real-time DuckDB logging during execution via:
  - `dag.run(dag_name="my_dag", logs_dir="logs", persist_duckdb=True, duckdb_file_name="run.duckdb")`

State transitions:
- `pending -> running -> done/failed`
- `pending -> skipped`

### `Dag.to_yaml(path="dag.yaml")`

Serializes structure plus last run metadata to YAML.

### `Dag.to_html(dag_name="dag", logs_dir="logs", persist_duckdb=False, duckdb_file_name="run.duckdb")`

Generates:
- `<logs_dir>/<dag_name>_<timestamp>/dag.yaml`
- `<logs_dir>/<dag_name>_<timestamp>/dag.html`
- optionally `<logs_dir>/<dag_name>_<timestamp>/run.duckdb` when `persist_duckdb=True`
  (or already created during `run(..., persist_duckdb=True)`)

Returns `(yaml_path, html_path)`.

### `Dag.validate()`

Runs structural graph validations without executing steps.

### `Dag.disable_edge(source, target)`

Disables one directed hop (`source -> target`) in the execution plan.

Behavior:
- validates that both steps exist;
- validates that the hop exists in `target.deps`;
- causes `target` to be skipped, propagating skip downstream.

### `Dag.enable_edge(source, target)`

Re-enables a previously disabled hop.

### `Dag.list_disabled_edges()`

Returns the list of currently disabled hops.

### `Dag.disable_step(step)`

Disables a step entirely for execution.

Behavior:
- the step is marked as `skipped`;
- downstream dependents are also skipped by dependency propagation.

### `Dag.enable_step(step)`

Re-enables a previously disabled step.

### `Dag.list_disabled_steps()`

Returns the list of currently disabled steps.

## Step statuses

Values used by the project:
- `pending`
- `running`
- `done`
- `failed`
- `skipped`
