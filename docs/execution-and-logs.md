# Execution and Logs

## Running a DAG

Example with a local script:

```bash
uv run transformations/example_dag.py
```

Another example:

```bash
uv run transformations/api_to_json.py
```

## Output generated per run

Each run creates a timestamped folder under `logs/`:

```text
logs/<dag_name>_<YYYYMMDD_HHMMSS>/
```

Files:
- `dag.yaml`: structured snapshot of the DAG plus execution data.
- `dag.html`: interactive visualization generated from YAML.

## What is stored in YAML

For each step, YAML stores:
- structural metadata (`name`, `deps`, `when`, `condition`, `is_branch`);
- step source code (`code`);
- execution status (`status`);
- timing (`duration_ms`);
- volume metrics (`input_lines`, `output_lines`);
- serialized `result`;
- captured `print` output (`output`).

## Reading the HTML

In HTML:
- the top section shows the DAG as SVG;
- clicking a node opens the detail panel;
- preview attempts to render JSON as a table;
- statuses and metrics are shown as badges and cards.

## Error behavior

- A failed step is marked as `failed`.
- Dependent steps usually become `skipped`.
- With `raise_on_fail=True`, execution ends by raising an exception.

## Runtime notes

- The scheduler evaluates pending steps continuously and runs ready steps in parallel.
- Branch mismatch does not fail a step; it marks the step as `skipped`.
- `stdout`/`stderr` are captured per step and stored in YAML under `output`.
- Disabled hops (`source -> target`) also mark target/downstream as `skipped`.
- Disabled steps (`disable_step`) are marked as `skipped` and propagate skip downstream.
