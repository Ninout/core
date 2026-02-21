# Examples

## Example 1: branch DAG

Reference: `transformations/example_dag.py`

Run:

```bash
uv run transformations/example_dag.py
```

## Example 2: API to JSON

Reference: `transformations/api_to_json.py`

Run:

```bash
uv run transformations/api_to_json.py
```

## Example 3: hybrid modes

Reference: `transformations/hybrid_modes_example.py`

Run:

```bash
uv run transformations/hybrid_modes_example.py
```

## Inspect executions in dashboard

Start API:

```bash
uv run uvicorn ninout.core.api.main:app --reload
```

Open:
- `http://127.0.0.1:8000/dashboard`
