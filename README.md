# ninout

`ninout` is a Python package to model and execute DAG pipelines with decorators.
Each decorated function becomes a step. The engine supports conditional paths, task/row/sql modes, parallel scheduling, and DuckDB-first observability.

## Project layout

- `src/ninout/`: installable package.
- `src/ninout/core/engine/`: DAG model, planner, executor, validation.
- `src/ninout/core/api/`: FastAPI app to query run logs from DuckDB.
- `src/ninout/core/ui/dashboard/`: web dashboard frontend assets.
- `docs/`: project documentation.
- `transformations/`: runnable DAG examples.
- `logs/`: one folder per execution, `logs/<dag_name>_<timestamp>/`.
- `src/ninout/core/tests/`: unit and integration tests.

## Quick start

```python
from ninout import Dag

def build_dag() -> Dag:
    dag = Dag()

    @dag.step()
    def extract():
        return [{"value": "ok"}]

    @dag.branch(depends_on=[extract])
    def should_transform(results) -> bool:
        return True

    @dag.step(depends_on=[extract], when=should_transform, condition=True)
    def transform(results):
        return [{"value": results["extract"][0]["value"].upper()}]

    return dag

if __name__ == "__main__":
    dag = build_dag()
    dag.run(dag_name="example")
```

Run a DAG:

```bash
uv run transformations/<dag>.py
```

Run API + dashboard:

```bash
uv run uvicorn ninout.core.api.main:app --reload
```

Open:
- `http://127.0.0.1:8000/dashboard`
- `http://127.0.0.1:8000/api/runs`

## Logs per execution

Each run generates:
- `logs/<dag_name>_<timestamp>/run.duckdb`

DuckDB stores:
- run metadata,
- step definition metadata,
- step runtime status/metrics,
- per-step row payload tables.

## Tests

```bash
uv run python -m pytest -q
```

Coverage report:

```bash
uv run python -m pytest
```

Generated files:
- `src/ninout/core/tests/reports/coverage.xml`
- `src/ninout/core/tests/reports/htmlcov/index.html`
