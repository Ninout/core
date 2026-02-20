# ninout

`ninout` is a Python package to create and execute DAGs with decorators.  
Each decorated function becomes a step. The engine runs steps in parallel when possible, supports `if/else`, and generates per-run logs (YAML + HTML).

Current local quality snapshot (February 20, 2026):
- `36` tests passing
- `99%` coverage for production code under `src/ninout` (tests excluded from coverage scope)

## Project layout

- `src/ninout/`: installable Python package.
- `src/ninout/core/`: engine, validation, rendering, serialization.
- `docs/`: project documentation split by topic.
- `transformations/`: user DAGs runnable with `uv run`.
- `logs/`: one folder per execution, `logs/<dag_name>_<timestamp>/`.
- `src/ninout/core/tests/`: unit and integration tests.

## Quick start

```python
from ninout import Dag

def build_dag() -> Dag:
    dag = Dag()

    @dag.step()
    def extract():
        return "data"

    @dag.branch(depends_on=[extract])
    def should_transform() -> bool:
        return True

    @dag.step(depends_on=[extract], when=should_transform, condition=True)
    def transform(results):
        return results["extract"].upper()

    return dag

if __name__ == "__main__":
    dag = build_dag()
    dag.run()
    yaml_path, html_path = dag.to_html(dag_name="example")
    print(f"DAG generated in {html_path}")
```

Run a DAG:
```
uv run transformations/<dag>.py
```

## Logs per execution

Each run generates:
- `logs/<dag_name>_<timestamp>/dag.yaml`
- `logs/<dag_name>_<timestamp>/dag.html`
- optional `logs/<dag_name>_<timestamp>/run.duckdb` when `dag.run(..., persist_duckdb=True)`

## Development

Check environment:
```
uv run python -V
```

Run an example DAG:
```
uv run transformations/example_dag.py
```

## Tests

```
uv run python -m pytest -q
```

Coverage:
```
uv run python -m pytest
```
Generated files:
- `src/ninout/core/tests/reports/coverage.xml`
- `src/ninout/core/tests/reports/htmlcov/index.html`

## Notes

- If a step accepts a parameter, it receives `results` with all completed outputs.
- HTML preview explodes JSON at the first level and shows it as a table.
