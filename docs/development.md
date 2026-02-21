# Development

## Requirements

- Python 3.12+
- `uv`

## Running tests

```bash
uv run python -m pytest -q
```

## Coverage

```bash
uv run python -m pytest
```

Generated reports:
- `src/ninout/core/tests/reports/coverage.xml`
- `src/ninout/core/tests/reports/htmlcov/index.html`

Coverage scope:
- production code in `src/ninout`
- tests under `src/ninout/core/tests/*` excluded

## Current test focus

- DAG registration and run behavior
- planner/executor transitions
- DuckDB persistence and metrics
- FastAPI dashboard endpoints

## Contribution notes

- Keep examples in `transformations/`.
- Prefer tests in `src/ninout/core/tests/`.
- Validate at least one real run after structural changes.
