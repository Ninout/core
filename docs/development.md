# Development

## Requirements

- Python 3.12+ (defined in `pyproject.toml`)
- `uv` to run project commands

## Quick setup

Check the Python version in the environment:

```bash
uv run python -V
```

## Running tests

In this environment, the most reliable command is:

```bash
uv run python -m pytest -q
```

## Coverage

Coverage is enabled via `pytest-cov` in `pyproject.toml`.

Run:

```bash
uv run python -m pytest
```

Generated reports:
- terminal report with missing lines;
- `src/ninout/core/tests/reports/coverage.xml`.
- `src/ninout/core/tests/reports/htmlcov/index.html` for a navigable HTML report.

Coverage scope:
- coverage is measured for production code in `src/ninout`;
- files under `src/ninout/core/tests/*` are excluded.

## Current test coverage

- Unit:
  - validation and topological ordering;
  - node layout;
  - YAML serialization and parsing edge cases;
  - executor state transitions and branch behavior;
  - DAG API registration and run metadata.
- Integration:
  - executor (results and metrics);
  - HTML rendering from YAML.

Latest local run (February 20, 2026):
- `36 passed`
- `99%` coverage (production code scope)

## Contribution best practices

- Keep example DAGs in `transformations/`.
- Cover engine/UI changes with tests in `src/ninout/core/tests/`.
- Manually run at least one DAG after structural changes.
- Avoid adding dependencies unless necessary.
