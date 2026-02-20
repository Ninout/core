# Architecture

## Folder structure

- `src/ninout/`: installable package.
- `src/ninout/core/engine/`: execution engine, validation, and data models.
- `src/ninout/core/ui/`: YAML serialization, layout, and HTML rendering.
- `transformations/`: runnable example DAGs.
- `logs/`: per-run output (`dag.yaml` + `dag.html`).
- `src/ninout/core/tests/`: unit and integration tests.

## Main modules

### `src/ninout/core/engine/dag.py`

Defines the `Dag` class, which:
- registers steps/branches through decorators,
- executes the graph with `run`,
- stores data from the last run (`_last_run`),
- serializes and renders log artifacts.

### `src/ninout/core/engine/models.py`

Defines the `Step` dataclass, with metadata for:
- graph structure (`name`, `deps`, `when`, `condition`, `is_branch`),
- observability fields (`status`, `duration_ms`, `input_lines`, `output_lines`, `result`, `output`).

### `src/ninout/core/engine/validate.py`

Responsible for:
- validating unknown dependencies,
- enforcing `when`/`condition` consistency,
- detecting cycles,
- generating topological order and graph levels.

### `src/ninout/core/engine/executor.py`

Executes the DAG with `ThreadPoolExecutor`:
- schedules ready steps in parallel,
- tracks status (`pending`, `running`, `done`, `failed`, `skipped`) with guarded transitions,
- applies skip rules for failed/skipped dependencies,
- resolves branch conditions by comparing branch output with `condition`,
- captures `stdout/stderr` per thread,
- calculates input/output metrics and duration.

The execution loop follows this order per pending step:
1. Skip if any dependency already failed/skipped.
2. Wait if dependencies are not all `done`.
3. Skip if branch condition does not match.
4. Submit to worker pool and transition to `running`.

### `src/ninout/core/ui/serialize.py`

Converts steps to YAML and reconstructs them from YAML:
- includes step source code (when available),
- persists execution metadata for rendering.

### `src/ninout/core/ui/render.py`

Generates interactive HTML with:
- SVG graph (nodes and edges),
- per-step panel with status, metrics, code, and result preview.

## Execution cycle (technical view)

1. `Dag.run()` calls `executor.run(...)`.
2. The executor validates and topologically sorts the DAG.
3. Steps run respecting dependencies and branches.
4. Results and status transitions are consolidated in-memory.
5. Consolidated run data is saved to `Dag._last_run`.
6. `Dag.to_html()` writes YAML and generates HTML from it.
