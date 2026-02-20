# Overview

## What ninout is

`ninout` is a Python framework to model and execute DAG (Directed Acyclic Graph) pipelines using decorators.

Each decorated function becomes a graph step, with:
- explicit dependencies,
- conditional branch support (`if/else`),
- parallel execution when dependencies allow it,
- execution observability through YAML logs and HTML visualization,
- per-step status and metrics (`duration_ms`, `input_lines`, `output_lines`).

## Practical goal

Enable declarative and lightweight data transformations while keeping:
- simplicity in DAG creation,
- graph consistency validation,
- traceability for each step result.

## Usage flow

1. Create a `Dag()` instance.
2. Declare steps with `@dag.step(...)` and branches with `@dag.branch(...)`.
3. Execute with `dag.run()`.
4. Generate artifacts with `dag.to_html(dag_name=...)`.

## Core concepts

- `step`: unit of work.
- `deps`: list of steps that must complete first.
- `branch`: step that returns `bool` to drive conditional execution.
- `when` + `condition`: mechanism to model conditional paths.
- `results`: dictionary with completed step outputs, injected when the function accepts a parameter.
