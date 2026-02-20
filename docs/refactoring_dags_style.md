# Refactoring DAG Style

## Goal

Propose a more friendly way to model relationships between steps/nodes in `ninout`, using ideas from Airflow and other orchestrators.

## Problem in current style

Current usage is explicit but verbose:

```python
@dag.step(depends_on=[extract], when=should_transform, condition=True)
def transform(results):
    ...
```

This works, but becomes noisy in bigger DAGs with fan-out/fan-in and branching.

## Reference patterns in orchestrators

### Airflow

- Fluent dependency operators: `task_a >> task_b`, `task_b << task_a`
- Helpers for graph wiring: `chain(...)`, `cross_downstream(...)`
- Easy to read dependency lines in code.

### Prefect

- Data dependency + explicit control dependency (`wait_for=[...]`)
- Separates "needs output" from "must wait for completion".

### Argo Workflows

- DAG dependencies with expressive conditions (`depends`)
- Can model advanced branch logic by result/status.

### n8n

- Node-connection visual style
- Friendly for modeling and understanding flow topology quickly.

### Luigi

- Strong task dependency model with clear lifecycle contracts (`requires`, `run`, `output`)

## Recommended direction for `ninout` (hybrid API)

### 1. Fluent syntax like Airflow

Support:

```python
extract >> [clean, validate] >> load
```

Benefits:
- very readable flow definition
- less repeated `depends_on=[...]`

### 2. Wiring helpers for common patterns

Add utilities:
- `chain(a, b, c)`
- `cross_downstream([a, b], [c, d])`
- `fan_out(a, [b, c])`
- `fan_in([b, c], d)`

Benefits:
- easier composition for ETL-like DAGs
- fewer manual dependency declarations

### 3. Explicit wait/control dependencies

Add `wait_for=[...]` in addition to data dependencies.

Example:

```python
@dag.step(wait_for=[clean, validate])
def load():
    ...
```

Benefits:
- clearer orchestration intent
- matches Prefect-like control dependency semantics

### 4. Friendlier branch API

Instead of `when + condition` only, add:
- `when_true=branch_step`
- `when_false=branch_step`

Benefits:
- avoids implicit behavior confusion
- clearer `if/else` reading

### 5. Keep backward compatibility

Keep existing API valid:
- `depends_on`
- `when`
- `condition`

Expose new style as ergonomic layer. No breaking migration required.

## Suggested migration plan

1. Add step handles and operator overloads (`>>`, `<<`) without changing executor semantics.
2. Add helper functions (`chain`, `cross_downstream`, `fan_out`, `fan_in`).
3. Add `when_true`/`when_false` aliases and map internally to existing `when/condition`.
4. Add `wait_for` as control dependency.
5. Update docs/examples to use fluent style as default recommendation.

## Visual editor roadmap

Yes, this is planned as a future milestone.

Suggested phases:
1. JSON graph schema for nodes/hops as a source of truth.
2. Import/export between Python DAG and JSON graph.
3. Basic visual editor (create nodes, connect/disconnect hops, enable/disable hop).
4. Live validation (cycle detection, missing deps, branch contract checks).
5. Run configuration panel (depth/hops toggles for development runs).

## Example target API

```python
from ninout import Dag, chain

dag = Dag()

extract = dag.task()(extract_fn)
clean = dag.task()(clean_fn)
validate = dag.task()(validate_fn)
load = dag.task(wait_for=[clean, validate])(load_fn)

extract >> [clean, validate]
chain(clean, load)
chain(validate, load)
```

## References

- Airflow tasks and dependencies:
  - https://airflow.apache.org/docs/apache-airflow/2.11.0/core-concepts/tasks.html
- Airflow DAG helpers (`chain`, `cross_downstream`):
  - https://airflow.apache.org/docs/apache-airflow/3.0.0/core-concepts/dags.html
- Prefect tasks and orchestration concepts:
  - https://docs.prefect.io/v3/concepts/tasks
- Argo DAG:
  - https://argo-workflows.readthedocs.io/en/latest/walk-through/dag/
- Argo enhanced depends logic:
  - https://argo-workflows.readthedocs.io/en/latest/enhanced-depends-logic/
- n8n node connections:
  - https://docs.n8n.io/workflows/components/connections/
- Luigi task model:
  - https://luigi.readthedocs.io/en/latest/tasks.html
