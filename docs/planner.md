# Pipeline Planner Strategy

## Objective

Support two authoring interfaces for pipelines:
- visual modeling (drag-and-drop, Pentaho-style),
- code modeling (Python API, current `ninout` style),

with a single intermediate representation and a unified execution path.

## Core idea

Use a single intermediate model as source of truth:

- `PipelinePlan` (authoring/runtime-neutral representation)

Both interfaces (visual and code) read/write this model.
The executor should run from a compiled execution plan derived from it.

## Architecture

### 1. Authoring layer

- **Code authoring**: decorators/API generate a `PipelinePlan`.
- **Visual authoring**: UI edits the same `PipelinePlan` in JSON form.

### 2. Planning layer (intermediate step)

Introduce `planner.py` to compile:

- `PipelinePlan` + runtime options
- into `ExecutionPlan`

The planner is responsible for:
- validation (cycles, missing deps, branch contracts),
- applying enable/disable rules (steps and hops),
- partial-run filters (depth, targets),
- pruning and status attribution (`planned`, `pruned`, `skipped`).

### 3. Execution layer

Executor consumes only `ExecutionPlan`.
No UI-specific or code-specific behavior in the executor.

### 4. Observability layer

Persist plan + run metadata and render:
- node status,
- hop status,
- reasons (`disabled`, `pruned_by_depth`, `excluded`, etc.).

## Proposed `PipelinePlan` contents

- pipeline metadata (`name`, `version`, `schema_version`)
- nodes:
  - `id`, `type` (`step`, `branch`)
  - runtime config (retry, timeout, tags)
  - enable/disable flags
  - visual metadata (`x`, `y`, groups)
- edges (hops):
  - `source`, `target`
  - enable/disable flags
  - optional condition metadata
- optional defaults/global settings

## Why this approach works

- Prevents divergence between visual and code modes.
- Enables incremental migration without breaking existing DAG code.
- Centralizes advanced behavior (partial run, pruning, disabled hops/steps).
- Improves CI/governance with schema validation and semantic diffs.

## Recommended implementation phases

### Phase 1: Foundation

1. Define `PipelinePlan` schema (JSON).
2. Add `Dag.to_plan()` and `Dag.from_plan()`.
3. Keep current executor behavior, but allow plan-based input path.

### Phase 2: Planner

1. Implement `planner.py` producing `ExecutionPlan`.
2. Add support for:
   - disabled hops/steps,
   - `max_depth`,
   - `target_steps`.
3. Feed `ExecutionPlan` into executor.

### Phase 3: Visual MVP

1. Build basic editor (nodes, edges, enable/disable).
2. Save/load `PipelinePlan`.
3. Run pipeline from visual model through planner.

### Phase 4: Round-trip and DX

1. Export plan to code and import code to plan.
2. Add semantic diff tooling for PRs.
3. Improve collaboration workflow (review visual changes + code changes).

## Suggested data flow

1. Author in code or UI.
2. Persist `pipeline.plan.json`.
3. Validate plan in CI.
4. Compile with planner to `ExecutionPlan`.
5. Execute.
6. Persist run logs and render graph/results.

## Practical recommendation

Treat `PipelinePlan` as the single source of truth.
Code API and visual editor are two frontends for the same model.
This keeps behavior consistent and makes the platform scalable.
