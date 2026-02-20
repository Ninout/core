# Examples

## Example 1: DAG with branch

Reference file: `transformations/example_dag.py`

Flow:
1. `extrair` returns base data.
2. `precisa_transformar` decides the path (`True/False`).
3. `transformar` runs only when the branch is `True`.
4. `pular_transformacao` runs only when the branch is `False`.
5. `carregar` consumes transformed output.

Command:

```bash
uv run transformations/example_dag.py
```

## Example 2: API to JSON

Reference file: `transformations/api_to_json.py`

Flow:
1. `fetch_posts`: fetches posts from `jsonplaceholder`.
2. `transform_posts`: keeps top 10 and normalizes titles.
3. `save_json`: writes to `posts_output.json`.

Command:

```bash
uv run transformations/api_to_json.py
```

## Minimal example to create a DAG

```python
from ninout import Dag

dag = Dag()

@dag.step()
def extract():
    return "data"

@dag.step(depends_on=[extract])
def transform(results):
    return results["extract"].upper()

@dag.step(depends_on=[transform])
def load(results):
    print(results["transform"])

dag.run()
dag.to_html(dag_name="mini")
```
