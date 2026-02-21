from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ninout import Dag


def build_dag() -> Dag:
    dag = Dag()

    @dag.step()
    def extract():
        print("extract")
        return {"value": "raw-data"}

    @dag.step(depends_on=[extract])
    def transform(results):
        print("transform")
        return {"value": str(results["extract"]["value"]).upper()}

    @dag.step(depends_on=[transform])
    def load(results):
        print(f"load: {results['transform']['value']}")
        return {"status": "ok"}

    # Disable the hop extract -> transform.
    # Expected behavior:
    # - extract runs
    # - transform is skipped
    # - load is skipped (downstream propagation)
    dag.disable_edge(extract, transform)

    return dag


if __name__ == "__main__":
    dag = build_dag()
    _results, status = dag.run(raise_on_fail=False)
    print(f"Status: {status}")
    yaml_path, html_path = dag.to_html(dag_name="disabled_edge_example")
    print(f"DAG generated in {html_path}")
    print(f"YAML generated in {yaml_path}")
