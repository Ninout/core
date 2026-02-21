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
    def extract_users():
        return [
            {"id": 1, "name": "ana"},
            {"id": 2, "name": "bruno"},
            {"id": 3, "name": "carla"},
        ]

    @dag.branch(depends_on=[extract_users])
    def should_uppercase() -> bool:
        return False

    @dag.step(depends_on=[extract_users], when=should_uppercase, condition=True)
    def uppercase_names(results):
        return [{"id": row["id"], "name": row["name"].upper()} for row in results["extract_users"]]

    @dag.step(depends_on=[extract_users], when=should_uppercase, condition=False)
    def keep_original(results):
        return results["extract_users"]

    @dag.step(depends_on=[keep_original])
    def load(results):
        rows = results["keep_original"]
        return {"loaded_rows": len(rows)}

    return dag


if __name__ == "__main__":
    dag_name = "test_metadata_to_duckdb"
    dag = build_dag()
    _results, status = dag.run(
        raise_on_fail=False,
        dag_name=dag_name,
        persist_duckdb=True,
        duckdb_file_name="metadata.duckdb",
    )
    print(f"Status: {status}")
    print(f"Run dir: {dag._last_run_dir}")
    if dag._last_run_dir:
        print(f"DuckDB: {os.path.join(dag._last_run_dir, 'metadata.duckdb')}")
