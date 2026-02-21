from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ninout import Dag


def build_dag() -> Dag:
    dag = Dag()

    @dag.step(mode="task")
    def extract():
        return [
            {"id": 1, "name": "ana"},
            {"id": 2, "name": "bruno"},
            {"id": 3, "name": "carla"},
        ]

    @dag.step(depends_on=[extract], mode="row")
    def enrich(rows):
        return [
            {
                "id": row["id"],
                "name": row["name"],
                "name_upper": str(row["name"]).upper(),
            }
            for row in rows
        ]

    @dag.step(depends_on=[enrich], mode="sql")
    def summarize(results):
        total_rows = len(results["enrich"])
        return f"SELECT {total_rows}::INTEGER AS total_rows, 'hybrid' AS pipeline_mode"

    @dag.step(depends_on=[summarize], mode="task")
    def load(results):
        summary = results["summarize"][0]
        return {
            "message": "pipeline finished",
            "total_rows": summary["total_rows"],
            "mode": summary["pipeline_mode"],
        }

    return dag


if __name__ == "__main__":
    dag = build_dag()
    _results, status = dag.run(raise_on_fail=False, dag_name="hybrid_modes_example")
    print(f"Status: {status}")
    print(f"Run dir: {dag._last_run_dir}")
