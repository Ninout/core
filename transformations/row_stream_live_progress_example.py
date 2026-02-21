from __future__ import annotations

import os
import random
import sys
import time
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ninout import Dag


def build_dag(total_rows: int = 500) -> Dag:
    dag = Dag()

    @dag.step(mode="task")
    def extract_orders():
        random.seed(7)
        rows = []
        for order_id in range(1, total_rows + 1):
            rows.append(
                {
                    "order_id": order_id,
                    "customer_id": random.randint(1, 80),
                    "amount": round(random.uniform(5.0, 500.0), 2),
                    "status": random.choice(["paid", "pending", "cancelled"]),
                }
            )
        return rows

    @dag.step(depends_on=[extract_orders], mode="row")
    def normalize_orders(row):
        # Sleep to make row-by-row progress visible in live UI.
        time.sleep(0.008)
        row["amount"] = float(row["amount"])
        row["is_paid"] = row["status"] == "paid"
        return row

    @dag.step(depends_on=[normalize_orders], mode="row")
    def keep_paid_orders(row):
        time.sleep(0.005)
        if not row["is_paid"]:
            return None
        return row

    @dag.step(depends_on=[keep_paid_orders], mode="row")
    def add_risk_score(row):
        time.sleep(0.003)
        risk_score = min(100.0, (row["amount"] / 5.0))
        return {
            "order_id": row["order_id"],
            "customer_id": row["customer_id"],
            "amount": row["amount"],
            "risk_score": round(risk_score, 2),
        }

    @dag.step(depends_on=[add_risk_score], mode="task")
    def summarize(results):
        rows = results["add_risk_score"]
        total = len(rows)
        avg_amount = round(sum(row["amount"] for row in rows) / total, 2) if total else 0.0
        avg_risk = round(sum(row["risk_score"] for row in rows) / total, 2) if total else 0.0
        return {
            "rows_processed": total,
            "avg_amount": avg_amount,
            "avg_risk_score": avg_risk,
        }

    return dag


if __name__ == "__main__":
    dag_name = "row_stream_live_progress_example"
    dag = build_dag(total_rows=500)
    print("Running pipeline (watch progress in dashboard/API)...")
    state: dict[str, object] = {}

    def _run_pipeline() -> None:
        results, status = dag.run(
            raise_on_fail=False,
            dag_name=dag_name,
            persist_duckdb=True,
        )
        state["results"] = results
        state["status"] = status

    run_thread = threading.Thread(target=_run_pipeline)
    run_thread.start()

    while run_thread.is_alive():
        run_dir = dag._last_run_dir
        if run_dir:
            print(f"Run dir: {run_dir}", end="\r")
        time.sleep(0.5)

    run_thread.join()
    results = state["results"]
    status = state["status"]
    print(f"Status: {status}")
    print(f"Summary: {results['summarize']}")
    print(f"Run dir: {dag._last_run_dir}")
