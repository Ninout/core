from __future__ import annotations

import json
import os
import sys
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ninout import Dag


def build_dag() -> Dag:
    dag = Dag()

    @dag.step()
    def fetch_posts():
        url = "https://jsonplaceholder.typicode.com/posts"
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))
        return data

    @dag.step(depends_on=[fetch_posts])
    def transform_posts(results):
        posts = results["fetch_posts"]
        transformed = [
            {"id": post["id"], "title": post["title"].strip().title()}
            for post in posts[:10]
        ]
        return transformed

    @dag.step(depends_on=[transform_posts])
    def save_json(results):
        output_path = os.path.join(ROOT, "posts_output.json")
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results["transform_posts"], f, ensure_ascii=False, indent=2)
            return results["transform_posts"]
        print(f"Arquivo salvo em {output_path}")

    return dag


if __name__ == "__main__":
    dag = build_dag()
    _results, status = dag.run(dag_name="api_to_json")
    print(f"Status: {status}")
    print(f"Run dir: {dag._last_run_dir}")
