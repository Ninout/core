from __future__ import annotations

from fastapi.testclient import TestClient

from ninout.core.api.main import app
from ninout.core.engine.dag import Dag


def _create_sample_run(logs_dir: str) -> str:
    dag = Dag()

    @dag.step()
    def extract():
        return [{"id": 1}, {"id": 2}]

    @dag.step(depends_on=[extract], mode="row")
    def enrich(row):
        return {"id": row["id"], "value": row["id"] * 10}

    dag.run(dag_name="api_test_run", logs_dir=logs_dir)
    assert dag._last_run_dir is not None
    return dag._last_run_dir


def test_api_lists_runs_and_returns_details_and_rows(tmp_path, monkeypatch) -> None:
    logs_dir = str(tmp_path / "logs")
    _create_sample_run(logs_dir)
    monkeypatch.setenv("NINOUT_LOGS_DIR", logs_dir)

    client = TestClient(app)

    runs_response = client.get("/api/runs")
    assert runs_response.status_code == 200
    runs = runs_response.json()
    assert len(runs) == 1
    run_name = runs[0]["run_name"]

    detail_response = client.get(f"/api/runs/{run_name}")
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["dag_name"] == "api_test_run"
    step_names = {step["step_name"] for step in detail["steps"]}
    assert "extract" in step_names
    assert "enrich" in step_names

    rows_response = client.get(f"/api/runs/{run_name}/steps/enrich/rows?limit=10&offset=0")
    assert rows_response.status_code == 200
    payload = rows_response.json()
    assert payload["total_rows"] == 2
    assert len(payload["rows"]) == 2

    graph_response = client.get(f"/api/runs/{run_name}/graph")
    assert graph_response.status_code == 200
    graph = graph_response.json()
    assert graph["run_name"] == run_name
    assert len(graph["nodes"]) == 2
    assert len(graph["edges"]) == 1
    assert graph["edges"][0]["source"] == "extract"
    assert graph["edges"][0]["target"] == "enrich"


def test_dashboard_route_serves_html(tmp_path, monkeypatch) -> None:
    logs_dir = str(tmp_path / "logs")
    _create_sample_run(logs_dir)
    monkeypatch.setenv("NINOUT_LOGS_DIR", logs_dir)

    client = TestClient(app)
    response = client.get("/dashboard")
    assert response.status_code == 200
    assert "ninout dashboard" in response.text
