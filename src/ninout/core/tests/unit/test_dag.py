from __future__ import annotations

from pathlib import Path

import pytest

from ninout import Dag


def test_step_registration_and_when_dependency_auto_added() -> None:
    dag = Dag()

    @dag.step()
    def extract():
        return {"value": "ok"}

    @dag.branch(depends_on=[extract])
    def should_run() -> bool:
        return True

    @dag.step(depends_on=[extract], when=should_run, condition=True)
    def transform(results):
        return results["extract"]

    step = dag._steps["transform"]
    assert "extract" in step.deps
    assert "should_run" in step.deps
    assert step.when == "should_run"
    assert step.condition is True
    assert len(dag) == 3


def test_branch_sets_is_branch() -> None:
    dag = Dag()

    @dag.branch()
    def decision() -> bool:
        return False

    assert dag._steps["decision"].is_branch is True


def test_step_accepts_string_dependency_and_defaults_condition_true_when_when_is_set() -> None:
    dag = Dag()

    @dag.step(depends_on=["raw_source"])
    def extract():
        return {"value": "ok"}

    @dag.step(when="extract")
    def conditional():
        return {"value": "ran"}

    assert dag._steps["extract"].deps == ["raw_source"]
    assert dag._steps["conditional"].when == "extract"
    assert dag._steps["conditional"].condition is True


def test_step_ignores_condition_when_when_is_missing() -> None:
    dag = Dag()

    @dag.step(condition=True)
    def invalid():
        return {}

    dag.validate()
    assert dag._steps["invalid"].condition is None


def test_run_populates_last_run_and_to_yaml(tmp_path: Path) -> None:
    dag = Dag()

    @dag.step()
    def a():
        return [{"value": "x"}, {"value": "y"}]

    @dag.step(depends_on=[a])
    def b(results):
        return {"count": len(results["a"])}

    results, status = dag.run()
    assert results["b"]["count"] == 2
    assert status["a"] == "done"
    assert status["b"] == "done"
    assert dag._last_run is not None
    assert dag._last_run["b"]["output_lines"] == 1

    yaml_path = tmp_path / "manual.yaml"
    dag.to_yaml(path=str(yaml_path))
    assert yaml_path.exists()
    assert "steps:" in yaml_path.read_text(encoding="utf-8")


def test_to_html_creates_run_files(tmp_path: Path) -> None:
    dag = Dag()

    @dag.step()
    def only():
        return {"id": 1}

    dag.run()
    yaml_path, html_path = dag.to_html(dag_name="unit", logs_dir=str(tmp_path))
    assert Path(yaml_path).exists()
    assert Path(html_path).exists()
    assert "unit_" in yaml_path
    assert html_path.endswith("dag.html")


def test_disable_and_enable_edge_affects_execution() -> None:
    dag = Dag()

    @dag.step()
    def a():
        return {"value": "a"}

    @dag.step(depends_on=[a])
    def b():
        return {"value": "b"}

    @dag.step(depends_on=[b])
    def c():
        return {"value": "c"}

    dag.disable_edge(a, b)
    _results, status = dag.run(raise_on_fail=False)
    assert status["a"] == "done"
    assert status["b"] == "skipped"
    assert status["c"] == "skipped"
    assert dag._last_run is not None
    assert dag._last_run["b"]["disabled_deps"] == ["a"]
    assert dag.list_disabled_edges() == [("a", "b")]

    dag.enable_edge("a", "b")
    _results2, status2 = dag.run(raise_on_fail=False)
    assert status2["b"] == "done"
    assert status2["c"] == "done"


def test_disable_edge_validation_errors() -> None:
    dag = Dag()

    @dag.step()
    def a():
        return {"value": "a"}

    @dag.step(depends_on=[a])
    def b():
        return {"value": "b"}

    with pytest.raises(ValueError):
        dag.disable_edge("a", "missing")

    with pytest.raises(ValueError):
        dag.disable_edge("b", "a")


def test_disable_and_enable_step_affects_execution() -> None:
    dag = Dag()

    @dag.step()
    def a():
        return {"value": "a"}

    @dag.step(depends_on=[a])
    def b():
        return {"value": "b"}

    @dag.step(depends_on=[b])
    def c():
        return {"value": "c"}

    dag.disable_step("b")
    _results, status = dag.run(raise_on_fail=False)
    assert status["a"] == "done"
    assert status["b"] == "skipped"
    assert status["c"] == "skipped"
    assert dag.list_disabled_steps() == ["b"]
    assert dag._last_run is not None
    assert dag._last_run["b"]["disabled_self"] is True

    dag.enable_step(b)
    _results2, status2 = dag.run(raise_on_fail=False)
    assert status2["b"] == "done"
    assert status2["c"] == "done"


def test_disable_step_validation_error() -> None:
    dag = Dag()

    @dag.step()
    def only():
        return {"value": "ok"}

    with pytest.raises(ValueError):
        dag.disable_step("missing")
