from __future__ import annotations

from ninout.core.engine.models import Step
from ninout.core.ui.serialize import load_yaml, to_yaml


def test_yaml_roundtrip(tmp_path) -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
    }
    run_data = {
        "a": {
            "status": "done",
            "output": "ok",
            "duration_ms": 1.23,
            "result": {"k": "v"},
            "input_lines": 0,
            "output_lines": 1,
        },
        "b": {
            "status": "done",
            "output": "",
            "duration_ms": 2.0,
            "result": ["x", "y"],
            "input_lines": 1,
            "output_lines": 2,
        },
    }
    yaml_path = tmp_path / "dag.yaml"
    to_yaml(steps, path=str(yaml_path), run_data=run_data)
    loaded = load_yaml(str(yaml_path))
    assert loaded["a"].status == "done"
    assert loaded["a"].result is not None
    assert loaded["b"].output_lines == 2
