from __future__ import annotations

from ninout.core.engine.models import Step
from ninout.core.ui.render import to_html_from_yaml
from ninout.core.ui.serialize import to_yaml


def test_render_from_yaml(tmp_path) -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
    }
    run_data = {
        "a": {
            "status": "done",
            "output": "",
            "duration_ms": 0.1,
            "result": {"id": 1},
            "input_lines": 0,
            "output_lines": 1,
        }
    }
    yaml_path = tmp_path / "dag.yaml"
    html_path = tmp_path / "dag.html"
    to_yaml(steps, path=str(yaml_path), run_data=run_data)
    to_html_from_yaml(str(yaml_path), html_path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "DAG Visualization" in content
    assert "Preview" in content
