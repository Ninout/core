from __future__ import annotations

from ninout.core.engine.models import Step
import ninout.core.ui.render as render_module
from ninout.core.ui.render import to_html_from_duckdb, to_html_from_steps


def test_render_includes_branch_edge_styles_and_status_badges(tmp_path) -> None:
    steps = {
        "start": Step(name="start", func=lambda: None, deps=[], status="done", code="pass"),
        "decision": Step(
            name="decision",
            func=lambda: None,
            deps=["start"],
            is_branch=True,
            status="done",
            code="return True",
        ),
        "on_true": Step(
            name="on_true",
            func=lambda: None,
            deps=["start", "decision"],
            when="decision",
            condition=True,
            status="done",
            code="return 1",
            result='{"v": 1}',
        ),
        "on_false": Step(
            name="on_false",
            func=lambda: None,
            deps=["start", "decision"],
            when="decision",
            condition=False,
            status="skipped",
            code="return 0",
        ),
    }
    html_path = tmp_path / "dag.html"
    to_html_from_steps(steps, path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "status-ok" in content
    assert "status-skip" in content
    assert "stroke='#1f8a4c'" in content
    assert "stroke='#c0392b'" in content
    assert "stroke-dasharray='6 4'" in content


def test_render_escapes_code_and_result_content(tmp_path) -> None:
    steps = {
        "a": Step(
            name="a",
            func=lambda: None,
            deps=[],
            status="done",
            code='print("<script>alert(1)</script>")',
            result='<img src=x onerror=alert(1)>',
        ),
    }
    html_path = tmp_path / "dag.html"
    to_html_from_steps(steps, path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "<script>alert(1)</script>" not in content
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in content
    assert "&lt;img src=x onerror=alert(1)&gt;" in content


def test_render_includes_failed_status_badge(tmp_path) -> None:
    steps = {
        "fail_step": Step(
            name="fail_step",
            func=lambda: None,
            deps=[],
            status="failed",
            code="raise RuntimeError()",
        ),
    }
    html_path = tmp_path / "dag.html"
    to_html_from_steps(steps, path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "status-fail" in content


def test_render_includes_disabled_edge_style(tmp_path) -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(
            name="b",
            func=lambda: None,
            deps=["a"],
            disabled_deps=["a"],
        ),
    }
    html_path = tmp_path / "dag.html"
    to_html_from_steps(steps, path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "stroke='#9aa0a6'" in content
    assert "stroke-dasharray='4 4'" in content


def test_render_marks_disabled_node_style_and_badge(tmp_path) -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[], disabled_self=True),
    }
    html_path = tmp_path / "dag.html"
    to_html_from_steps(steps, path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "fill='#e2e2e2'" in content
    assert "disabled" in content


def test_render_from_duckdb_uses_loaded_steps(monkeypatch, tmp_path) -> None:
    def fake_load_steps(_db_path: str):
        return {"a": Step(name="a", func=lambda: None, deps=[], status="done", code="pass")}

    monkeypatch.setattr(render_module, "load_steps_from_duckdb", fake_load_steps)
    html_path = tmp_path / "dag.html"
    to_html_from_duckdb("fake.duckdb", html_path=str(html_path))
    content = html_path.read_text(encoding="utf-8")
    assert "DAG Visualization" in content
