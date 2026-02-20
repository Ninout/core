from __future__ import annotations

import inspect

from ninout.core.engine.models import Step
from ninout.core.ui import serialize
from ninout.core.ui.serialize import _safe_to_string, load_yaml, to_yaml


def test_to_yaml_writes_null_run_fields_when_no_run_data(tmp_path) -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
    }
    yaml_path = tmp_path / "dag.yaml"
    to_yaml(steps, path=str(yaml_path), run_data=None)
    content = yaml_path.read_text(encoding="utf-8")
    assert "status: null" in content
    assert "duration_ms: null" in content
    assert "output_lines: null" in content


def test_load_yaml_parses_empty_result_and_output(tmp_path) -> None:
    text = """steps:
  - name: a
    deps: []
    when: null
    condition: null
    is_branch: false
    code: |-
      def a():
          return None
    status: null
    duration_ms: null
    input_lines: null
    output_lines: null
    result: |-
      
    output: |-
      
"""
    path = tmp_path / "tmp_dag.yaml"
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    loaded = load_yaml(str(path))
    assert loaded["a"].status is None
    assert loaded["a"].result == ""
    assert loaded["a"].output == ""


def test_safe_to_string_handles_unserializable_values() -> None:
    class BrokenString:
        def __str__(self) -> str:
            raise RuntimeError("boom")

    assert _safe_to_string(BrokenString()) == "<resultado indisponivel>"


def test_safe_source_fallback_when_inspect_fails(monkeypatch, tmp_path) -> None:
    def broken_getsource(_obj):
        raise OSError("source unavailable")

    monkeypatch.setattr(inspect, "getsource", broken_getsource)
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
    }
    yaml_path = tmp_path / "dag.yaml"
    serialize.to_yaml(steps, path=str(yaml_path))
    content = yaml_path.read_text(encoding="utf-8")
    assert "# Codigo fonte indisponivel" in content


def test_to_yaml_writes_when_and_condition_fields(tmp_path) -> None:
    steps = {
        "branch": Step(name="branch", func=lambda: True, deps=[], is_branch=True),
        "child": Step(
            name="child",
            func=lambda: "ok",
            deps=["branch"],
            when="branch",
            condition=False,
        ),
    }
    yaml_path = tmp_path / "dag.yaml"
    to_yaml(steps, path=str(yaml_path))
    content = yaml_path.read_text(encoding="utf-8")
    assert "when: branch" in content
    assert "condition: false" in content


def test_load_yaml_parses_invalid_numeric_fields_as_none_and_condition_bool(tmp_path) -> None:
    text = """steps:
  - name: a
    deps: []
    when: branch
    condition: true
    is_branch: false
    code: |-
      def a():
          return None
    status: done
    duration_ms: not-a-number
    input_lines: nope
    output_lines: also-no
    result: |-
      value
    output: |-
      out
"""
    path = tmp_path / "dag_bad_numbers.yaml"
    path.write_text(text, encoding="utf-8")
    loaded = load_yaml(str(path))
    assert loaded["a"].when == "branch"
    assert loaded["a"].condition is True
    assert loaded["a"].duration_ms is None
    assert loaded["a"].input_lines is None
    assert loaded["a"].output_lines is None
    assert loaded["a"].disabled_deps == []
    assert loaded["a"].func() is None


def test_yaml_roundtrip_includes_disabled_deps(tmp_path) -> None:
    steps = {
        "a": Step(name="a", func=lambda: None, deps=[]),
        "b": Step(name="b", func=lambda: None, deps=["a"]),
    }
    run_data = {
        "a": {"status": "done", "disabled_deps": [], "disabled_self": False},
        "b": {"status": "skipped", "disabled_deps": ["a"], "disabled_self": True},
    }
    yaml_path = tmp_path / "dag_disabled.yaml"
    to_yaml(steps, path=str(yaml_path), run_data=run_data)
    loaded = load_yaml(str(yaml_path))
    assert loaded["b"].disabled_deps == ["a"]
    assert loaded["b"].disabled_self is True
