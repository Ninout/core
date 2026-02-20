from __future__ import annotations

import inspect
import json
from typing import Mapping

from ninout.core.engine.models import Step
from ninout.core.engine.validate import validate_steps


def _indent(text: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(pad + line if line else pad for line in text.splitlines())


def _safe_source(func: object) -> str:
    try:
        return inspect.getsource(func).rstrip()
    except OSError:
        return "# Codigo fonte indisponivel"


def to_yaml(
    steps: Mapping[str, Step],
    path: str = "dag.yaml",
    run_data: Mapping[str, Mapping[str, object]] | None = None,
) -> None:
    validate_steps(steps)

    lines: list[str] = []
    lines.append("steps:")
    for step in steps.values():
        lines.append(f"  - name: {step.name}")
        if step.deps:
            lines.append("    deps:")
            for dep in step.deps:
                lines.append(f"      - {dep}")
        else:
            lines.append("    deps: []")
        if step.when is not None:
            lines.append(f"    when: {step.when}")
            lines.append(f"    condition: {str(step.condition).lower()}")
        else:
            lines.append("    when: null")
            lines.append("    condition: null")
        lines.append(f"    is_branch: {str(step.is_branch).lower()}")
        source = _safe_source(step.func)
        lines.append("    code: |-")
        lines.append(_indent(source, 6))
        if run_data and step.name in run_data:
            meta = run_data[step.name]
            status = str(meta.get("status", ""))
            output = str(meta.get("output", ""))
            duration = meta.get("duration_ms", None)
            result = meta.get("result", None)
            input_lines = meta.get("input_lines", None)
            output_lines = meta.get("output_lines", None)
        else:
            status = ""
            output = ""
            duration = None
            result = None
            input_lines = None
            output_lines = None
        lines.append(f"    status: {status or 'null'}")
        lines.append(
            f"    duration_ms: {duration if isinstance(duration, (int, float)) else 'null'}"
        )
        lines.append(
            f"    input_lines: {input_lines if isinstance(input_lines, int) else 'null'}"
        )
        lines.append(
            f"    output_lines: {output_lines if isinstance(output_lines, int) else 'null'}"
        )
        lines.append("    result: |-")
        lines.append(_indent(_safe_to_string(result), 6))
        lines.append("    output: |-")
        lines.append(_indent(output, 6))

    content = "\n".join(lines) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def load_yaml(path: str) -> dict[str, Step]:
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    steps: dict[str, Step] = {}
    idx = 0
    current: dict[str, object] | None = None

    while idx < len(lines):
        line = lines[idx]
        if line.startswith("  - name: "):
            if current is not None:
                _add_step_from_dict(steps, current)
            name = line[len("  - name: ") :].strip()
            current = {"name": name, "deps": []}
            idx += 1
            continue
        if current is None:
            idx += 1
            continue

        if line.startswith("    deps:"):
            idx += 1
            while idx < len(lines) and lines[idx].startswith("      - "):
                dep = lines[idx][len("      - ") :].strip()
                current["deps"].append(dep)
                idx += 1
            continue

        if line.startswith("    when: "):
            value = line[len("    when: ") :].strip()
            current["when"] = None if value == "null" else value
            idx += 1
            continue

        if line.startswith("    condition: "):
            value = line[len("    condition: ") :].strip()
            if value == "null":
                current["condition"] = None
            else:
                current["condition"] = value == "true"
            idx += 1
            continue

        if line.startswith("    is_branch: "):
            value = line[len("    is_branch: ") :].strip()
            current["is_branch"] = value == "true"
            idx += 1
            continue

        if line.startswith("    code: |-"):
            idx += 1
            code_lines: list[str] = []
            while idx < len(lines) and lines[idx].startswith("      "):
                code_lines.append(lines[idx][6:])
                idx += 1
            current["code"] = "\n".join(code_lines)
            continue
        if line.startswith("    status: "):
            value = line[len("    status: ") :].strip()
            current["status"] = None if value == "null" else value
            idx += 1
            continue
        if line.startswith("    duration_ms: "):
            value = line[len("    duration_ms: ") :].strip()
            if value == "null":
                current["duration_ms"] = None
            else:
                try:
                    current["duration_ms"] = float(value)
                except ValueError:
                    current["duration_ms"] = None
            idx += 1
            continue
        if line.startswith("    input_lines: "):
            value = line[len("    input_lines: ") :].strip()
            if value == "null":
                current["input_lines"] = None
            else:
                try:
                    current["input_lines"] = int(value)
                except ValueError:
                    current["input_lines"] = None
            idx += 1
            continue
        if line.startswith("    output_lines: "):
            value = line[len("    output_lines: ") :].strip()
            if value == "null":
                current["output_lines"] = None
            else:
                try:
                    current["output_lines"] = int(value)
                except ValueError:
                    current["output_lines"] = None
            idx += 1
            continue
        if line.startswith("    output: |-"):
            idx += 1
            output_lines: list[str] = []
            while idx < len(lines) and lines[idx].startswith("      "):
                output_lines.append(lines[idx][6:])
                idx += 1
            current["output"] = "\n".join(output_lines)
            continue
        if line.startswith("    result: |-"):
            idx += 1
            result_lines: list[str] = []
            while idx < len(lines) and lines[idx].startswith("      "):
                result_lines.append(lines[idx][6:])
                idx += 1
            current["result"] = "\n".join(result_lines)
            continue

        idx += 1

    if current is not None:
        _add_step_from_dict(steps, current)

    return steps


def _add_step_from_dict(steps: dict[str, Step], data: dict[str, object]) -> None:
    name = str(data["name"])
    deps = [str(dep) for dep in data.get("deps", [])]
    when = data.get("when")
    condition = data.get("condition")
    is_branch = bool(data.get("is_branch", False))
    code = data.get("code")
    output = data.get("output")
    result = data.get("result")
    status = data.get("status")
    duration_ms = data.get("duration_ms")
    input_lines = data.get("input_lines")
    output_lines = data.get("output_lines")

    def _noop() -> None:
        return None

    steps[name] = Step(
        name=name,
        func=_noop,
        deps=deps,
        when=when if isinstance(when, str) else None,
        condition=condition if isinstance(condition, bool) else None,
        is_branch=is_branch,
        code=code if isinstance(code, str) else None,
        output=output if isinstance(output, str) else None,
        result=result if isinstance(result, str) else None,
        status=status if isinstance(status, str) else None,
        duration_ms=duration_ms if isinstance(duration_ms, (int, float)) else None,
        input_lines=input_lines if isinstance(input_lines, int) else None,
        output_lines=output_lines if isinstance(output_lines, int) else None,
    )


def _safe_to_string(value: object) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, indent=2)
        return str(value)
    except Exception:  # noqa: BLE001
        return "<resultado indisponivel>"
