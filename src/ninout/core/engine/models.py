from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Literal, TypeAlias

Row: TypeAlias = dict[str, object]
StepPayload: TypeAlias = Row | list[Row]
StepResult: TypeAlias = StepPayload | bool
StepMode: TypeAlias = Literal["task", "row", "sql"]


@dataclass
class Step:
    name: str
    func: Callable[..., StepResult]
    deps: list[str] = field(default_factory=list)
    when: str | None = None
    condition: bool | None = None
    is_branch: bool = False
    mode: StepMode = "task"
    code: str | None = None
    output: str | None = None
    result: str | None = None
    status: str | None = None
    duration_ms: float | None = None
    input_lines: int | None = None
    output_lines: int | None = None
    throughput_in_lps: float | None = None
    throughput_out_lps: float | None = None
    disabled_deps: list[str] = field(default_factory=list)
    disabled_self: bool = False
