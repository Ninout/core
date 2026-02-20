from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable


@dataclass
class Step:
    name: str
    func: Callable[..., object]
    deps: list[str] = field(default_factory=list)
    when: str | None = None
    condition: bool | None = None
    is_branch: bool = False
    code: str | None = None
    output: str | None = None
    result: str | None = None
    status: str | None = None
    duration_ms: float | None = None
    input_lines: int | None = None
    output_lines: int | None = None
