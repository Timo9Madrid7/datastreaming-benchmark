from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class RunSpec:
    scenario: str
    tech: str
    run: str


SCENARIO_DURATION_RE = re.compile(r"(?P<duration>\d+)s(?:$|[^0-9A-Za-z])")
LOGS_ROOT_GLOB = "logs*"
DEFAULT_LOGS_ROOT = "logs"

# Backwards-compatible default used across analysis code.
PATH = DEFAULT_LOGS_ROOT


def discover_log_roots(
    base_dir: str | Path = ".",
    pattern: str = LOGS_ROOT_GLOB,
) -> list[str]:
    """Discover available experiment log roots (directories) under `base_dir`.

    Typical examples: logs/, logs_FLAT_latest/, logs_COMPLEX_v0/, ...
    """
    base = Path(base_dir)
    if not base.exists():
        return [DEFAULT_LOGS_ROOT]

    roots = sorted(
        {
            p.name
            for p in base.glob(pattern)
            if p.is_dir() and not p.name.startswith(".")
        }
    )

    # Prefer the conventional default if present.
    if DEFAULT_LOGS_ROOT in roots:
        return roots
    if (base / DEFAULT_LOGS_ROOT).is_dir():
        return [DEFAULT_LOGS_ROOT, *roots]

    return roots or [DEFAULT_LOGS_ROOT]

def infer_duration_seconds_from_logs(
    scenario: str, logs_root: str | Path = PATH
) -> int | None:
    """Only for sidebar labels in plots. Not for actual calculation."""
    scenario_dir = Path(logs_root) / scenario
    if not scenario_dir.exists():
        return None

    for tech_dir in sorted(
        p for p in scenario_dir.iterdir() if p.is_dir() and not p.name.startswith(".")
    ):
        for run_dir in sorted(
            p for p in tech_dir.iterdir() if p.is_dir() and not p.name.startswith(".")
        ):
            for item in sorted(run_dir.iterdir()):
                if not item.is_file():
                    continue
                match = SCENARIO_DURATION_RE.search(
                    item.stem
                ) or SCENARIO_DURATION_RE.search(item.name)
                if match:
                    return int(match.group("duration"))
    return None


def discover_scenarios(
    logs_root: str | Path = PATH,
) -> dict[str, dict[str, list[str]]]:
    root = Path(logs_root)
    if not root.exists():
        return {}
    scenarios: dict[str, dict[str, list[str]]] = {}
    for scenario_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        techs: dict[str, list[str]] = {}
        for tech_dir in sorted(
            p
            for p in scenario_dir.iterdir()
            if p.is_dir() and not p.name.startswith(".")
        ):
            runs = sorted(p.name for p in tech_dir.iterdir() if p.is_dir())
            if runs:
                techs[tech_dir.name] = runs
        if techs:
            scenarios[scenario_dir.name] = techs
    return scenarios


def iter_run_specs(
    scenario: str,
    techs: Iterable[str],
    runs_by_tech: dict[str, list[str]],
) -> list[RunSpec]:
    specs: list[RunSpec] = []
    for tech in techs:
        for run in runs_by_tech.get(tech, []):
            specs.append(RunSpec(scenario=scenario, tech=tech, run=run))
    return specs


def get_run_dir(
    scenario: str, tech: str, run: str, logs_root: str | Path = PATH
) -> Path:
    return Path(logs_root) / scenario / tech / run


def get_cache_dir(scenario: str, logs_root: str | Path = PATH) -> Path:
    cache_dir = Path(logs_root) / scenario / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir
