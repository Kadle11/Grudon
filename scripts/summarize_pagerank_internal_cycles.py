#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

PHASE_PREFIX: dict[str, str] = {
    "apply_updates": "pr_apply_",
    "gen_updates": "pr_gen_",
    "update_frontier": "pr_frontier_",
}

PHASE_NODE_TYPE: dict[str, str] = {
    "apply_updates": "compute",
    "update_frontier": "compute",
    "gen_updates": "memory",
}


def load_records(input_pattern: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(Path().glob(input_pattern)):
        if not path.is_file():
            continue
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue

        if record.get("algorithm") != "PageRank":
            continue

        record["_path"] = str(path)
        records.append(record)

    return records


def to_float(value: Any) -> float:
    return float(value) if isinstance(value, (int, float)) else 0.0


def summarize_phase(records: list[dict[str, Any]], phase: str) -> dict[str, Any]:
    expected_node_type = PHASE_NODE_TYPE[phase]
    op_prefix = PHASE_PREFIX[phase]

    op_total_cycles: dict[str, list[float]] = defaultdict(list)
    op_per_iter_cycles: dict[str, list[float]] = defaultdict(list)
    used_records = 0

    for record in records:
        if record.get("node_type") != expected_node_type:
            continue

        operation_counters = record.get("operation_counters", {}) or {}
        iterations = to_float(record.get("iterations"))
        used_records += 1

        for op in operation_counters.keys():
            if not str(op).startswith(op_prefix):
                continue
            op_cycles = to_float((operation_counters.get(op, {}) or {}).get("cycles"))
            op_total_cycles[op].append(op_cycles)
            op_per_iter_cycles[op].append(op_cycles / iterations if iterations > 0 else 0.0)

    rows: list[dict[str, Any]] = []
    for op in sorted(op_total_cycles.keys()):
        totals = op_total_cycles.get(op, [])
        per_iters = op_per_iter_cycles.get(op, [])

        if not totals:
            continue

        rows.append(
            {
                "operation": op,
                "avg_total_cycles": sum(totals) / len(totals),
                "avg_cycles_per_iter": sum(per_iters) / len(per_iters),
                "min_total_cycles": min(totals),
                "max_total_cycles": max(totals),
            }
        )

    rows.sort(key=lambda item: item["avg_total_cycles"], reverse=True)

    return {
        "phase": phase,
        "node_type": expected_node_type,
        "records_considered": used_records,
        "operations_ranked": len(rows),
        "top_operations": rows[:3],
    }


def build_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    phases = [summarize_phase(records, phase) for phase in ("apply_updates", "gen_updates", "update_frontier")]
    return {
        "records_total": len(records),
        "phases": phases,
    }


def write_json(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def format_cycles(value: float) -> str:
    return f"{value:,.2f}"


def write_markdown(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("# PageRank Internal Top-3 Cycle Cost")
    lines.append("")
    lines.append(f"Records analyzed: {summary.get('records_total', 0)}")
    lines.append("")

    for phase_summary in summary.get("phases", []):
        phase = phase_summary.get("phase", "")
        node_type = phase_summary.get("node_type", "")
        records_considered = phase_summary.get("records_considered", 0)
        operations_ranked = phase_summary.get("operations_ranked", 0)
        lines.append(f"## Phase: {phase} ({node_type})")
        lines.append("")
        lines.append(f"Records considered: {records_considered}")
        lines.append(f"Operations ranked: {operations_ranked}")
        lines.append("")
        lines.append("| rank | operation | avg_total_cycles | avg_cycles_per_iter | min_total_cycles | max_total_cycles |")
        lines.append("| --- | --- | ---: | ---: | ---: | ---: |")

        top_ops = phase_summary.get("top_operations", [])
        if not top_ops:
            lines.append("| - | no-data | 0.00 | 0.00 | 0.00 | 0.00 |")
        else:
            for index, op in enumerate(top_ops, start=1):
                lines.append(
                    "| "
                    + f"{index} | {op.get('operation', '')} | "
                    + f"{format_cycles(to_float(op.get('avg_total_cycles')))} | "
                    + f"{format_cycles(to_float(op.get('avg_cycles_per_iter')))} | "
                    + f"{format_cycles(to_float(op.get('min_total_cycles')))} | "
                    + f"{format_cycles(to_float(op.get('max_total_cycles')))} |"
                )

        lines.append("")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize PageRank internal operation cycle costs.")
    parser.add_argument(
        "--input-pattern",
        default="output/pr_internal/run_*/*.json",
        help="Glob pattern for PageRank profiler JSON files.",
    )
    parser.add_argument(
        "--output-json",
        default="output/pr_internal/pagerank_internal_top3_cycles.json",
        help="Path to output JSON summary.",
    )
    parser.add_argument(
        "--output-markdown",
        default="output/pr_internal/pagerank_internal_top3_cycles.md",
        help="Path to output Markdown summary.",
    )
    args = parser.parse_args()

    records = load_records(args.input_pattern)
    summary = build_summary(records)

    write_json(Path(args.output_json), summary)
    write_markdown(Path(args.output_markdown), summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
