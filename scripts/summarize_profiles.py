#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def summarize_numeric_series(values: list[float]) -> dict[str, float | int]:
    return {
        "average": mean(values),
        "count": len(values),
        "min": min(values),
        "max": max(values),
    }


def summarize_counter(counter: Counter[str]) -> dict[str, int]:
    return dict(sorted(counter.items()))


def summarize_mapping(records: list[dict[str, Any]], key: str) -> dict[str, dict[str, float | int]]:
    per_field: dict[str, list[float]] = defaultdict(list)
    for record in records:
        mapping = record.get(key, {})
        for field, value in mapping.items():
            if isinstance(value, (int, float)):
                per_field[field].append(float(value))

    return {field: summarize_numeric_series(values) for field, values in sorted(per_field.items())}


def summarize_operation_counters(records: list[dict[str, Any]]) -> dict[str, dict[str, dict[str, float | int]]]:
    per_operation: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    for record in records:
        for operation, metrics in record.get("operation_counters", {}).items():
            for metric_name, value in metrics.items():
                if isinstance(value, (int, float)):
                    per_operation[operation][metric_name].append(float(value))

    return {
        operation: {
            metric_name: summarize_numeric_series(values)
            for metric_name, values in sorted(metrics.items())
        }
        for operation, metrics in sorted(per_operation.items())
    }


def summarize_group(records: list[dict[str, Any]]) -> dict[str, Any]:
    algorithm_counts = Counter(record.get("algorithm", "unknown") for record in records)
    rank_counts = Counter(str(record.get("rank", "unknown")) for record in records)
    world_size_counts = Counter(str(record.get("world_size", "unknown")) for record in records)
    iterations = [float(record.get("iterations", 0)) for record in records if isinstance(record.get("iterations"), (int, float))]

    return {
        "algorithm_counts": summarize_counter(algorithm_counts),
        "rank_counts": summarize_counter(rank_counts),
        "world_size_counts": summarize_counter(world_size_counts),
        "iterations": summarize_numeric_series(iterations),
        "operation_call_counts": summarize_mapping(records, "operation_call_counts"),
        "global_operation_call_counts": summarize_mapping(records, "global_operation_call_counts"),
        "operation_counters": summarize_operation_counters(records),
    }


def build_summary(records_by_node_type: dict[str, list[tuple[str, dict[str, Any]]]], node_type: str) -> dict[str, Any]:
    typed_records = records_by_node_type.get(node_type, [])
    overall_records = [record for _, record in typed_records]

    return {
        "type": node_type,
        "operational_counters": summarize_group(overall_records),
        "local_data_movement_costs": summarize_mapping(overall_records, "data_movement_bytes"),
    }


def load_records(input_pattern: str) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    records_by_node_type: dict[str, list[tuple[str, dict[str, Any]]]] = defaultdict(list)

    for path in sorted(Path().glob(input_pattern)):
        if not path.is_file():
            continue
        with path.open("r", encoding="utf-8") as handle:
            record = json.load(handle)
        node_type = record.get("node_type", "unknown")
        records_by_node_type[node_type].append((str(path), record))

    return records_by_node_type


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2, sort_keys=True)
        handle.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize Grudon profiler JSON outputs.")
    parser.add_argument(
        "--input-pattern",
        default="output/run_*/*.json",
        help="Glob pattern for profiler JSON files relative to the workspace root.",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory where summary JSON files will be written.",
    )
    args = parser.parse_args()

    records_by_node_type = load_records(args.input_pattern)
    output_dir = Path(args.output_dir)

    for node_type in ("compute", "memory"):
        summary = build_summary(records_by_node_type, node_type)
        write_summary(output_dir / f"profile_summary_{node_type}.json", summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
