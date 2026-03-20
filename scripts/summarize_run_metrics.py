#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


OPS = [
    "host_prepare_frontier",
    "host_send_updates_to_remote",
    "host_receive_remote_updates",
    "host_update_frontier",
    "remote_apply_host_updates",
    "remote_generate_updates",
    "remote_send_updates_to_host",
]

PHASES = OPS


def safe_div(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator else 0.0


def enabled_events(record: dict[str, object]) -> set[str]:
    value = record.get("events_enabled", []) or []
    if isinstance(value, str):
        if not value:
            return set()
        return {item for item in value.replace(",", "+").split("+") if item}
    return set(value)


def metric_available(record: dict[str, object], metric_name: str) -> bool:
    events = enabled_events(record)
    if metric_name in {
        "instructions_total",
        "cycles_total",
        "instructions_per_iter",
        "cycles_per_iter",
        "ipc",
    }:
        return "instructions" in events and "cycles" in events
    if metric_name in {"cache_misses_total", "cache_misses_per_iter"}:
        return "cache-misses" in events
    if metric_name in {"dtlb_loads_total", "dtlb_loads_per_iter"}:
        return "dTLB-loads" in events
    if metric_name in {"l3_misses_total", "l3_misses_per_iter"}:
        return "mem_load_uops_retired.l3_miss" in events
    return True


def load_records(root: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for path in sorted(root.glob("output/run_*/*.json")):
        if not path.is_file():
            continue
        with path.open("r", encoding="utf-8") as handle:
            record = json.load(handle)

        iterations = float(record.get("iterations", 0) or 0)
        counters = record.get("operation_counters", {}) or {}
        data_movement = record.get("data_movement_bytes", {}) or {}

        def metric_total(metric_name: str) -> float:
            return sum(float(counters.get(operation, {}).get(metric_name, 0.0) or 0.0) for operation in OPS)

        instructions = metric_total("instructions")
        cycles = metric_total("cycles")
        cache_misses = metric_total("cache-misses")
        dtlb_loads = metric_total("dTLB-loads")
        l3_misses = metric_total("mem_load_uops_retired.l3_miss")
        total_local = float(data_movement.get("total_local", 0.0) or 0.0)
        host_to_remote = float(data_movement.get("host_to_remote", 0.0) or 0.0)
        remote_to_host = float(data_movement.get("remote_to_host", 0.0) or 0.0)

        instructions_total = instructions if metric_available(record, "instructions_total") else None
        cycles_total = cycles if metric_available(record, "cycles_total") else None
        cache_misses_total = cache_misses if metric_available(record, "cache_misses_total") else None
        dtlb_loads_total = dtlb_loads if metric_available(record, "dtlb_loads_total") else None
        l3_misses_total = l3_misses if metric_available(record, "l3_misses_total") else None

        instructions_per_iter = safe_div(instructions, iterations) if metric_available(record, "instructions_per_iter") else None
        cycles_per_iter = safe_div(cycles, iterations) if metric_available(record, "cycles_per_iter") else None
        cache_misses_per_iter = safe_div(cache_misses, iterations) if metric_available(record, "cache_misses_per_iter") else None
        dtlb_loads_per_iter = safe_div(dtlb_loads, iterations) if metric_available(record, "dtlb_loads_per_iter") else None
        l3_misses_per_iter = safe_div(l3_misses, iterations) if metric_available(record, "l3_misses_per_iter") else None
        ipc = safe_div(instructions, cycles) if metric_available(record, "ipc") else None

        records.append(
            {
                "folder": path.parent.name,
                "file": path.name,
                "algorithm": record.get("algorithm", ""),
                "rank": record.get("rank", ""),
                "node_type": record.get("node_type", ""),
                "events_enabled": "+".join(record.get("events_enabled", []) or []),
                "operation_counters": counters,
                "iterations": iterations,
                "total_local_bytes": total_local,
                "host_to_remote_bytes": host_to_remote,
                "remote_to_host_bytes": remote_to_host,
                "instructions_total": instructions_total,
                "cycles_total": cycles_total,
                "cache_misses_total": cache_misses_total,
                "dtlb_loads_total": dtlb_loads_total,
                "l3_misses_total": l3_misses_total,
                "instructions_per_iter": instructions_per_iter,
                "cycles_per_iter": cycles_per_iter,
                "cache_misses_per_iter": cache_misses_per_iter,
                "dtlb_loads_per_iter": dtlb_loads_per_iter,
                "l3_misses_per_iter": l3_misses_per_iter,
                "ipc": ipc,
                "total_local_per_iter": safe_div(total_local, iterations),
                "host_to_remote_per_iter": safe_div(host_to_remote, iterations),
                "remote_to_host_per_iter": safe_div(remote_to_host, iterations),
            }
        )

    return records


def phase_metric(record: dict[str, object], phase: str, metric_name: str) -> float:
    counters = record.get("operation_counters", {}) or {}
    return float(counters.get(phase, {}).get(metric_name, 0.0) or 0.0)


def phase_value_per_iter(record: dict[str, object], phase: str, metric_name: str) -> float | None:
    if not metric_available(record, f"{metric_name}_per_iter"):
        return None

    iterations = float(record.get("iterations", 0) or 0)
    return safe_div(phase_metric(record, phase, metric_name), iterations)


def numeric_fields() -> list[str]:
    return [
        "iterations",
        "total_local_bytes",
        "host_to_remote_bytes",
        "remote_to_host_bytes",
        "instructions_total",
        "cycles_total",
        "cache_misses_total",
        "dtlb_loads_total",
        "l3_misses_total",
        "instructions_per_iter",
        "cycles_per_iter",
        "cache_misses_per_iter",
        "dtlb_loads_per_iter",
        "l3_misses_per_iter",
        "ipc",
        "total_local_per_iter",
        "host_to_remote_per_iter",
        "remote_to_host_per_iter",
    ]


def write_csv(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "folder",
        "file",
        "algorithm",
        "rank",
        "node_type",
        "events_enabled",
        *numeric_fields(),
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow(record)


def write_phase_csv(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "repetition",
        "phase",
        "node_type",
        "count",
        "iterations_avg",
        "instructions_per_iter_avg",
        "cycles_per_iter_avg",
        "cache_misses_per_iter_avg",
        "dtlb_loads_per_iter_avg",
        "l3_misses_per_iter_avg",
        "ipc_avg",
    ]
    rows = summarize_by_phase(records)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def average(values: list[float]) -> float | None:
    filtered = [value for value in values if value is not None]
    return sum(filtered) / len(filtered) if filtered else None


def format_value(value: object, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def summarize(records: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for record in records:
        grouped[(str(record["folder"]), str(record["node_type"]))].append(record)

    summaries: list[dict[str, object]] = []
    for (folder, node_type), group in sorted(grouped.items()):
        row: dict[str, object] = {
            "folder": folder,
            "node_type": node_type,
            "count": len(group),
        }
        for field in numeric_fields():
            row[f"{field}_avg"] = average([item[field] for item in group])
        summaries.append(row)

    overall = {
        "folder": "overall",
        "node_type": "all",
        "count": len(records),
    }
    for field in numeric_fields():
        overall[f"{field}_avg"] = average([item[field] for item in records])
    summaries.append(overall)

    return summaries


def summarize_by_repetition(records: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for record in records:
        folder = str(record["folder"])
        repetition_name = "_".join(folder.split("_", 2)[:2])
        grouped[(repetition_name, str(record["node_type"]))].append(record)

    rows: list[dict[str, object]] = []
    for repetition in ("run_0", "run_1", "run_2"):
        row: dict[str, object] = {"repetition": repetition}
        for node_type in ("compute", "memory"):
            group = grouped.get((repetition, node_type), [])
            prefix = f"{node_type}_"
            row[f"{prefix}count"] = len(group)
            for field in (
                "total_local_per_iter",
                "instructions_per_iter",
                "cycles_per_iter",
                "cache_misses_per_iter",
                "dtlb_loads_per_iter",
                "l3_misses_per_iter",
                "ipc",
            ):
                row[f"{prefix}{field}_avg"] = average([item[field] for item in group])
        rows.append(row)

    return rows


def summarize_by_phase(records: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for phase in PHASES:
        for node_type in ("compute", "memory"):
            group = [record for record in records if str(record["node_type"]) == node_type]
            row: dict[str, object] = {
                "repetition": "all",
                "phase": phase,
                "node_type": node_type,
                "count": len(group),
                "iterations_avg": average([record["iterations"] for record in group]),
                "instructions_per_iter_avg": average([phase_value_per_iter(record, phase, "instructions") for record in group]),
                "cycles_per_iter_avg": average([phase_value_per_iter(record, phase, "cycles") for record in group]),
                "cache_misses_per_iter_avg": average([phase_value_per_iter(record, phase, "cache-misses") for record in group]),
                "dtlb_loads_per_iter_avg": average([phase_value_per_iter(record, phase, "dTLB-loads") for record in group]),
                "l3_misses_per_iter_avg": average([phase_value_per_iter(record, phase, "mem_load_uops_retired.l3_miss") for record in group]),
                "ipc_avg": average([
                    safe_div(phase_metric(record, phase, "instructions"), phase_metric(record, phase, "cycles"))
                    if metric_available(record, "ipc") and phase_metric(record, phase, "cycles")
                    else None
                    for record in group
                ]),
            }
            rows.append(row)

    return rows


def write_markdown(path: Path, records: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summaries = summarize(records)
    repetition_rows = summarize_by_repetition(records)
    phase_rows = summarize_by_phase(records)

    with path.open("w", encoding="utf-8") as handle:
        handle.write("# Grudon Run Metrics Summary\n\n")
        handle.write("Note: `N/A` means the corresponding perf event was not collected for that run, so the metric is excluded from the average instead of being treated as zero.\n\n")
        handle.write("## Per-folder averages\n\n")
        handle.write(
            "| folder | node_type | count | iterations_avg | total_local_per_iter_avg | instructions_per_iter_avg | cycles_per_iter_avg | cache_misses_per_iter_avg | dtlb_loads_per_iter_avg | l3_misses_per_iter_avg | ipc_avg |\n"
        )
        handle.write(
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
        )
        for row in summaries:
            if row["folder"] == "overall":
                continue
            handle.write(
                f"| {row['folder']} | {row['node_type']} | {row['count']} | {format_value(row['iterations_avg'])} | {format_value(row['total_local_per_iter_avg'])} | {format_value(row['instructions_per_iter_avg'])} | {format_value(row['cycles_per_iter_avg'])} | {format_value(row['cache_misses_per_iter_avg'])} | {format_value(row['dtlb_loads_per_iter_avg'])} | {format_value(row['l3_misses_per_iter_avg'])} | {format_value(row['ipc_avg'], 3)} |\n"
            )

        handle.write("\n## Repetition comparison\n\n")
        handle.write(
            "| repetition | compute_count | memory_count | compute_total_local_per_iter_avg | memory_total_local_per_iter_avg | compute_instructions_per_iter_avg | memory_instructions_per_iter_avg | compute_cycles_per_iter_avg | memory_cycles_per_iter_avg | compute_cache_misses_per_iter_avg | memory_cache_misses_per_iter_avg | compute_dtlb_loads_per_iter_avg | memory_dtlb_loads_per_iter_avg | compute_l3_misses_per_iter_avg | memory_l3_misses_per_iter_avg | compute_ipc_avg | memory_ipc_avg |\n"
        )
        handle.write(
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
        )
        for row in repetition_rows:
            handle.write(
                f"| {row['repetition']} | {format_value(row['compute_count'])} | {format_value(row['memory_count'])} | {format_value(row['compute_total_local_per_iter_avg'])} | {format_value(row['memory_total_local_per_iter_avg'])} | {format_value(row['compute_instructions_per_iter_avg'])} | {format_value(row['memory_instructions_per_iter_avg'])} | {format_value(row['compute_cycles_per_iter_avg'])} | {format_value(row['memory_cycles_per_iter_avg'])} | {format_value(row['compute_cache_misses_per_iter_avg'])} | {format_value(row['memory_cache_misses_per_iter_avg'])} | {format_value(row['compute_dtlb_loads_per_iter_avg'])} | {format_value(row['memory_dtlb_loads_per_iter_avg'])} | {format_value(row['compute_l3_misses_per_iter_avg'])} | {format_value(row['memory_l3_misses_per_iter_avg'])} | {format_value(row['compute_ipc_avg'], 3)} | {format_value(row['memory_ipc_avg'], 3)} |\n"
            )

        handle.write("\n## Phase comparison\n\n")
        handle.write(
            "| phase | compute_count | memory_count | compute_instructions_per_iter_avg | memory_instructions_per_iter_avg | compute_cycles_per_iter_avg | memory_cycles_per_iter_avg | compute_cache_misses_per_iter_avg | memory_cache_misses_per_iter_avg | compute_dtlb_loads_per_iter_avg | memory_dtlb_loads_per_iter_avg | compute_l3_misses_per_iter_avg | memory_l3_misses_per_iter_avg | compute_ipc_avg | memory_ipc_avg |\n"
        )
        handle.write(
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
        )
        for phase in PHASES:
            compute_row = next(row for row in phase_rows if row["phase"] == phase and row["node_type"] == "compute")
            memory_row = next(row for row in phase_rows if row["phase"] == phase and row["node_type"] == "memory")
            handle.write(
                f"| {phase} | {format_value(compute_row['count'])} | {format_value(memory_row['count'])} | {format_value(compute_row['instructions_per_iter_avg'])} | {format_value(memory_row['instructions_per_iter_avg'])} | {format_value(compute_row['cycles_per_iter_avg'])} | {format_value(memory_row['cycles_per_iter_avg'])} | {format_value(compute_row['cache_misses_per_iter_avg'])} | {format_value(memory_row['cache_misses_per_iter_avg'])} | {format_value(compute_row['dtlb_loads_per_iter_avg'])} | {format_value(memory_row['dtlb_loads_per_iter_avg'])} | {format_value(compute_row['l3_misses_per_iter_avg'])} | {format_value(memory_row['l3_misses_per_iter_avg'])} | {format_value(compute_row['ipc_avg'], 3)} | {format_value(memory_row['ipc_avg'], 3)} |\n"
            )

        overall = next(row for row in summaries if row["folder"] == "overall")
        handle.write("\n## Overall averages\n\n")
        handle.write(
            "| scope | count | iterations_avg | total_local_per_iter_avg | instructions_per_iter_avg | cycles_per_iter_avg | cache_misses_per_iter_avg | dtlb_loads_per_iter_avg | l3_misses_per_iter_avg | ipc_avg |\n"
        )
        handle.write(
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n"
        )
        handle.write(
            f"| all runs | {format_value(overall['count'])} | {format_value(overall['iterations_avg'])} | {format_value(overall['total_local_per_iter_avg'])} | {format_value(overall['instructions_per_iter_avg'])} | {format_value(overall['cycles_per_iter_avg'])} | {format_value(overall['cache_misses_per_iter_avg'])} | {format_value(overall['dtlb_loads_per_iter_avg'])} | {format_value(overall['l3_misses_per_iter_avg'])} | {format_value(overall['ipc_avg'], 3)} |\n"
        )


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    records = load_records(root)
    write_csv(root / "output" / "run_metrics_summary.csv", records)
    write_phase_csv(root / "output" / "run_phase_summary.csv", records)
    write_markdown(root / "output" / "run_metrics_summary.md", records)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())