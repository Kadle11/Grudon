#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass


HEADER_RE = re.compile(r'^\s+(\d+)\s+([^:]+).*:$')
CALLCHAIN_RE = re.compile(r'^\s+[0-9a-fA-F]+\s+(.+)$')
FILENAME_RE = re.compile(r'^bench_(?P<algo>[^_]+)_(?P<dataset>.+)_run(?P<run>\d+)\.data$')


@dataclass
class FuncStats:
    cycles: float = 0.0
    instructions: float = 0.0


def canonical_function_name(symbol: str, algo: str) -> str | None:
    if symbol.startswith("run_"):
        return "run_algo"
    if symbol.startswith("generate_updates_"):
        return "generate_updates_algo"
    if symbol.startswith("update_frontier_"):
        return f"update_frontier_{algo}"
    if symbol.startswith("apply_updates_"):
        return "apply_updates_algo"
    return None


def parse_file(path: str) -> dict[str, FuncStats]:
    cmd = ["sudo", "perf", "script", "-i", path, "-F", "event,period,ip,sym"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    current_event = None
    current_period = 0
    current_stack: list[str] = []
    per_file: dict[str, FuncStats] = defaultdict(FuncStats)

    def commit_sample() -> None:
        nonlocal current_event, current_period, current_stack
        if not current_event or not current_stack:
            return

        ev = current_event.lower()
        cycles = float(current_period) if "cycle" in ev else 0.0
        instructions = float(current_period) if ("instruction" in ev or "insn" in ev) else 0.0
        if cycles == 0.0 and instructions == 0.0:
            return

        for sym in set(current_stack):
            stats = per_file[sym]
            stats.cycles += cycles
            stats.instructions += instructions

    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip()
        if not line:
            continue

        header_match = HEADER_RE.match(line)
        if header_match:
                        commit_sample()
                        current_period = int(header_match.group(1))
                        current_event = header_match.group(2).strip()
                        current_stack = []
                        continue

        call_match = CALLCHAIN_RE.match(line)
        if call_match:
                        current_stack.append(call_match.group(1).strip())

    commit_sample()
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"perf script failed for {path}: {stderr.strip() or stdout.strip()}")

    return per_file


def normalize_graph_name(dataset: str) -> str:
    return dataset.replace("_", "-")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize averaged perf callchain stats across bench_*.data files.")
    parser.add_argument("--input-dir", default="/mydata/Grudon/src-c-no-shared-pool", help="Directory containing bench_*.data files")
    parser.add_argument("--csv-out", default="/mydata/Grudon/src-c-no-shared-pool/perf_summary.csv", help="CSV output path")
    parser.add_argument("--md-out", default="/mydata/Grudon/src-c-no-shared-pool/perf_summary.md", help="Markdown output path")
    parser.add_argument("--top-n", type=int, default=7, help="Number of top functions per algorithm/dataset group")
    args = parser.parse_args()

    files = sorted(glob.glob(os.path.join(args.input_dir, "bench_*.data")))
    if not files:
        print(f"No bench_*.data files found in {args.input_dir}", file=sys.stderr)
        return 1

    groups: dict[tuple[str, str], list[str]] = defaultdict(list)
    for path in files:
        name = os.path.basename(path)
        match = FILENAME_RE.match(name)
        if not match:
            continue
        algo = match.group("algo")
        dataset = normalize_graph_name(match.group("dataset"))
        groups[(algo, dataset)].append(path)

    all_rows: list[dict[str, object]] = []
    markdown_sections: list[str] = []

    for (algo, dataset) in sorted(groups.keys()):
        run_files = sorted(groups[(algo, dataset)])
        if not run_files:
            continue

        per_run: list[dict[str, FuncStats]] = []
        for path in run_files:
            per_run.append(parse_file(path))

        aggregate: dict[str, FuncStats] = defaultdict(FuncStats)
        for run_stats in per_run:
            for func, stats in run_stats.items():
                canonical = canonical_function_name(func, algo)
                if canonical is None:
                    continue
                aggregate[canonical].cycles += stats.cycles
                aggregate[canonical].instructions += stats.instructions

        run_count = float(len(per_run))
        canonical_order = [
            "run_algo",
            "generate_updates_algo",
            f"update_frontier_{algo}",
            "apply_updates_algo",
        ]
        ranked = sorted(
            ((func, aggregate[func]) for func in canonical_order if func in aggregate),
            key=lambda item: (-item[1].cycles / run_count, -item[1].instructions / run_count, item[0]),
        )[: args.top_n]

        markdown_sections.append(f"## {algo} / {dataset}\n")
        markdown_sections.append("| Rank | Function | Avg Cycles | Avg Instructions | IPC |")
        markdown_sections.append("| --- | --- | ---: | ---: | ---: |")

        for rank, (func, stats) in enumerate(ranked, start=1):
            avg_cycles = stats.cycles / run_count
            avg_instructions = stats.instructions / run_count
            ipc = (avg_instructions / avg_cycles) if avg_cycles else 0.0

            row = {
                "algorithm": algo,
                "dataset": dataset,
                "rank": rank,
                "function": func,
                "avg_cycles": avg_cycles,
                "avg_instructions": avg_instructions,
                "ipc": ipc,
            }
            all_rows.append(row)

            markdown_sections.append(
                f"| {rank} | {func} | {avg_cycles:,.0f} | {avg_instructions:,.0f} | {ipc:.2f} |"
            )

        markdown_sections.append("")

    with open(args.csv_out, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["algorithm", "dataset", "rank", "function", "avg_cycles", "avg_instructions", "ipc"])
        for row in all_rows:
            writer.writerow([
                row["algorithm"],
                row["dataset"],
                row["rank"],
                row["function"],
                f"{row['avg_cycles']:.0f}",
                f"{row['avg_instructions']:.0f}",
                f"{row['ipc']:.6f}",
            ])

    with open(args.md_out, "w") as md_file:
        md_file.write("# Perf Summary\n\n")
        md_file.write("Averaged inclusive stats from bench_*.data files, grouped by algorithm then dataset.\n\n")
        md_file.write("\n".join(markdown_sections))
        md_file.write("\n")

    print(f"Wrote {len(all_rows)} rows to {args.csv_out}")
    print(f"Wrote markdown summary to {args.md_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())