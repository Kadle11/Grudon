#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from html import escape
from pathlib import Path


COMPUTE_OPERATIONS = [
    "host_prepare_frontier",
    "host_send_updates_to_remote",
    "host_receive_remote_updates",
    "host_update_frontier",
]

MEMORY_OPERATIONS = [
    "remote_apply_host_updates",
    "remote_generate_updates",
    "remote_send_updates_to_host",
]

NODE_TYPE_COLORS = {
    "compute": "#2563eb",
    "memory": "#f97316",
}

NODE_TYPE_LABELS = {
    "compute": "Compute",
    "memory": "Memory",
}

METRIC_COLUMNS = [
    ("instructions", "Instructions\n/iter", "instr/iter"),
    ("cycles", "Cycles\n/iter", "cycles/iter"),
    ("cache-misses", "Cache misses\n/iter", "misses/iter"),
    ("dTLB-loads", "dTLB loads\n/iter", "loads/iter"),
    ("mem_load_uops_retired.l3_miss", "L3 misses\n/iter", "misses/iter"),
    ("mem_uops_retired.all_loads", "Mem loads\n/iter", "loads/iter"),
    ("mem_uops_retired.all_stores", "Mem stores\n/iter", "stores/iter"),
    ("ipc", "IPC", ""),
]


METRIC_SERIES = [
    ("instructions", "Instructions", "#1d4ed8"),
    ("cycles", "Cycles", "#059669"),
    ("mem_uops_retired.all_loads", "Mem loads", "#d97706"),
    ("mem_uops_retired.all_stores", "Mem stores", "#7c3aed"),
]
SENSITIVITY_PHASE_GROUPS = [
    ("host_prepare_frontier", ["host_prepare_frontier"]),
    ("host_send_updates_to_remote", ["host_send_updates_to_remote"]),
    ("remote_apply_host_updates", ["remote_apply_host_updates"]),
    ("host_receive_remote_updates", ["host_receive_remote_updates"]),
    ("host_update_frontier", ["host_update_frontier"]),
    ("remote_generate_updates", ["remote_generate_updates"]),
    ("remote_send_updates_to_host", ["remote_send_updates_to_host"]),
]

SENSITIVITY_LIFECYCLE_GROUPS = [
    ("Prepare Frontier + Send", ["host_prepare_frontier", "host_send_updates_to_remote"]),
    ("Receive + Apply + Update Frontier", ["host_receive_remote_updates", "remote_apply_host_updates", "host_update_frontier"]),
    ("Generate Updates + Return", ["remote_generate_updates", "remote_send_updates_to_host"]),
]

SENSITIVITY_NO_COMM_GROUPS = [
    ("Prepare Frontier", ["host_prepare_frontier"]),
    ("Apply + Update Frontier", ["remote_apply_host_updates", "host_update_frontier"]),
    ("Generate Updates", ["remote_generate_updates"]),
]

SENSITIVITY_NO_COMM_PHASES = [
    "host_prepare_frontier",
    "remote_apply_host_updates",
    "host_update_frontier",
    "remote_generate_updates",
]


@dataclass(frozen=True)
class Section:
    node_type: str
    title: str
    description: str
    operations: list[str]
    values: dict[str, object]


def load_summary(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise FileNotFoundError(f"Missing profiler summary: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def operation_average(summary: dict[str, object], operation: str, metric_name: str) -> float:
    operation_counters = summary.get("operational_counters", {}).get("operation_counters", {}) or {}
    metrics = operation_counters.get(operation, {}) or {}
    metric = metrics.get(metric_name, {})
    if not isinstance(metric, dict):
        return 0.0
    value = metric.get("average")
    return float(value) if isinstance(value, (int, float)) else 0.0


def iteration_average(summary: dict[str, object]) -> float:
    iterations = summary.get("operational_counters", {}).get("iterations", {}) or {}
    average = iterations.get("average")
    return float(average) if isinstance(average, (int, float)) and average else 1.0


def collect_metric(summary: dict[str, object], operations: list[str], metric_name: str) -> dict[str, float]:
    average_iterations = iteration_average(summary)
    return {
        operation: operation_average(summary, operation, metric_name) / average_iterations
        for operation in operations
    }


def collect_operation_metrics(summary: dict[str, object], operations: list[str]) -> dict[str, dict[str, float]]:
    average_iterations = iteration_average(summary)
    result: dict[str, dict[str, float]] = {}
    for operation in operations:
        instructions = operation_average(summary, operation, "instructions")
        cycles = operation_average(summary, operation, "cycles")
        result[operation] = {
            "instructions": instructions / average_iterations,
            "cycles": cycles / average_iterations,
            "cache-misses": operation_average(summary, operation, "cache-misses") / average_iterations,
            "dTLB-loads": operation_average(summary, operation, "dTLB-loads") / average_iterations,
            "mem_load_uops_retired.l3_miss": operation_average(summary, operation, "mem_load_uops_retired.l3_miss") / average_iterations,
            "mem_uops_retired.all_loads": operation_average(summary, operation, "mem_uops_retired.all_loads") / average_iterations,
            "mem_uops_retired.all_stores": operation_average(summary, operation, "mem_uops_retired.all_stores") / average_iterations,
            "ipc": instructions / cycles if cycles else 0.0,
        }
    return result


def collect_phase_metrics(summary: dict[str, object], operations: list[str]) -> dict[str, dict[str, float]]:
    return collect_operation_metrics(summary, operations)


def aggregate_sensitivity_groups(
    phase_metrics: dict[str, dict[str, float]],
    groups: list[tuple[str, list[str]]],
) -> list[dict[str, object]]:
    aggregated_groups: list[dict[str, object]] = []
    for group_title, operations in groups:
        group_values = {
            metric_name: sum(phase_metrics.get(operation, {}).get(metric_name, 0.0) for operation in operations)
            for metric_name, _, _ in METRIC_SERIES
        }
        aggregated_groups.append(
            {
                "title": group_title,
                "operations": operations,
                "values": group_values,
            }
        )
    return aggregated_groups


def nice_step(max_value: float, target_ticks: int = 5) -> float:
    if max_value <= 0:
        return 1.0
    raw_step = max_value / target_ticks
    magnitude = 10 ** math.floor(math.log10(raw_step))
    for factor in (1, 2, 5, 10):
        step = factor * magnitude
        if step >= raw_step:
            return step
    return 10 * magnitude


def svg_begin(
    width: int,
    height: int,
    title: str,
    subtitle: str,
    *,
    legend_entries: list[tuple[str, str]] | None = None,
    title_x: int = 330,
) -> list[str]:
    if legend_entries is None:
        legend_entries = [
            ("compute", "#2563eb"),
            ("memory", "#f97316"),
        ]
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        f'  <title id="title">{escape(title)}</title>',
        f'  <desc id="desc">{escape(subtitle)}</desc>',
        '  <rect width="100%" height="100%" fill="#ffffff"/>',
        f'  <text x="{title_x}" y="34" font-family="DejaVu Sans, Arial, sans-serif" font-size="24" font-weight="700" fill="#0f172a">{escape(title)}</text>',
    ]
    
    lines.extend([
        *[
            f'  <rect x="{title_x + index * 110}" y="68" width="14" height="14" rx="3" fill="{color}"/>'
            for index, (_, color) in enumerate(legend_entries)
        ],
        *[
            f'  <text x="{title_x + 20 + index * 110}" y="80" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" fill="#334155">{escape(label)}</text>'
            for index, (label, _) in enumerate(legend_entries)
        ],
    ])
    return lines


def clean_label(label: str) -> str:
    cleaned = label.replace("host_", "").replace("remote_", "")
    return cleaned.replace("_", " ")

def compact_number(value: float) -> str:
    absolute = abs(value)
    if absolute >= 1_000_000_000_000:
        return f"{value / 1_000_000_000_000:.2f}T"
    if absolute >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if absolute >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{value / 1_000:.2f}K"
    if absolute >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}"


def interpolate_color(low: tuple[int, int, int], high: tuple[int, int, int], ratio: float) -> str:
    ratio = max(0.0, min(1.0, ratio))
    red = round(low[0] + (high[0] - low[0]) * ratio)
    green = round(low[1] + (high[1] - low[1]) * ratio)
    blue = round(low[2] + (high[2] - low[2]) * ratio)
    return f"#{red:02x}{green:02x}{blue:02x}"


def metric_ratio(value: float, metric_max: float) -> float:
    if value <= 0 or metric_max <= 0:
        return 0.0
    return math.log1p(value) / math.log1p(metric_max)


def render_text_lines(x: float, y: float, lines: list[str], *, anchor: str = "middle", size: int = 13, fill: str = "#334155") -> list[str]:
    parts = [
        f'  <text x="{x}" y="{y}" text-anchor="{anchor}" font-family="DejaVu Sans, Arial, sans-serif" font-size="{size}" fill="{fill}">'
    ]
    for index, line in enumerate(lines):
        dy = 0 if index == 0 else 14
        parts.append(f'    <tspan x="{x}" dy="{dy}">{escape(line)}</tspan>')
    parts.append("  </text>")
    return parts


def split_metric_label(label: str) -> list[str]:
    return label.split("\n")


def split_group_label(label: str) -> list[str]:
    if len(label) <= 18:
        return [label]

    if " + " in label:
        parts = label.split(" + ")
        if len(parts) == 2:
            return [parts[0] + " +", parts[1]]
        elif len(parts) == 3:
            return [parts[0] + " + " + parts[1] + " +", parts[2]]

    words = label.split()
    if len(words) <= 2:
        return [label]

    midpoint = max(1, len(words) // 2)
    return [" ".join(words[:midpoint]), " ".join(words[midpoint:])]


def split_operation_label(label: str) -> list[str]:
    parts = label.split("_")
    if len(parts) <= 1:
        return [label]
    if len(parts) == 2:
        return [" ".join(parts)]
    if len(parts) == 3:
        return [" ".join(parts[:2]), parts[2]]

    midpoint = max(2, len(parts) // 2)
    return [" ".join(parts[:midpoint]), " ".join(parts[midpoint:])]


def compact_phase_label(label: str) -> list[str]:
    label_map = {
        "host_prepare_frontier": ["prepare", "frontier"],
        "host_send_updates_to_remote": ["send", "updates"],
        "host_receive_remote_updates": ["receive", "updates"],
        "host_update_frontier": ["update", "frontier"],
        "remote_apply_host_updates": ["apply", "updates"],
        "remote_generate_updates": ["generate", "updates"],
        "remote_send_updates_to_host": ["send", "updates"],
    }
    return label_map.get(label, split_operation_label(label))


def group_scope_label(operations: list[str]) -> str:
    has_host = any(operation.startswith("host_") for operation in operations)
    has_remote = any(operation.startswith("remote_") for operation in operations)

    if has_host and has_remote:
        return "Host + Remote"
    if has_host:
        return "Host"
    if has_remote:
        return "Remote"
    return "Mixed"


def render_sectioned_bar_chart(
    title: str,
    subtitle: str,
    sections: list[Section],
    output_path: Path,
    x_axis_label: str,
    x_axis_max: float,
    x_axis_suffix: str,
) -> None:
    width = 1180
    left_margin = 330
    right_margin = 120
    section_gap = 36
    header_gap = 28
    row_height = 34
    bar_height = 18
    chart_width = width - left_margin - right_margin
    scale = chart_width / max(x_axis_max, 1.0)
    tick_step = nice_step(x_axis_max)
    tick_count = int(math.ceil(x_axis_max / tick_step)) if x_axis_max > 0 else 1

    height = 112 + sum(header_gap + len(section.operations) * row_height + section_gap for section in sections) + 44
    lines = svg_begin(width, height, title, subtitle)

    current_y = 110
    for section in sections:
        lines.append(
            f'  <text x="330" y="{current_y - 4}" font-family="DejaVu Sans, Arial, sans-serif" font-size="18" font-weight="700" fill="#0f172a">{escape(section.title)}</text>'
        )
        current_y += header_gap - 16

        for tick_index in range(tick_count + 1):
            tick_value = min(x_axis_max, tick_index * tick_step)
            x = left_margin + tick_value * scale
            lines.append(f'  <line x1="{x:.2f}" y1="{current_y - 14}" x2="{x:.2f}" y2="{current_y + len(section.operations) * row_height + 4}" stroke="#e2e8f0" stroke-width="1"/>')
            lines.append(
                f'  <text x="{x:.2f}" y="{current_y + len(section.operations) * row_height + 26}" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">{tick_value:,.0f}{x_axis_suffix}</text>'
            )

        for index, operation in enumerate(section.operations):
            value = section.values.get(operation, 0.0)
            y = current_y + index * row_height
            bar_width = value * scale
            color = NODE_TYPE_COLORS[section.node_type]
            lines.append(
                f'  <text x="318" y="{y + 14}" text-anchor="end" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" fill="#0f172a">{escape(clean_label(operation))}</text>'
            )
            lines.append(
                f'  <rect x="{left_margin}" y="{y}" width="{bar_width:.2f}" height="{bar_height}" rx="6" fill="{color}">'
                f'<title>{escape(operation)} ({NODE_TYPE_LABELS[section.node_type]}): {value:,.0f}{x_axis_suffix}</title></rect>'
            )
            lines.append(
                f'  <text x="{left_margin + max(bar_width + 10, 8):.2f}" y="{y + 14}" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" fill="#334155">{value:,.0f}{x_axis_suffix}</text>'
            )

        current_y += len(section.operations) * row_height + section_gap

    lines.append(
        f'  <text x="330" y="{height - 18}" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">{escape(x_axis_label)}</text>'
    )
    lines.append('</svg>')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_sensitivity_chart(
    title: str,
    subtitle: str,
    sections: list[Section],
    output_path: Path,
) -> None:
    width = 1180
    left_margin = 330
    right_margin = 120
    section_gap = 36
    header_gap = 28
    row_height = 34
    bar_height = 18
    chart_width = width - left_margin - right_margin
    x_axis_max = 100.0
    scale = chart_width / x_axis_max
    tick_values = [0, 20, 40, 60, 80, 100]

    height = 112 + sum(header_gap + len(section.operations) * row_height + section_gap for section in sections) + 44
    lines = svg_begin(width, height, title, subtitle)

    current_y = 110
    for section in sections:
        total = sum(section.values.values())
        lines.append(
            f'  <text x="330" y="{current_y - 4}" font-family="DejaVu Sans, Arial, sans-serif" font-size="18" font-weight="700" fill="#0f172a">{escape(section.title)}</text>'
        )
        current_y += header_gap - 16

        for tick_value in tick_values:
            x = left_margin + tick_value * scale
            lines.append(f'  <line x1="{x:.2f}" y1="{current_y - 14}" x2="{x:.2f}" y2="{current_y + len(section.operations) * row_height + 4}" stroke="#e2e8f0" stroke-width="1"/>')
            lines.append(
                f'  <text x="{x:.2f}" y="{current_y + len(section.operations) * row_height + 26}" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">{tick_value}%</text>'
            )

        for index, operation in enumerate(section.operations):
            raw_value = section.values.get(operation, 0.0)
            share = (raw_value / total * 100.0) if total else 0.0
            y = current_y + index * row_height
            bar_width = share * scale
            color = NODE_TYPE_COLORS[section.node_type]
            lines.append(
                f'  <text x="318" y="{y + 14}" text-anchor="end" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" fill="#0f172a">{escape(clean_label(operation))}</text>'
            )
            lines.append(
                f'  <rect x="{left_margin}" y="{y}" width="{bar_width:.2f}" height="{bar_height}" rx="6" fill="{color}">'
                f'<title>{escape(operation)} ({NODE_TYPE_LABELS[section.node_type]}): {share:.1f}% of total cycles</title></rect>'
            )
            lines.append(
                f'  <text x="{left_margin + max(bar_width + 10, 8):.2f}" y="{y + 14}" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" fill="#334155">{share:.1f}%</text>'
            )

        current_y += len(section.operations) * row_height + section_gap

    lines.append('</svg>')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def render_metric_heatmap(
    title: str,
    subtitle: str,
    sections: list[Section],
    metric_columns: list[tuple[str, str, str]],
    output_path: Path,
) -> None:
    left_margin = 320
    right_margin = 100
    top_margin = 116
    section_gap = 58
    header_height = 42
    row_height = 44
    cell_height = 30
    metric_width = 150
    width = left_margin + right_margin + metric_width * len(metric_columns)
    palette_low = (248, 250, 252)
    palette_high = (29, 78, 216)

    height = top_margin + sum(header_height + len(section.operations) * row_height + section_gap for section in sections) + 36
    lines = svg_begin(width, height, title, "")

    current_y = top_margin
    for section in sections:
        metrics = section.values
        metric_maxima = {
            metric_name: max((metrics[operation].get(metric_name, 0.0) for operation in section.operations), default=0.0)
            for metric_name, _, _ in metric_columns
        }

        lines.append(
            f'  <text x="320" y="{current_y - 6}" font-family="DejaVu Sans, Arial, sans-serif" font-size="19" font-weight="700" fill="#0f172a">{escape(section.title)}</text>'
        )
        current_y += header_height - 16

        for metric_index, (_, metric_label, _) in enumerate(metric_columns):
            column_x = left_margin + metric_index * metric_width
            lines.append(
                f'  <text x="{column_x + metric_width / 2:.2f}" y="{current_y - 10}" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" font-weight="700" fill="#0f172a">'
            )
            for idx, text_line in enumerate(split_metric_label(metric_label)):
                dy = 0 if idx == 0 else 12
                lines.append(f'    <tspan x="{column_x + metric_width / 2:.2f}" dy="{dy}">{escape(text_line)}</tspan>')
            lines.append('  </text>')

        for index, operation in enumerate(section.operations):
            row_y = current_y + index * row_height
            lines.append(
                f'  <text x="308" y="{row_y + 20}" text-anchor="end" font-family="DejaVu Sans, Arial, sans-serif" font-size="13" fill="#0f172a">{escape(clean_label(operation))}</text>'
            )
            for metric_index, (metric_name, _, metric_suffix) in enumerate(metric_columns):
                value = metrics[operation].get(metric_name, 0.0)
                metric_max = metric_maxima[metric_name]
                ratio = metric_ratio(value, metric_max)
                fill = interpolate_color(palette_low, palette_high, ratio)
                column_x = left_margin + metric_index * metric_width
                lines.append(
                    f'  <rect x="{column_x + 8:.2f}" y="{row_y + 5}" width="{metric_width - 16:.2f}" height="{cell_height}" rx="6" fill="{fill}" stroke="#cbd5e1">'
                    f'<title>{escape(operation)} - {metric_name}: {compact_number(value)}{(" " + metric_suffix) if metric_suffix else ""}</title></rect>'
                )
                text_fill = "#ffffff" if ratio > 0.55 else "#0f172a"
                lines.append(
                    f'  <text x="{column_x + metric_width / 2:.2f}" y="{row_y + 25}" text-anchor="middle" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" font-weight="600" fill="{text_fill}">{compact_number(value)}</text>'
                )

        current_y += len(section.operations) * row_height + section_gap

    legend_x = left_margin
    legend_y = height - 34
    lines.append(f'  <text x="{legend_x}" y="{legend_y - 10}" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">Low</text>')
    for index in range(0, 11):
        ratio = index / 10
        x = legend_x + 34 + index * 16
        lines.append(f'  <rect x="{x}" y="{legend_y - 20}" width="16" height="10" fill="{interpolate_color(palette_low, palette_high, ratio)}" stroke="#e2e8f0"/>')
    lines.append(f'  <text x="{legend_x + 224}" y="{legend_y - 10}" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">High</text>')
    lines.append(f'  <text x="{legend_x + 288}" y="{legend_y - 10}" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">Color is relative within each metric column.</text>')
    lines.append('</svg>')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def phase_label_lines(phase: str) -> list[str]:
    parts = phase.split("_")
    if len(parts) <= 2:
        return [phase.replace("_", " ")]
    return [" ".join(parts[:2]), " ".join(parts[2:])]


def render_grouped_sensitivity_chart(
    title: str,
    subtitle: str,
    groups: list[dict[str, object]],
    output_path: Path,
    show_scope_labels: bool = True,
) -> None:
    left_margin = 96
    right_margin = 48
    top_margin = 122
    bottom_margin = 144
    plot_height = 372
    bar_width = 14
    bar_gap = 8
    group_width = len(METRIC_SERIES) * bar_width + (len(METRIC_SERIES) - 1) * bar_gap
    if len(groups) > 1:
        group_gap = 64
    else:
        group_gap = 0

    max_value = max((group["values"].get(metric_name, 0.0) for group in groups for metric_name, _, _ in METRIC_SERIES), default=0.0)
    max_value = max(max_value, 1.0)
    log_max = math.log10(max_value)
    if log_max <= 0:
        log_max = 1.0

    plot_width = len(groups) * group_width + max(0, len(groups) - 1) * group_gap
    width = int(max(800, left_margin + right_margin + plot_width))
    actual_left_margin = left_margin
    if width > left_margin + right_margin + plot_width:
        actual_left_margin += (width - left_margin - right_margin - plot_width) / 2

    height = top_margin + plot_height + bottom_margin
    lines = svg_begin(width, height, title, subtitle, legend_entries=[], title_x=left_margin)

    legend_x = left_margin
    legend_y = 68
    cursor_x = legend_x
    for metric_name, metric_label, color in METRIC_SERIES:
        lines.append(f'  <rect x="{cursor_x}" y="{legend_y}" width="12" height="12" rx="3" fill="{color}"/>')
        lines.append(f'  <text x="{cursor_x + 18}" y="{legend_y + 10}" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#334155">{escape(metric_label)}</text>')
        cursor_x += 112

    axis_top = top_margin
    axis_bottom = top_margin + plot_height
    axis_left = actual_left_margin
    axis_right = actual_left_margin + plot_width
    lines.append(f'  <line x1="{axis_left}" y1="{axis_top}" x2="{axis_right}" y2="{axis_top}" stroke="#e2e8f0" stroke-width="1"/>')
    lines.append(f'  <line x1="{axis_left}" y1="{axis_bottom}" x2="{axis_right}" y2="{axis_bottom}" stroke="#cbd5e1" stroke-width="1.5"/>')

    tick_values = []
    exponent = 0
    while 10 ** exponent <= max_value * 1.0001:
        tick_values.append(10 ** exponent)
        exponent += 1
        if exponent > 16:
            break

    for tick_value in tick_values:
        tick_y = axis_bottom - (math.log10(tick_value) / log_max) * plot_height
        lines.append(f'  <line x1="{axis_left}" y1="{tick_y:.2f}" x2="{axis_right}" y2="{tick_y:.2f}" stroke="#eef2f7" stroke-width="1"/>')
        lines.append(f'  <text x="{axis_left - 10}" y="{tick_y + 4:.2f}" text-anchor="end" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">{compact_number(tick_value)}</text>')

    lines.append(f'  <text x="28" y="{top_margin + plot_height / 2:.2f}" transform="rotate(-90 28 {top_margin + plot_height / 2:.2f})" font-family="DejaVu Sans, Arial, sans-serif" font-size="12" fill="#64748b">Amortized metric value (log scale)</text>')

    for index, group in enumerate(groups):
        group_x = actual_left_margin + index * (group_width + group_gap)
        label_x = group_x + group_width / 2
        group_title = str(group["title"])
        group_operations_list = [str(operation) for operation in group["operations"]]
        group_operations = ", ".join(group_operations_list)
        group_values = group["values"]
        scope_label = group_scope_label(group_operations_list)
        for metric_index, (metric_name, _, color) in enumerate(METRIC_SERIES):
            value = group_values.get(metric_name, 0.0)
            if value <= 0:
                bar_height = 0.0
                bar_y = axis_bottom
            else:
                bar_y = axis_bottom - (math.log10(value) / log_max) * plot_height
                bar_height = axis_bottom - bar_y
            bar_x = group_x + metric_index * (bar_width + bar_gap)
            lines.append(
                f'  <rect x="{bar_x:.2f}" y="{bar_y:.2f}" width="{bar_width}" height="{bar_height:.2f}" rx="4" fill="{color}">'
                f'<title>{escape(group_title)} ({escape(group_operations)}) - {metric_name}: {compact_number(value)}</title></rect>'
            )

        label_lines = compact_phase_label(group_title) if "_" in group_title else split_group_label(group_title)
        lines.extend(render_text_lines(label_x, axis_bottom + 20, label_lines, anchor="middle", size=10, fill="#0f172a"))
        if show_scope_labels:
            scope_y = axis_bottom + 20 + max(16, 14 * len(label_lines))
            lines.extend(render_text_lines(label_x, scope_y, [scope_label], anchor="middle", size=8, fill="#2563eb"))

    lines.append('</svg>')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_index(output_dir: Path) -> None:
    index_path = output_dir / "profile_plots" / "README.md"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        "# Profiling Plots\n\n"
        "- [Operation metric heatmap](operation_metric_heatmap.svg)\n"
        "- [Operation cost by node type](operation_costs.svg)\n"
        "- [Operation sensitivity by phase](operation_sensitivity.svg)\n"
        "- [Lifecycle-grouped sensitivity](operation_sensitivity_lifecycle_groups.svg)\n"
        "- [Sensitivity without communication phases](operation_sensitivity_no_comm.svg)\n"
        "Generate these with `python scripts/build_profile_plots.py`.\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Build SVG plots from Grudon profiler summaries.")
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory containing profile_summary_compute.json and profile_summary_memory.json.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    compute_summary = load_summary(output_dir / "profile_summary_compute.json")
    memory_summary = load_summary(output_dir / "profile_summary_memory.json")

    compute_costs = collect_metric(compute_summary, COMPUTE_OPERATIONS, "cycles")
    memory_costs = collect_metric(memory_summary, MEMORY_OPERATIONS, "cycles")
    compute_metrics = collect_operation_metrics(compute_summary, COMPUTE_OPERATIONS)
    memory_metrics = collect_operation_metrics(memory_summary, MEMORY_OPERATIONS)
    phase_metrics = {**collect_phase_metrics(compute_summary, COMPUTE_OPERATIONS), **collect_phase_metrics(memory_summary, MEMORY_OPERATIONS)}
    phase_level_groups = aggregate_sensitivity_groups(phase_metrics, SENSITIVITY_PHASE_GROUPS)
    lifecycle_groups = aggregate_sensitivity_groups(phase_metrics, SENSITIVITY_LIFECYCLE_GROUPS)
    no_comm_groups = aggregate_sensitivity_groups(
        phase_metrics,
        [(phase_name, [phase_name]) for phase_name in SENSITIVITY_NO_COMM_PHASES],
    )

    render_sectioned_bar_chart(
        title="Operation Cost by Node Type",
        subtitle="Average cycles per iteration from the profiler summaries. Higher bars indicate operations that dominate runtime on compute or memory ranks.",
        sections=[
            Section(
                node_type="compute",
                title="Compute operations",
                description="Host-side operations active on compute ranks.",
                operations=COMPUTE_OPERATIONS,
                values=compute_costs,
            ),
            Section(
                node_type="memory",
                title="Memory operations",
                description="Remote-side operations active on memory ranks.",
                operations=MEMORY_OPERATIONS,
                values=memory_costs,
            ),
        ],
        output_path=output_dir / "profile_plots" / "operation_costs.svg",
        x_axis_label="Average cycles per iteration",
        x_axis_max=max([*compute_costs.values(), *memory_costs.values(), 0.0]),
        x_axis_suffix=" cycles/iter",
    )

    render_metric_heatmap(
        title="Operation Intensity Heatmap",
        subtitle="Instructions, cycles, cache misses, dTLB loads, L3 misses, memory loads, and memory stores per iteration are shown together to reveal compute, cache, and memory behavior.",
        sections=[
            Section(
                node_type="compute",
                title="Compute operations",
                description="Host-side operations active on compute ranks.",
                operations=COMPUTE_OPERATIONS,
                values=compute_metrics,
            ),
            Section(
                node_type="memory",
                title="Memory operations",
                description="Remote-side operations active on memory ranks.",
                operations=MEMORY_OPERATIONS,
                values=memory_metrics,
            ),
        ],
        metric_columns=METRIC_COLUMNS,
        output_path=output_dir / "profile_plots" / "operation_metric_heatmap.svg",
    )

    render_grouped_sensitivity_chart(
        title="Operation Sensitivity by Phase",
        subtitle="Instructions, cycles, mem loads, and mem stores are plotted per phase on a log-scaled y-axis so all four series stay readable.",
        groups=phase_level_groups,
        output_path=output_dir / "profile_plots" / "operation_sensitivity.svg",
    )

    render_grouped_sensitivity_chart(
        title="Lifecycle-Grouped Sensitivity",
        subtitle="The frontier/update lifecycle is grouped into prepare-send, apply-update, and generate-return stages to keep the chart compact and aligned.",
        groups=lifecycle_groups,
        output_path=output_dir / "profile_plots" / "operation_sensitivity_lifecycle_groups.svg",
    )

    render_grouped_sensitivity_chart(
        title="Sensitivity Without Communication Phases",
        subtitle="Communication-heavy send and receive phases are removed, leaving only the compute, apply, and generate stages on the same scale.",
        groups=no_comm_groups,
        output_path=output_dir / "profile_plots" / "operation_sensitivity_no_comm.svg",
        show_scope_labels=False,
    )

    write_index(output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())