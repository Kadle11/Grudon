#!/bin/bash

set -euo pipefail

if [ $# -lt 1 ] || [ $# -gt 3 ]; then
  echo "Usage: $0 <path_to_mlc> [top_n] [sample_seconds]"
  echo "Example: $0 ./mlc"
  echo "Example: $0 ./mlc 5 2"
  exit 1
fi

mlc_bin="$1"
top_n="${2:-5}"
sample_seconds="${3:-1}"

if [ ! -x "$mlc_bin" ]; then
  echo "Error: MLC binary '$mlc_bin' is not executable"
  exit 1
fi

if ! [[ "$top_n" =~ ^[0-9]+$ ]] || [ "$top_n" -le 0 ]; then
  echo "Error: top_n must be a positive integer"
  exit 1
fi

if ! [[ "$sample_seconds" =~ ^[0-9]+$ ]] || [ "$sample_seconds" -le 0 ]; then
  echo "Error: sample_seconds must be a positive integer"
  exit 1
fi

echo "Collecting current memory bandwidth matrix using MLC..."
mlc_output=$($mlc_bin --bandwidth_matrix)

readarray -t lines <<< "$mlc_output"

col_headers_line=""
matrix_lines=()

for idx in "${!lines[@]}"; do
  line="${lines[$idx]}"
  if [[ $line =~ ^[[:space:]]*Numa[[:space:]]+node[[:space:]]+([0-9[:space:]]+)$ ]]; then
    col_headers_line="$line"
    matrix_lines=("${lines[@]:$((idx + 1))}")
    break
  fi
done

if [[ -z "$col_headers_line" ]]; then
  echo "Error: unable to parse MLC output (Numa node header not found)"
  exit 1
fi

read -ra col_headers <<< "$(echo "$col_headers_line" | sed -E 's/.*Numa node[[:space:]]+//')"

declare -A local_bw

for line in "${matrix_lines[@]}"; do
  [[ -z "$line" ]] && continue

  read -ra tokens <<< "$line"
  [[ ${#tokens[@]} -lt 2 ]] && continue

  row_socket_id="${tokens[0]}"
  vals=("${tokens[@]:1}")

  for i in "${!col_headers[@]}"; do
    col_socket_id="${col_headers[i]}"
    if [[ "$row_socket_id" == "$col_socket_id" ]]; then
      raw_val=""
      if [[ $i -lt ${#vals[@]} ]]; then
        raw_val="${vals[i]}"
      fi
      parsed_val=$(echo "$raw_val" | grep -oE '[0-9.]+' || echo "0")
      local_bw[$row_socket_id]="$parsed_val"
      break
    fi
  done
done

if [ ${#local_bw[@]} -eq 0 ]; then
  echo "Error: unable to parse per-socket local bandwidth from MLC output"
  exit 1
fi

echo
echo "Current local memory bandwidth per socket (MB/s):"
total_bw=0
for s in $(printf "%s\n" "${!local_bw[@]}" | sort -n); do
  bw="${local_bw[$s]}"
  echo "Socket $s: $bw"
  total_bw=$(echo "$total_bw + $bw" | bc -l)
done

echo "Aggregate local bandwidth (sum of socket diagonals): $total_bw MB/s"

echo
echo "Top memory consumers (RSS, best effort):"
ps -e -o pid=,rss=,comm= --sort=-rss | head -n "$top_n" | awk '{printf "PID=%s RSS=%sKB CMD=%s\n", $1, $2, $3}'

if command -v pidstat >/dev/null 2>&1; then
  echo
  echo "Top memory-activity consumers (page faults over ${sample_seconds}s, best effort):"
  pidstat_output=$(pidstat -r -h "$sample_seconds" 1 2>/dev/null || true)

  if [[ -n "$pidstat_output" ]]; then
    echo "$pidstat_output" \
      | awk -v top_n="$top_n" '
        /^Average:/ && $3 ~ /^[0-9]+$/ {
          pid=$3; minflt=$4+0; majflt=$5+0; rss=$7+0; cmd=$NF;
          score=minflt + (majflt * 1000);
          printf "%f\t%s\t%f\t%f\t%s\t%f\n", score, pid, minflt, majflt, cmd, rss;
        }
      ' \
      | sort -nr \
      | head -n "$top_n" \
      | awk '{printf "PID=%s MINFLT/s=%s MAJFLT/s=%s RSS=%sKB CMD=%s\n", $2, $3, $4, $6, $5}'
  else
    echo "No pidstat output available."
  fi
else
  echo
  echo "Note: 'pidstat' not found; skipping page-fault activity ranking."
fi

echo
echo "Done."
