#!/bin/bash

set -euo pipefail

if [ $# -ne 3 ]; then
  echo "Usage: $0 <path_to_mlc> <path_to_stream> <target_bandwidth_percentages_comma_separated>"
  echo "Example: $0 ./mlc ./stream 80,90"
  exit 1
fi

mlc_bin="$1"
stream_bin="$2"
targets_csv="$3"

IFS=',' read -r -a target_pcts <<< "$targets_csv"

declare -A SOCKET_CPUS
while IFS=',' read -r cpu socket; do
  [[ $cpu =~ ^# ]] && continue
  if [[ -n $socket ]]; then
    if [[ -n ${SOCKET_CPUS[$socket]:-} ]]; then
      SOCKET_CPUS[$socket]="${SOCKET_CPUS[$socket]} $cpu"
    else
      SOCKET_CPUS[$socket]="$cpu"
    fi
  fi
done < <(lscpu -p=CPU,Socket)

num_sockets="${#SOCKET_CPUS[@]}"
echo "Detected $num_sockets sockets"

if [ "${#target_pcts[@]}" -ne "$num_sockets" ]; then
  echo "Error: target percentages count (${#target_pcts[@]}) does not match number of sockets ($num_sockets)"
  exit 1
fi

echo "Running mlc to get max local bandwidth per socket..."
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

read -ra col_headers <<< $(echo "$col_headers_line" | sed -E 's/.*Numa node[[:space:]]+//')

declare -A max_local_bw

for line in "${matrix_lines[@]}"; do
  [[ -z "$line" ]] && continue
  read -ra tokens <<< "$line"
  row_socket_id="${tokens[0]}"
  vals=("${tokens[@]:1}")
  for i in "${!col_headers[@]}"; do
    col_socket_id="${col_headers[i]}"
    val=""
    if [[ $i -lt ${#vals[@]} ]]; then
      val="${vals[i]}"
    fi
    num_val=$(echo "$val" | grep -oE '[0-9.]+' || echo "")
    if [ "$row_socket_id" == "$col_socket_id" ]; then
      max_local_bw[$row_socket_id]="${num_val:-0}"
      break
    fi
  done
done

echo "Max local bandwidth per socket:"
for s in $(printf "%s\n" "${!max_local_bw[@]}" | sort -n); do
  echo "Socket $s: ${max_local_bw[$s]} MB/s"
done

measure_local_bw() {
  local socket="$1"
  local cores="$2"

  echo "Running stream pinned to cores $cores on socket $socket" >&2
  IFS=',' read -r -a core_list <<< "$cores"
  local pids=()

  for core in "${core_list[@]}"; do
    numactl -C "$core" -- "$stream_bin" ReadWrite64 180 &
    pids+=($!)
  done

  sleep 10

  timeout 180 $mlc_bin --bandwidth_matrix > mlc_output.tmp

  set +e
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done
  set -e

  local bw_out=$(<mlc_output.tmp)

  readarray -t bw_lines <<< "$bw_out"

  col_headers_line=""
  matrix_lines=()

  for idx in "${!bw_lines[@]}"; do
    line="${bw_lines[$idx]}"
    if [[ $line =~ ^[[:space:]]*Numa[[:space:]]+node[[:space:]]+([0-9[:space:]]+)$ ]]; then
      col_headers_line="$line"
      matrix_lines=("${bw_lines[@]:$((idx + 1))}")
      break
    fi
  done

  read -ra col_headers <<< $(echo "$col_headers_line" | sed -E 's/.*Numa node[[:space:]]+//')

  local local_bw_val=0
  local found=0
  for line in "${matrix_lines[@]}"; do
    [[ -z "$line" ]] && continue
    read -ra tokens <<< "$line"
    row_socket_id="${tokens[0]}"
    vals=("${tokens[@]:1}")
    if [ "$row_socket_id" == "$socket" ]; then
      for i in "${!col_headers[@]}"; do
        if [ "${col_headers[i]}" == "$row_socket_id" ]; then
          if [[ $i -lt ${#vals[@]} ]]; then
            local_bw_val=$(echo "${vals[i]}" | grep -oE '[0-9.]+' || echo 0)
            found=1
          else
            echo "Warning: missing bandwidth value for socket $socket in measure_local_bw parsing" >&2
            local_bw_val=0
          fi
          break
        fi
      done
      break
    fi
  done

  if [ "$found" -eq 0 ] || [ -z "$local_bw_val" ] || [[ "$local_bw_val" == "0" ]]; then
    echo "Warning: bandwidth for socket $socket not found or zero, falling back to max bandwidth" >&2
    local_bw_val="${max_local_bw[$socket]}"
  fi

  echo "$local_bw_val"
}

declare -A socket_selected_cores

for socket in $(printf "%s\n" "${!SOCKET_CPUS[@]}" | sort -n); do
  cores_str="${SOCKET_CPUS[$socket]}"
  read -r -a cores_arr <<< "$cores_str"

  reversed_cores=()
  for (( idx=${#cores_arr[@]}-1 ; idx>=0 ; idx-- )) ; do
    reversed_cores+=( "${cores_arr[idx]}" )
  done

  target_percent="${target_pcts[$socket]}"
  target_bw=$(echo "${max_local_bw[$socket]} * $target_percent / 100" | bc -l)
  echo "Socket $socket target bandwidth = $target_percent% of max = $target_bw MB/s"

  selected=()
  bw=$(measure_local_bw "$socket" "")
  echo "Cores: None Bandwidth: $bw MB/s"

  lower_limit=$(echo "$target_bw * 0.95" | bc -l)
  upper_limit=$(echo "$target_bw * 1.05" | bc -l)

  if (( $(echo "$bw >= $lower_limit" | bc -l) )) && (( $(echo "$bw <= $upper_limit" | bc -l) )); then
    socket_selected_cores[$socket]=""
    continue
  fi

  for core in "${reversed_cores[@]}"; do
    selected+=("$core")
    cores_csv=$(IFS=','; echo "${selected[*]}")

    bw=$(measure_local_bw "$socket" "$cores_csv")
    echo "Cores: $cores_csv Bandwidth: $bw MB/s"

    if (( $(echo "$bw >= $lower_limit" | bc -l) )) && (( $(echo "$bw <= $upper_limit" | bc -l) )); then
      break
    fi
  done

  socket_selected_cores[$socket]="$cores_csv"
done

echo "Final selected cores to achieve target bandwidth:"
for s in $(printf "%s\n" "${!socket_selected_cores[@]}" | sort -n); do
  cores_csv="${socket_selected_cores[$s]}"
  if [[ -z "$cores_csv" ]]; then
    echo "Socket $s: None"
  else
    echo "Socket $s: $cores_csv"
  fi
done

echo "Starting 10-hour stress streams..."
declare -a all_pids=()
for s in $(printf "%s\n" "${!socket_selected_cores[@]}" | sort -n); do
  cores_csv="${socket_selected_cores[$s]}"
  if [[ -n "$cores_csv" ]]; then
    echo "Starting streams on socket $s cores: $cores_csv"
    IFS=',' read -r -a cores_arr <<< "$cores_csv"
    for core in "${cores_arr[@]}"; do
      numactl -C "$core" -- "$stream_bin" ReadWrite64 36000 &
      all_pids+=($!)
    done
  fi
done

echo "All streams launched. PIDs: ${all_pids[*]}"
echo "You can monitor or kill these processes as needed."
echo "Done."

exit 0
