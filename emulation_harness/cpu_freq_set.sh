#!/bin/bash
# Usage: ./set_socket_freq_and_stress.sh 1200,1400,1600

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 freq_socket1,freq_socket2,..."
  exit 1
fi

IFS=',' read -ra FREQS <<< "$1"

# Get mapping: CPU number to socket id (skip comments with '#')
declare -A SOCKET_CPUS
while IFS=',' read -r cpu socket; do
  SOCKET_CPUS[$socket]="${SOCKET_CPUS[$socket]}$cpu,"
done < <(lscpu -p=CPU,Socket | grep -v '^#')

num_sockets="${#SOCKET_CPUS[@]}"

if [[ ${#FREQS[@]} -ne $num_sockets ]]; then
  echo "Number of frequency values (${#FREQS[@]}) does not match number of sockets ($num_sockets)"
  exit 2
fi

i=0
for socket in $(printf "%s\n" "${!SOCKET_CPUS[@]}" | sort -n); do
  cpulist="${SOCKET_CPUS[$socket]%,}"  # Remove trailing comma
  freq="${FREQS[$i]}MHz"
  echo "Setting frequency $freq for CPUs on socket $socket: $cpulist"
  sudo cpupower -c "$cpulist" frequency-set -f "$freq"
  ((i++))
done

# Get total number of CPUs for stress-ng
TOTAL_CPUS=$(nproc)

echo "Starting stress-ng on all $TOTAL_CPUS CPUs for 20 seconds..."
sudo stress-ng --cpu "$TOTAL_CPUS" --timeout 20s &

STRESS_PID=$!

echo "Starting turbostat to visualize CPU stats..."
sudo turbostat --interval 5 &
TURBOSTAT_PID=$!

# Wait for stress-ng to complete
wait $STRESS_PID

# Stop turbostat
sudo kill $TURBOSTAT_PID

echo "Stress test and monitoring complete."

