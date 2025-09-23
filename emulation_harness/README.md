# Emulation Harness Scripts

These scripts are designed to manipulate CPU frequency, memory bandwidth, and SMT (Simultaneous Multithreading) settings for experimental purposes across NUMA nodes.

## Scripts Overview

### 1. cpu_freq_set.sh

**Description**: Sets CPU frequency per socket and runs stress testing with visualization.

This script allows you to set different CPU frequencies for different CPU sockets and then runs a stress test while monitoring CPU statistics with turbostat.

**Usage**:
```bash
./cpu_freq_set.sh freq_socket1,freq_socket2,...
```

**Parameters**:
- `freq_socket1,freq_socket2,...`: Comma-separated list of frequencies (in MHz) for each socket

**Example**:
```bash
# Set socket 0 to 1200MHz and socket 1 to 1600MHz
./cpu_freq_set.sh 1200,1600

# Set single socket to 2400MHz
./cpu_freq_set.sh 2400
```

**What it does**:
1. Maps CPU cores to their respective sockets
2. Sets the specified frequency for each socket using `cpupower`
3. Runs `stress-ng` on all CPUs for 20 seconds
4. Monitors CPU statistics with `turbostat` during the stress test

**Requirements**: 
- `cpupower` utility
- `stress-ng` utility  
- `turbostat` utility
- Root privileges (uses `sudo`)

---

### 2. memory_bandwidth_set.sh

**Description**: Controls memory bandwidth utilization per socket using targeted core allocation.

This script measures maximum memory bandwidth per socket and then selectively enables cores to achieve specified target bandwidth percentages.

**Usage**:
```bash
./memory_bandwidth_set.sh <path_to_mlc> <path_to_stream> <target_bandwidth_percentages_comma_separated>
```

**Parameters**:
- `path_to_mlc`: Path to Intel Memory Latency Checker (MLC) binary
- `path_to_stream`: Path to STREAM benchmark binary
- `target_bandwidth_percentages_comma_separated`: Comma-separated target bandwidth percentages for each socket

**Example**:
```bash
# Set socket 0 to 80% and socket 1 to 90% of maximum bandwidth
./memory_bandwidth_set.sh ./mlc ./stream 80,90

# Set single socket to 75% of maximum bandwidth
./memory_bandwidth_set.sh /usr/local/bin/mlc /opt/stream/stream 75
```

**What it does**:
1. Uses MLC to measure maximum local bandwidth for each socket
2. Iteratively selects cores to achieve target bandwidth percentages
3. Launches long-running STREAM benchmarks (10 hours) on selected cores
4. Provides real-time bandwidth measurements during calibration

**Requirements**:
- Intel MLC (Memory Latency Checker) binary
- STREAM benchmark binary
- `numactl` utility
- `bc` calculator
- Root privileges for some operations

---

### 3. smt_toggle.sh

**Description**: Enables or disables SMT (hyperthreading) for specific NUMA nodes.

This script provides fine-grained control over SMT by allowing you to enable or disable logical cores (hyperthreads) on a per-NUMA-node basis.

**Usage**:
```bash
./smt_toggle.sh [enable|disable] [numa_node]
```

**Parameters**:
- `enable|disable`: Action to perform on SMT
- `numa_node`: NUMA node number to target

**Examples**:
```bash
# Disable SMT on NUMA node 0
./smt_toggle.sh disable 0

# Enable SMT on NUMA node 1
./smt_toggle.sh enable 1
```

**What it does**:
1. Identifies physical and logical cores for the specified NUMA node
2. Determines which cores are hyperthreads (logical cores)
3. Enables or disables the logical cores as requested
4. Displays the current online status of all CPUs in the NUMA node

**Requirements**:
- Root privileges (modifies `/sys/devices/system/cpu/`)
- NUMA-aware system
- Hyperthreading-capable CPUs