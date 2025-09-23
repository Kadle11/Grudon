#!/bin/bash

# SMT Toggle Script with NUMA Support
# Usage: ./smt_toggle_fixed.sh [enable|disable] [numa_node]

ACTION="$1"
NUMA_NODE="$2"

if [[ "$ACTION" != "enable" && "$ACTION" != "disable" ]]; then
    echo "Usage: $0 [enable|disable] [numa_node]"
    echo "Example: $0 disable 0"
    exit 1
fi

# Function to get CPUs for a NUMA node
get_numa_cpus() {
    local node=$1
    if [[ -n "$node" ]]; then
        cat /sys/devices/system/node/node${node}/cpulist 2>/dev/null
    else
        echo "Error: Invalid NUMA node"
        return 1
    fi
}

# Function to get sibling CPUs (hyperthreads)
get_cpu_siblings() {
    local cpu=$1
    cat /sys/devices/system/cpu/cpu${cpu}/topology/thread_siblings_list 2>/dev/null
}

# Function to identify logical cores (higher numbered siblings)
get_logical_cores() {
    local numa_cpus="$1"
    local logical_cores=""
    
    # Convert comma-separated list to array
    IFS=',' read -ra CPU_ARRAY <<< "$numa_cpus"
    
    declare -A processed_cores
    
    for cpu in "${CPU_ARRAY[@]}"; do
        # Handle ranges like "0-3"
        if [[ "$cpu" == *"-"* ]]; then
            IFS='-' read -ra RANGE <<< "$cpu"
            for ((i=${RANGE[0]}; i<=${RANGE[1]}; i++)); do
                if [[ -z "${processed_cores[$i]}" ]]; then
                    siblings=$(get_cpu_siblings $i)
                    if [[ -n "$siblings" ]]; then
                        IFS=',' read -ra SIB_ARRAY <<< "$siblings"
                        if [[ ${#SIB_ARRAY[@]} -gt 1 ]]; then
                            # Mark all siblings as processed
                            for sib in "${SIB_ARRAY[@]}"; do
                                processed_cores[$sib]=1
                            done
                            # The logical core is the higher numbered sibling
                            max_sib=${SIB_ARRAY[0]}
                            for sib in "${SIB_ARRAY[@]}"; do
                                if [[ $sib -gt $max_sib ]]; then
                                    max_sib=$sib
                                fi
                            done
                            if [[ -n "$logical_cores" ]]; then
                                logical_cores="${logical_cores},${max_sib}"
                            else
                                logical_cores="$max_sib"
                            fi
                        fi
                    fi
                fi
            done
        else
            if [[ -z "${processed_cores[$cpu]}" ]]; then
                siblings=$(get_cpu_siblings $cpu)
                if [[ -n "$siblings" ]]; then
                    IFS=',' read -ra SIB_ARRAY <<< "$siblings"
                    if [[ ${#SIB_ARRAY[@]} -gt 1 ]]; then
                        # Mark all siblings as processed
                        for sib in "${SIB_ARRAY[@]}"; do
                            processed_cores[$sib]=1
                        done
                        # The logical core is the higher numbered sibling
                        max_sib=${SIB_ARRAY[0]}
                        for sib in "${SIB_ARRAY[@]}"; do
                            if [[ $sib -gt $max_sib ]]; then
                                max_sib=$sib
                            fi
                        done
                        if [[ -n "$logical_cores" ]]; then
                            logical_cores="${logical_cores},${max_sib}"
                        else
                            logical_cores="$max_sib"
                        fi
                    fi
                fi
            fi
        fi
    done
    
    echo "$logical_cores"
}

# Main logic
if [[ -n "$NUMA_NODE" ]]; then
    echo "Processing NUMA node $NUMA_NODE..."
    numa_cpus=$(get_numa_cpus $NUMA_NODE)
    if [[ -z "$numa_cpus" ]]; then
        echo "Error: Could not get CPU list for NUMA node $NUMA_NODE"
        exit 1
    fi
    echo "NUMA node $NUMA_NODE CPUs: $numa_cpus"
    
    logical_cores=$(get_logical_cores "$numa_cpus")
    echo "Logical cores to toggle: $logical_cores"
    
    if [[ -n "$logical_cores" ]]; then
        IFS=',' read -ra LOGICAL_ARRAY <<< "$logical_cores"
        
        disabled_cpus=""
        enabled_cpus=""
        
        for cpu in "${LOGICAL_ARRAY[@]}"; do
            if [[ "$ACTION" == "disable" ]]; then
                if [[ -f "/sys/devices/system/cpu/cpu${cpu}/online" ]]; then
                    current_state=$(cat /sys/devices/system/cpu/cpu${cpu}/online)
                    if [[ "$current_state" == "1" ]]; then
                        echo 0 > /sys/devices/system/cpu/cpu${cpu}/online
                        if [[ $? -eq 0 ]]; then
                            if [[ -n "$disabled_cpus" ]]; then
                                disabled_cpus="${disabled_cpus},${cpu}"
                            else
                                disabled_cpus="$cpu"
                            fi
                        else
                            echo "Failed to disable CPU $cpu"
                        fi
                    else
                        echo "CPU $cpu already disabled"
                    fi
                fi
            else
                if [[ -f "/sys/devices/system/cpu/cpu${cpu}/online" ]]; then
                    current_state=$(cat /sys/devices/system/cpu/cpu${cpu}/online)
                    if [[ "$current_state" == "0" ]]; then
                        echo 1 > /sys/devices/system/cpu/cpu${cpu}/online
                        if [[ $? -eq 0 ]]; then
                            if [[ -n "$enabled_cpus" ]]; then
                                enabled_cpus="${enabled_cpus},${cpu}"
                            else
                                enabled_cpus="$cpu"
                            fi
                        else
                            echo "Failed to enable CPU $cpu"
                        fi
                    else
                        echo "CPU $cpu already enabled"
                    fi
                fi
            fi
        done
        
        if [[ "$ACTION" == "disable" ]]; then
            echo "CPUs disabled: $disabled_cpus"
        else
            echo "CPUs enabled: $enabled_cpus"
        fi
    else
        echo "No logical cores found for NUMA node $NUMA_NODE"
    fi
else
    echo "Error: NUMA node not specified"
    exit 1
fi

# Verify current state
echo ""
echo "Current CPU online status for NUMA node $NUMA_NODE:"
numa_cpus=$(get_numa_cpus $NUMA_NODE)
IFS=',' read -ra CPU_ARRAY <<< "$numa_cpus"
for cpu in "${CPU_ARRAY[@]}"; do
    if [[ "$cpu" == *"-"* ]]; then
        IFS='-' read -ra RANGE <<< "$cpu"
        for ((i=${RANGE[0]}; i<=${RANGE[1]}; i++)); do
            if [[ -f "/sys/devices/system/cpu/cpu${i}/online" ]]; then
                status=$(cat /sys/devices/system/cpu/cpu${i}/online)
                echo "CPU $i: $status"
            else
                echo "CPU $i: always online (boot CPU)"
            fi
        done
    else
        if [[ -f "/sys/devices/system/cpu/cpu${cpu}/online" ]]; then
            status=$(cat /sys/devices/system/cpu/cpu${cpu}/online)
            echo "CPU $cpu: $status"
        else
            echo "CPU $cpu: always online (boot CPU)"
        fi
    fi
done
