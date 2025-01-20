#!/bin/bash

# Input File
file=$1

# Use AWK to calculate MAX values for TraversalTimer, UpdateTimer, CPhase1Timer, and CPhase2Timer
awk -F, '
BEGIN {
    OFS=", "; 
    runtime = 0;
    host_idle_time = 0;
    ndp_idle_time = 0;
    idle_count = 0;
}
$3 ~ /Timer_0/ { 
    runtime = $5
}
$3 ~ /IdleTimer/ {
    idle_count++
    if (idle_count <= 3) {
        ndp_idle_time += $5
    } else {
        host_idle_time = $5
    }
}
END {
    print runtime + 0, host_idle_time + 0, ndp_idle_time
}' "$file"
