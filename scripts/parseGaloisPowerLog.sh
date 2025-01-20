#!/bin/bash

# Input File
file=$1

# Use AWK to calculate MAX values for TraversalTimer, UpdateTimer, CPhase1Timer, and CPhase2Timer
awk -F, '
BEGIN {
    OFS=", "; 
    avg_runtime = 0;
    avg_idle_time = 0;
}
$4 ~ /Timer_0/ { 
    runtime = $6
}
$4 ~ /ReduceIdle_*/ { 
    avg_idle_time = $6
}
END {
    print  runtime*3, avg_idle_time*3, 0
}' "$file"
