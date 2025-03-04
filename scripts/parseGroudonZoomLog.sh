#!/bin/bash

# Input File
file=$1

# Use AWK to calculate MAX values for TraversalTimer, UpdateTimer, CPhase1Timer, and CPhase2Timer
awk -F, '
BEGIN {
    OFS=", "; 
    max_traversal = 0; 
    max_update = 0;
    cphase1 = 0;
    cphase2 = 0;
    runtime = 0;
    traversal_count = 0;
    host_idle_time = 0;
    ndp_idle_time = 0;
    host_traversal_time = 0;
}
$3 ~ /TraversalTimer/ {
    traversal_count++;
    if (traversal_count <= 3) {
        if ($5 > max_traversal) {
            max_traversal = $5;
        }
    } else if (traversal_count == 4) {
        host_traversal_time += $5;
    }
}
$3 ~ /IdleTimer/ {
    if (traversal_count <= 3) {
        ndp_idle_time += $5
    } else {
        host_idle_time = $5
    }
}
$3 ~ /UpdateTimer/ && $5 > max_update { 
    max_update = $5 
}
$3 ~ /CPhase1Timer/ && traversal_count <= 3 { 
    cphase1 += $5 
}
$3 ~ /CPhase2Timer/ { 
    cphase2 = $5 
}
$3 ~ /Timer_0/ { 
    runtime = $5 
}
$3 ~ /TotalBytesMoved/ { 
    bytes_moved = $5 
}
END {
    print runtime + 0, cphase2 - host_idle_time - host_traversal_time, cphase1 - ndp_idle_time, host_idle_time + 0,  ndp_idle_time + 0, max_update + host_traversal_time, max_traversal + 0, bytes_moved + 0
}' "$file"
