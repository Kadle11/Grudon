#!/bin/bash

# Input File
file=$1

# Use AWK to calculate MAX values for TraversalTimer, UpdateTimer, CPhase1Timer, and CPhase2Timer
awk -F, '
BEGIN {
    OFS=", "; 
    max_traversal = 0; 
    max_update = 0;
    max_cphase = 0;
    runtime = 0;
}
$3 ~ /PageRank_0/ && $6 > max_traversal { 
    max_traversal = $6 
}
$3 ~ /delta/ && $6 > max_update { 
    max_update = $6 
}
$4 ~ /Sync_PageRank_0/ && $6 > max_cphase { 
    max_cphase = $6
}
$4 ~ /Timer_0/ { 
    runtime = $6 
}
END {
    print max_traversal + max_update, max_cphase + 0, runtime + 0
}' "$file"
