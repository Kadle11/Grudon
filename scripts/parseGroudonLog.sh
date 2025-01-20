#!/bin/bash

# Input File
file=$1

# Use AWK to calculate MAX values for TraversalTimer, UpdateTimer, CPhase1Timer, and CPhase2Timer
awk -F, '
BEGIN {
    OFS=", "; 
    max_traversal = 0; 
    max_update = 0;
    max_cphase1 = 0;
    max_cphase2 = 0;
    runtime = 0;
    traversal_count = 0;
    max_traversal = 0;
}
$3 ~ /TraversalTimer/ {
    traversal_count++;
    if (traversal_count <= 3) {
        if ($5 > max_traversal) {
            max_traversal = $5;
        }
    } else if (traversal_count == 4) {
        max_traversal += $5;
    }
}
$3 ~ /UpdateTimer/ && $5 > max_update { 
    max_update = $5 
}
$3 ~ /CPhase1Timer/ && $5 > max_cphase1 { 
    max_cphase1 = $5 
}
$3 ~ /CPhase2Timer/ && $5 > max_cphase2 { 
    max_cphase2 = $5 
}
$3 ~ /Timer_0/ { 
    runtime = $5 
}
END {
    print max_traversal + max_update, max_cphase1 + max_cphase2, runtime + 0
}' "$file"
