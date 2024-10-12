#!/bin/bash

# Input file (log file containing the iterations)
LOG_FILE="$1"

# Output file for cleaned results
OUTPUT_FILE="pagerank_output.txt"

# Regular expression to match lines with the new format
# Example line format:
# "Vertex 5: 0.3569032443172878/0.016995003353625405"
REGEX="Vertex ([0-9]+): ([0-9]+\.[0-9]+)/([0-9]+\.[0-9]+)"

# Regular expression to detect iteration number
# Example: "[Proc 0] Iteration: 1"
ITERATION_REGEX="\[Proc 0\] Iteration: ([0-9]+)"

# Clear output file if it already exists
> "$OUTPUT_FILE"

# Initialize iteration variable
iteration=0

# Iterate through each line of the log file
while IFS= read -r line
do
    # Check if line matches the iteration format
    if [[ $line =~ $ITERATION_REGEX ]]; then
        # Extract iteration number from the match
        iteration="${BASH_REMATCH[1]}"
    fi

    # Check if line matches the expected vertex format
    if [[ $line =~ $REGEX ]]; then
        # Extract vertex number, PR value, and residual from the match
        vertex="${BASH_REMATCH[1]}"
        pr_value=$(printf "%.4f" "${BASH_REMATCH[2]}")
        residual=$(printf "%.4f" "${BASH_REMATCH[3]}")
        
        # Write iteration, vertex, PR value, and residual to the output file
        echo "(Iteration $iteration, Vertex $vertex, PageRank $pr_value, Residual $residual)" >> "$OUTPUT_FILE"
    fi
done < "$LOG_FILE"

echo "Extraction complete. Check the output in $OUTPUT_FILE"
