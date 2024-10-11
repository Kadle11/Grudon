#!/bin/bash

# Input log files
LOG_FILE_1="$1"
LOG_FILE_2="$2"

# Temporary files to store parsed data
TEMP_FILE_1="/tmp/pagerank_output_1.txt"
TEMP_FILE_2="/tmp/pagerank_output_2.txt"

# Output file for the results of the comparison
OUTPUT_FILE="pagerank_diff_output.txt"

# Regex to match vertex Ids, PageRank values, and residuals
REGEX="([0-9]+)[[:space:]]+([0-9]+\.[0-9]+)[[:space:]]+([0-9]+\.[0-9]+)"

# Regex to detect iteration number
ITERATION_REGEX="\[Leader\] Iteration ([0-9]+) complete"

# Function to parse log file
parse_log() {
    local log_file=$1
    local output_file=$2
    local iteration=0

    # Clear the output file if it exists
    > "$output_file"

    while IFS= read -r line; do
        # Check for iteration number
        if [[ $line =~ $ITERATION_REGEX ]]; then
            iteration="${BASH_REMATCH[1]}"
        fi

        # Check for vertex data
        if [[ $line =~ $REGEX ]]; then
            vertex="${BASH_REMATCH[1]}"
            pagerank=$(printf "%.4f" "${BASH_REMATCH[2]}")
            residual=$(printf "%.4f" "${BASH_REMATCH[3]}")
            
            # Store iteration, vertex, and residual to the output file
            echo "$iteration $vertex $residual" >> "$output_file"
        fi
    done < "$log_file"
}

# Parse both log files
parse_log "$LOG_FILE_1" "$TEMP_FILE_1"
parse_log "$LOG_FILE_2" "$TEMP_FILE_2"

# Compare the parsed results and find where residuals differ by more than 0.001
echo "Differences in residuals greater than 0.001:" > "$OUTPUT_FILE"

while IFS= read -r line1; do
    # Read corresponding line from the second temp file
    read -r line2 <&3

    # Extract iteration, vertex, and residuals from both files
    iteration1=$(echo "$line1" | awk '{print $1}')
    vertex1=$(echo "$line1" | awk '{print $2}')
    residual1=$(echo "$line1" | awk '{print $3}')
    
    iteration2=$(echo "$line2" | awk '{print $1}')
    vertex2=$(echo "$line2" | awk '{print $2}')
    residual2=$(echo "$line2" | awk '{print $3}')
    
    # Ensure the iterations and vertex IDs match
    if [[ "$iteration1" == "$iteration2" && "$vertex1" == "$vertex2" ]]; then
        # Calculate the difference in residuals
        diff=$(echo "$residual1 $residual2" | awk '{printf "%.4f", ($1 > $2 ? $1 - $2 : $2 - $1)}')

        # Check if the difference is greater than 0.001
        if (( $(echo "$diff > 0.001" | bc -l) )); then
            echo "Iteration $iteration1, Vertex $vertex1: Residual1 = $residual1, Residual2 = $residual2, Difference = $diff" >> "$OUTPUT_FILE"
        fi
    else
        echo "ERROR: Mismatch in iteration or vertex. Log files may not align properly." >> "$OUTPUT_FILE"
    fi
done < "$TEMP_FILE_1" 3<"$TEMP_FILE_2"

echo "Comparison complete. Check the differences in $OUTPUT_FILE"
