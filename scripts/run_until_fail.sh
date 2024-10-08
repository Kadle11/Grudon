#!/bin/bash

while true; do 
	output=$(time mpirun -np 2 ./bin/Debug/Groudon -g /proj/prismgt-PG0/vrao79/galois-graphs/mini_graph.gr -c 1 -m 1 -t 2 | tee HelloWorld.log) 
	if ! echo "$output" | grep -q "Iteration: 39"; then
		if echo "$output" | grep -q "Iteration: 10"; then
			break
		fi 
	fi 
	sleep 1 
done
