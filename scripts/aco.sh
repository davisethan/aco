#!/bin/bash

steps="$1"
iterations="$2"
input="$3"

# Build program
javac -classpath $(hadoop classpath) -d aco/build/classes aco/src/*.java
jar -cvf aco/build/AntColonyOptimizationJob.jar -C aco/build/classes/ .

# Explore parameter space
# Using Sobol sampling
while IFS=, read -r run alpha beta rho ants; do
    for (( it=0; it<=steps; it++ )); do
        # Update number of ants
        python3 aco/scripts/ants.py $ants
        hdfs dfs -rm input/ants.txt
        hdfs dfs -put ants.txt input

        # Run program
        hadoop jar aco/build/AntColonyOptimizationJob.jar AntColonyOptimization $iterations input/ants.txt output/${run}/${it} graph.txt 52 ${alpha} ${beta} 1000 ${rho} > logs/log_${run}_${it}.log 2>&1
    done
done < $input
