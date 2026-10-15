#!/bin/bash

INPUT_DIR="../../../data/glove"
OUTPUT_DIR="../../output/weak_scaling"
JOB_NAME_PREFIX="weak_scaling_naive_dist"
INIT_INPUT_SIZE=2000
M=16
EF_CONSTRUCTION=200
DIM=300
RANDOMIZE_INPUT=1

for i in {0..9}; do
    NUM_OF_PROCS=$((2**i))
    if [ $NUM_OF_PROCS -le 32 ]; then
        NUM_OF_PROC_NODES=1
        CPUS_PER_NODE=$NUM_OF_PROCS
    else
        NUM_OF_PROC_NODES=$((NUM_OF_PROCS / 32))
        CPUS_PER_NODE=32
    fi

    INPUT_SIZE=$((NUM_OF_PROCS * INIT_INPUT_SIZE))

    JOB_NAME="${JOB_NAME_PREFIX}_${NUM_OF_PROCS}"
    INPUT_FILE="${INPUT_DIR}/glove.${INPUT_SIZE}.txt"
    OUTPUT_FILE="$OUTPUT_DIR/raw/weak_scaling_naive_dist_${NUM_OF_PROCS}.csv"
    OUTPUT_LOG_FILE="$OUTPUT_DIR/logs/weak_scaling_naive_dist_${NUM_OF_PROCS}.log"
    TEMP_SCRIPT="temp_weak_scaling_naive_dist_script_${NUM_OF_PROCS}.sh"

    sed "s|__OUTPUT_FILE__|$OUTPUT_FILE|" ../run_naive_dist.sh | \
    sed "s|__NUM_OF_PROC_NODES__|$NUM_OF_PROC_NODES|" | \
    sed "s|__CPUS_PER_NODE__|$CPUS_PER_NODE|" | \
    sed "s|__JOB_NAME__|$JOB_NAME|" | \
    sed "s|__NUM_OF_PROCS__|$NUM_OF_PROCS|" | \
    sed "s|__INPUT_FILE__|$INPUT_FILE|" | \
    sed "s|__INPUT_SIZE__|$INPUT_SIZE|" | \
    sed "s|__DIM__|$DIM|" | \
    sed "s|__M__|$M|" | \
    sed "s|__EF_CONSTRUCTION__|$EF_CONSTRUCTION|" | \
    sed "s|__OUTPUT_FILE__|$OUTPUT_FILE|" | \
    sed "s|__RANDOMIZE_INPUT__|$RANDOMIZE_INPUT|" | \
    sed "s|__OUTPUT_LOG_FILE__|$OUTPUT_LOG_FILE|" > $TEMP_SCRIPT

    sbatch $TEMP_SCRIPT
    sleep 1
done
