#!/bin/bash

INPUT_DIR="../../../data/glove"
OUTPUT_DIR="../../output/branching_factor_sensitivity"
JOB_NAME_PREFIX="branching_factor_sensitivity_pyramid_v2"
INPUT_SIZE=512000
SAMPLE_SIZE=10240
M_CENTERS=512
INIT_BRANCHING_FACTOR=1
M=16
EF_CONSTRUCTION=200
DIM=300
NUM_OF_PROCS=256
NUM_OF_PROC_NODES=$((NUM_OF_PROCS / 32))
CPUS_PER_NODE=32

for i in {0..6}; do
    FACTOR=$((2**i))
    BRANCHING_FACTOR=$((INIT_BRANCHING_FACTOR * FACTOR))

    JOB_NAME="${JOB_NAME_PREFIX}_${BRANCHING_FACTOR}"
    INPUT_FILE="${INPUT_DIR}/glove.${INPUT_SIZE}.txt"
    OUTPUT_FILE="$OUTPUT_DIR/raw/branching_factor_sensitivity_pyramid_v2_${BRANCHING_FACTOR}.csv"
    OUTPUT_LOG_FILE="$OUTPUT_DIR/logs/branching_factor_sensitivity_pyramid_v2_${BRANCHING_FACTOR}.log"
    TEMP_SCRIPT="temp_branching_factor_sensitivity_pyramid_v2_script_${BRANCHING_FACTOR}.sh"

    sed "s|__OUTPUT_FILE__|$OUTPUT_FILE|" ../run_pyramid_v2.sh | \
    sed "s|__NUM_OF_PROC_NODES__|$NUM_OF_PROC_NODES|" | \
    sed "s|__CPUS_PER_NODE__|$CPUS_PER_NODE|" | \
    sed "s|__JOB_NAME__|$JOB_NAME|" | \
    sed "s|__NUM_OF_PROCS__|$NUM_OF_PROCS|" | \
    sed "s|__INPUT_FILE__|$INPUT_FILE|" | \
    sed "s|__INPUT_SIZE__|$INPUT_SIZE|" | \
    sed "s|__DIM__|$DIM|" | \
    sed "s|__SAMPLE_SIZE__|$SAMPLE_SIZE|" | \
    sed "s|__M_CENTERS__|$M_CENTERS|" | \
    sed "s|__BRANCHING_FACTOR__|$BRANCHING_FACTOR|" | \
    sed "s|__M__|$M|" | \
    sed "s|__EF_CONSTRUCTION__|$EF_CONSTRUCTION|" | \
    sed "s|__OUTPUT_FILE__|$OUTPUT_FILE|" | \
    sed "s|__OUTPUT_LOG_FILE__|$OUTPUT_LOG_FILE|" > $TEMP_SCRIPT

    sbatch $TEMP_SCRIPT
    sleep 1
done
