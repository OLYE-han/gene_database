#!/bin/bash

INPUT_DIR="../../../data/glove"
OUTPUT_DIR="../../output/m_center_sensitivity"
JOB_NAME_PREFIX="m_center_sensitivity_pyramid_v2"
INPUT_SIZE=512000
SAMPLE_SIZE=10240
INIT_M_CENTERS=128
BRANCHING_FACTOR=8
M=16
EF_CONSTRUCTION=200
DIM=300
NUM_OF_PROCS=256
NUM_OF_PROC_NODES=$((NUM_OF_PROCS / 32))
CPUS_PER_NODE=32

for i in {0..5}; do
    FACTOR=$((2**i))
    M_CENTERS=$((INIT_M_CENTERS * FACTOR))

    JOB_NAME="${JOB_NAME_PREFIX}_${M_CENTERS}"
    INPUT_FILE="${INPUT_DIR}/glove.${INPUT_SIZE}.txt"
    OUTPUT_FILE="$OUTPUT_DIR/raw/m_center_sensitivity_pyramid_v2_${M_CENTERS}.csv"
    OUTPUT_LOG_FILE="$OUTPUT_DIR/logs/m_center_sensitivity_pyramid_v2_${M_CENTERS}.log"
    TEMP_SCRIPT="temp_m_center_sensitivity_pyramid_v2_script_${M_CENTERS}.sh"

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