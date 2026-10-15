#!/bin/bash
#SBATCH --nodes=__NUM_OF_PROC_NODES__
#SBATCH --ntasks-per-node=__CPUS_PER_NODE__
#SBATCH --time=00:50:00
#SBATCH --mem-per-cpu=5GB
#SBATCH --job-name=__JOB_NAME__
#SBATCH --output=__OUTPUT_LOG_FILE__

# Load GCC and MPI modules.
module load GCC OpenMPI 

# Run naively distributed hnsw.
mpirun -n __NUM_OF_PROCS__ ../../dist_hnsw_hnswlib_v0.1.out __INPUT_FILE__ __INPUT_SIZE__ __DIM__ __M__ __EF_CONSTRUCTION__ __RANDOMIZE_INPUT__ __OUTPUT_FILE__
