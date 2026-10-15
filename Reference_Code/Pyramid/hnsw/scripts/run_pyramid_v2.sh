#!/bin/bash
#SBATCH --nodes=__NUM_OF_PROC_NODES__
#SBATCH --ntasks-per-node=__CPUS_PER_NODE__
#SBATCH --time=02:00:00
#SBATCH --mem-per-cpu=5GB
#SBATCH --job-name=__JOB_NAME__
#SBATCH --output=__OUTPUT_LOG_FILE__

# Load GCC and MPI modules.
module load GCC OpenMPI 

export PKG_CONFIG_PATH=$SCRATCH/apps/opencv-4.9.0/lib64/pkgconfig:$PKG_CONFIG_PATH
export LD_LIBRARY_PATH=$SCRATCH/apps/opencv-4.9.0/lib64:$LD_LIBRARY_PATH

# Run pyramid v2 hnsw.
mpirun -n __NUM_OF_PROCS__ ../../pyramid_hnsw_hnswlib_v2.1.out __INPUT_FILE__ __INPUT_SIZE__ __DIM__ __SAMPLE_SIZE__ __M_CENTERS__ __BRANCHING_FACTOR__ __M__ __EF_CONSTRUCTION__ __OUTPUT_FILE__
 
