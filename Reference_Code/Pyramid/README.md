# ANNS
## HNSW

### 1. Shared Memory Parallelized HNSW
#### 1.1 Custom
1. Compile the code.
```bash
g++ ./hnsw.cpp -o hnsw.out -fopenmp
```
2. Run the code.
```bash
./hnsw.out <input_file_path> <input_size> <dimension> <num_of_levels> <l> <M> <num_threads> <query_input_filepath> 
```
```bash
./hnsw.out ../data/iris_dataset/iris.data.txt 150 4 5 12 13 1 ../data/iris_dataset/query/iris_query_points_1.txt 
```
#### 1.2 hnswlib
1. Compile the code.
```bash
g++ ./hnsw_hnswlib.cpp -o hnsw_hnswlib.out -fopenmp
```
2. Run the code.
```bash
./hnsw_hnswlib.out <input_filepath> <input_size> <dimension> <M> <ef_construction> <num_threads> <query_input_filepath> <query_input_size>
```
```bash
./hnsw_hnswlib.out ../data/iris_dataset/iris.data.txt 150 4 16 200 2 ../data/iris_dataset/query/iris_query_points_1.txt 11
```

### 2. Distributed HNSW (Naive)
#### 2.1 Custom
1. Compile the code.
```bash
mpic++ dist_hnsw.cpp -o dist_hnsw.out -fopenmp
```
2. Run the code.
```bash
mpirun -n <num_of_nodes> ./dist_hnsw.out <input_filepath> <input_size> <dimension> <num_of_levels> <l> <M> <num_threads> <randomize_input> <query_inpuy_file_path>
```
```bash
mpirun -n 4 ./dist_hnsw.out ../data/iris_dataset/iris.data.txt 150 4 5 12 15 2 0 ../data/iris_dataset/query/iris_query_points_1.txt
```

#### 2.2 hnswlib
1. Compile the code.
```bash
mpic++ dist_hnsw_hnswlib_v0.1.cpp -o dist_hnsw_hnswlib_v0.1.out
```
2. Run the code.
```bash
mpirun -n <num_of_nodes> ./dist_hnsw_hnswlib_v0.1.out <input_filepath> <input_size> <dimension> <M> <ef_construction> <randomize_input> <output_filepath>
```
```bash
mpirun -n 4 ./dist_hnsw_hnswlib_v0.1.out ../data/iris_dataset/iris.data.txt 150 4 16 200 0 ../output/file.csv
```

### 3. Distributed HNSW (Pyramid Approach)
#### 3.1 hnswlib
1. Compile the code.
```bash
mpic++ pyramid_hnsw_hnswlib_v2.1.cpp -o pyramid_hnsw_hnswlib_v2.1.out `pkg-config --cflags --libs opencv4`
```
```bash
mpic++ pyramid_hnsw_hnswlib_v2.1.cpp \
  -I$SCRATCH/apps/opencv-4.9.0/include/opencv4 \
  -L$SCRATCH/apps/opencv-4.9.0/lib64 \
  -lopencv_core -lopencv_imgproc -lopencv_highgui -lopencv_videoio -lopencv_imgcodecs \
  -o pyramid_hnsw_hnswlib_v2.1.out
```
2. Run the code.
```bash
mpirun -n <num_of_nodes> ./pyramid_hnsw_hnswlib_v2.1.out . <input_filepath> <input_size> <dimension> <sample_size> <m> <branching_factor> <M> <ef_construction> <output_filepath>
```
```bash
mpirun -n 4 ./pyramid_hnsw_hnswlib_v2.1.out ../data/iris_dataset/iris.data.txt 150 4 30 12 2 16 200 ../output/file.csv
```

## Tests

### 1. Setting up Glove dataset.
```bash
mkdir glove
cd glove
wget https://nlp.stanford.edu/data/glove.840B.300d.zip
unzip glove.840B.300d.zip
rm glove.840B.300d.zip
```

### 2. Installing hnswlib
```bash
git clone https://github.com/nmslib/hnswlib.git
```
### 3. Initialization
```bash
module load GCC OpenMPI

export PKG_CONFIG_PATH=$SCRATCH/apps/opencv-4.9.0/lib64/pkgconfig:$PKG_CONFIG_PATH
export LD_LIBRARY_PATH=$SCRATCH/apps/opencv-4.9.0/lib64:$LD_LIBRARY_PATH
```

### 4. Interactive Allocation
```bash
salloc --nodes=1 --ntasks-per-node=8 -t 00:05:00 --mem-per-cpu=5GB 
```

### 5.OpenCV Installation
```bash
mkdir -p $SCRATCH/apps/opencv_build
cd $SCRATCH/apps/opencv_build

git clone https://github.com/opencv/opencv.git
git clone https://github.com/opencv/opencv_contrib.git
cd opencv
git checkout 4.9.0
cd ../opencv_contrib
git checkout 4.9.0

mkdir -p ../build && cd ../build
cmake ../opencv \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=$SCRATCH/apps/opencv-4.9.0 \
  -DOPENCV_EXTRA_MODULES_PATH=../opencv_contrib/modules \
  -DBUILD_opencv_python3=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DBUILD_TESTS=OFF \
  -DWITH_OPENMP=ON \
  -DWITH_TBB=ON \
  -DWITH_EIGEN=ON \
  -DBUILD_SHARED_LIBS=ON \
  -DCMAKE_CXX_STANDARD=17

make -j$(nproc)
make install
```
