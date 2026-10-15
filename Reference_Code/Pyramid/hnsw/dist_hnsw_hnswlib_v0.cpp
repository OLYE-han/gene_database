#include <iostream>
#include <random>
#include <queue>
#include <vector>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <random>
#include <omp.h>
#include <mpi.h>
#include "../../hnswlib/hnswlib/hnswlib.h"

void read_txt(std::string filename, float* data, int input_size, int dimension) {
    
    std::ifstream infile(filename); // Open the file for reading.

    if (infile) {
        std::string line;
        int i = 0;
        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            float value;
            int j = 0;

            while (iss >> value) {
                data[i * dimension + j] = value;
                j++;
            }
            i++;
        }
    } else {
        std::cerr << "Error opening file: " << filename << std::endl;
    }
}

int main(int argc, char** argv) {

    MPI_Init(&argc, &argv);

    int rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (argc < 10) {
        std::cerr << "Usage: " << argv[0] << " <input_filepath> <input_size> <dimension> <M> <ef_construction> <num_threads> <randomize_input> <query_input_filepath> <query_input_size>" << std::endl;
        return 1;
    }
    
    // Parse command line arguments into variables.
    std::string input_filepath = argv[1];
    int input_size = std::stoi(argv[2]);
    int dimension = std::stoi(argv[3]);
    int M = std::stoi(argv[4]);
    int ef_construction = std::stoi(argv[5]);
    int p = std::stoi(argv[6]);
    bool randomize_input = std::stoi(argv[7]);
    std::string query_input_filepath = argv[8];
    int query_input_size = std::stoi(argv[9]);

    float* data = new float[input_size * dimension];

    int local_input_size = input_size / world_size;
    if (rank == world_size - 1) {
        local_input_size = input_size - (world_size - 1) * local_input_size;
    }
    float* local_data = new float[input_size * dimension];

    if (rank == 0) {
        read_txt(input_filepath, data, input_size, dimension);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double hnsw_build_start = MPI_Wtime();

    if (rank == 0) {       
        // Randomize input data if specified.
        if (randomize_input) {
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(data, data + input_size * dimension, g);
        }

        // Copy local data for process 0.
        #pragma omp parallel for num_threads(p)
        for (int i = 0; i < local_input_size; ++i) {
            for (int j = 0; j < dimension; ++j) {
                local_data[i * dimension + j] = data[i * dimension + j];
            }
        }

        // Send data to other processes.
        for (int i = 1; i < world_size; ++i) {
            int start_index = i * local_input_size;
            int end_index = (i + 1) * local_input_size;
            if (i == world_size - 1) {
                end_index = input_size;
            }
            
            int num_elements = (end_index - start_index) * dimension;
            MPI_Send(data + start_index * dimension, num_elements, MPI_FLOAT, i, 1, MPI_COMM_WORLD);
        }
    } else {
        // Receiving data from process 0.
        MPI_Recv(local_data, local_input_size * dimension, MPI_FLOAT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    int label_start = 0;
    MPI_Exscan(&local_input_size, &label_start, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

    // Initiate hnsw index.
    hnswlib::L2Space space(dimension);
    hnswlib::HierarchicalNSW<float>* alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, local_input_size, M, ef_construction);

    // Add data to hnsw index.
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < local_input_size; i++) {
        alg_hnsw->addPoint(local_data + i * dimension, label_start + i);
    }

    double hnsw_build_end = MPI_Wtime();
    double local_hnsw_build_duration = hnsw_build_end - hnsw_build_start;
    double global_hnsw_build_duration;
    MPI_Reduce(&local_hnsw_build_duration, &global_hnsw_build_duration, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        std::cout << "Time taken to build HNSW index: " << global_hnsw_build_duration << " seconds\n";
    }

    // Initialize brute-force index.
    hnswlib::BruteforceSearch<float>* alg_brute = new hnswlib::BruteforceSearch<float>(&space, input_size);
    if (rank == 0) {
        // Add data to brute-force index.
        #pragma omp parallel for num_threads(p)
        for (int i = 0; i < input_size; i++) {
            alg_brute->addPoint(data + i * dimension, i);
        }
    }

    float* query_data = new float[query_input_size * dimension];
    read_txt(query_input_filepath, query_data, query_input_size, dimension);

    MPI_Barrier(MPI_COMM_WORLD);
    double search_start = MPI_Wtime();

    struct {
        float value;
        int id;
    } local_results[query_input_size], global_results[query_input_size];

    // Find nearest neighbors of the queries using HNSW.
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < query_input_size; ++i) {
        std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_hnsw->searchKnn(query_data + i * dimension, 1);
        float distance = result.top().first;
        int label_id = result.top().second;

        local_results[i].value = distance;
        local_results[i].id = label_id;
    }

    // Reduce to find min distance and corresponding label across all processes.
    MPI_Reduce(local_results, global_results, query_input_size, MPI_FLOAT_INT, MPI_MINLOC, 0, MPI_COMM_WORLD);

    double search_end = MPI_Wtime();
    double local_search_duration = search_end - search_start;
    double global_search_duration;
    MPI_Reduce(&local_search_duration, &global_search_duration, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        std::cout << "Time taken for search: " << global_search_duration << " seconds\n";

        // Calculate recall.
        double correct = 0;
        #pragma omp parallel for num_threads(p) reduction(+:correct)
        for (int i = 0; i < query_input_size; i++) {
            std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_brute->searchKnn(query_data + i * dimension, 1);
            int brute_force_id = result.top().second;
            if (global_results[i].id == brute_force_id) {
                correct++;
            }
        }

        float recall = correct / query_input_size;
        std::cout << "Recall: " << recall << "\n";
    }

    delete[] local_data;
    delete alg_hnsw;

    MPI_Finalize();
    return 0;
}
