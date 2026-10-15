#include <iostream>
#include <random>
#include <queue>
#include <vector>
#include <fstream>
#include <sstream>
#include <omp.h>
#include "../../hnswlib/hnswlib/hnswlib.h"

void read_txt(std::string filename, float* data, int input_size, int dimension) {
    
    std::ifstream infile(filename); // Open the file for reading

    if (infile) {
        std::string line;
        int i = 0;
        while (std::getline(infile, line)) {
            std::istringstream iss(line);
            float value;
            int j = 0;

            // Read each value (label followed by features)
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

    if (argc < 9) {
        std::cerr << "Usage: " << argv[0] << " <input_filepath> <input_size> <dimension> <M> <ef_construction> <num_threads> <query_input_filepath> <query_input_size>" << std::endl;
        return 1;
    }
    
    std::string input_filepath = argv[1];
    int input_size = std::stoi(argv[2]);
    int dimension = std::stoi(argv[3]);
    int M = std::stoi(argv[4]);
    int ef_construction = std::stoi(argv[5]);
    int p = std::stoi(argv[6]);
    std::string query_input_filepath = argv[7];
    int query_input_size = std::stoi(argv[8]);

    float* data = new float[input_size * dimension];
    read_txt(input_filepath, data, input_size, dimension);

    double hnsw_build_start = omp_get_wtime();

    // Initing index
    hnswlib::L2Space space(dimension);
    hnswlib::HierarchicalNSW<float>* alg_hnsw = new hnswlib::HierarchicalNSW<float>(&space, input_size, M, ef_construction);

    // Add data to index
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < input_size; i++) {
        alg_hnsw->addPoint(data + i * dimension, i);
    }

    double hnsw_build_end = omp_get_wtime();
    std::cout << "Time taken to build HNSW index: " << (hnsw_build_end - hnsw_build_start) << " seconds\n";

    hnswlib::BruteforceSearch<float>* alg_brute = new hnswlib::BruteforceSearch<float>(&space, input_size);
    
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < input_size; i++) {
        alg_brute->addPoint(data + i * dimension, i);
    }

    float* query_data = new float[query_input_size * dimension];
    read_txt(query_input_filepath, query_data, query_input_size, dimension);


    // Time the search
    double search_start = omp_get_wtime();
    std::vector<int> hnsw_ids(query_input_size);
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < query_input_size; i++) {
        std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_hnsw->searchKnn(query_data + i * dimension, 1);
        hnsw_ids[i] = result.top().second;
    }

    double search_end = omp_get_wtime();
    std::cout << "Time taken for search: " << (search_end - search_start) << " seconds\n";

    double correct = 0;
    #pragma omp parallel for num_threads(p) reduction(+:correct)
    for (int i = 0; i < query_input_size; i++) {
        std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_brute->searchKnn(query_data + i * dimension, 1);
        int brute_force_id = result.top().second;
        if (hnsw_ids[i] == brute_force_id) {
            correct++;
        }
    }

    float recall = correct / query_input_size;
    std::cout << "Recall: " << recall << "\n";

    delete[] data;
    delete alg_hnsw;
    return 0;
}
