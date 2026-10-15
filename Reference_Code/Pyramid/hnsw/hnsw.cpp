#include <vector>
#include <map>
#include <unordered_set>
#include <math.h>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <numeric>
#include <omp.h>

template <typename VALUE_TYPE>
using ValueType2DVector = std::vector<std::vector<VALUE_TYPE>>;

struct Node {
    
    int id;
    int node_max_level;
    std::vector<float> feature_vector;
    std::vector<std::vector<std::pair<int, float>>> neighbors;
};

struct HNSW {
    
    int max_level = -1;
    int entry_point = -1;
    std::vector<Node> nodes;
};

void read_txt(std::string filename, ValueType2DVector<float>* datamatrix) {
    
    std::ifstream infile(filename); // Open the file for reading
    std::vector<std::vector<float>> data; // Vector to hold the loaded data

    if (infile) {
        std::string line;
        while (std::getline(infile, line)) {
            std::vector<float> row;
            std::istringstream iss(line);
            float value;

            // Read each value (label followed by features)
            while (iss >> value) {
                row.push_back(value);
            }

            // Add the row (data vector) to the data vector
            data.push_back(row);
        }
        datamatrix->resize(data.size());
        for (int i = 0; i < data.size(); i++) {
            (*datamatrix)[i] = data[i];
        }
    } else {
        std::cerr << "Error opening file: " << filename << std::endl;
    }
}

std::vector<std::pair<int, float>> find_top_K(std::vector<std::pair<int, float>>& candidates, int k) {
    
    std::vector<std::pair<int, float>> top_k = std::vector<std::pair<int, float>>(candidates.begin(), candidates.end());
    std::sort(top_k.begin(), top_k.end(), [](const std::pair<int, float>& a, const std::pair<int, float>& b) {
        return a.second < b.second;
    });
    if (top_k.size() > k) {
        top_k.resize(k);
    }
    return top_k;
}

float find_min(std::vector<std::pair<int, float>>& nodes) {
    
    float min_distance = std::numeric_limits<float>::max();
    for (const auto& node : nodes) {
        if (node.second < min_distance) {
            min_distance = node.second;
        }
    }
    return min_distance;
}

float euclidean_distance(const std::vector<float>& a, const std::vector<float>& b) {
    
    float sum = 0.0;
    for (size_t i = 0; i < a.size(); ++i) {
        float diff = a[i] - b[i];
        sum += diff * diff;
    }
    return std::sqrt(sum);
}

int query_brute_force(std::vector<float>& query_vector, ValueType2DVector<float>& datamatrix) {

    std::pair<int, float> brute_force_results;
    brute_force_results.first = -1;
    brute_force_results.second = std::numeric_limits<float>::max();

    for (int i = 0; i < datamatrix.size(); ++i) {
        float distance = euclidean_distance(query_vector, datamatrix[i]);
        if (distance < brute_force_results.second) {
            brute_force_results.first = i;
            brute_force_results.second = distance;
        }
    }

   return brute_force_results.first;
}

std::vector<std::pair<int, float>> search_level(HNSW& hnsw, int level, std::vector<float> query_feature_vec, int factor, std::vector<std::pair<int, float>>& candidates) {
    
    std::vector<std::pair<int, float>> winners = std::vector<std::pair<int, float>>(candidates.begin(), candidates.end());
    std::vector<int> visited_node_ids;

    while (candidates.size() > 0 && find_min(candidates) <= find_min(winners)) {
        Node current_node = hnsw.nodes[candidates.back().first];
        candidates.pop_back();
        visited_node_ids.push_back(current_node.id);

        for (const auto& _neighbor : current_node.neighbors[level]) {
            int neighbor = _neighbor.first;
            if (std::find(visited_node_ids.begin(), visited_node_ids.end(), neighbor) != visited_node_ids.end()) {
                continue;
            }
            visited_node_ids.push_back(neighbor);

            float distance = euclidean_distance(query_feature_vec, hnsw.nodes[neighbor].feature_vector);
            candidates.push_back(std::make_pair(neighbor, distance));
            winners.push_back(std::make_pair(neighbor, distance));
        }
        winners = find_top_K(winners, factor);
    }
    return winners; 
}

std::vector<std::pair<int, float>> query_hnsw(HNSW& hnsw, std::vector<float> query_feature_vec, int k, int l) {
    
    std::vector<std::pair<int, float>> results;
    std::vector<std::pair<int, float>> candidates;
    candidates.push_back(std::make_pair(hnsw.entry_point, euclidean_distance(query_feature_vec, hnsw.nodes[hnsw.entry_point].feature_vector)));

    for (int j = hnsw.max_level; j > 0; --j) {
        candidates = search_level(hnsw, j, query_feature_vec, 1, candidates);
    }
    candidates = search_level(hnsw, 0, query_feature_vec, l, candidates);
    results = find_top_K(candidates, k);
    return results;
}

void build_hnsw(HNSW& hnsw, int input_size, ValueType2DVector<float>& datamatrix, int l, int M) {
    
    for (int i = 0; i < input_size; ++i) {
        const auto& feature_vec = datamatrix[i];

        Node* node = new Node();
        node->id = i;
        node->feature_vector = feature_vec;

        if (hnsw.entry_point == -1) {
            node->node_max_level = hnsw.max_level;
            node->neighbors.resize(hnsw.max_level + 1);
            hnsw.entry_point = i;
            hnsw.nodes[i] = *node;
            continue;
        }

        int level = rand() % (hnsw.max_level + 1);
        node->node_max_level = level;
        node->neighbors.resize(level + 1);
        std::vector<std::pair<int, float>> candidates;
        candidates.push_back(std::make_pair(hnsw.entry_point, euclidean_distance(feature_vec, hnsw.nodes[hnsw.entry_point].feature_vector)));

        for (int j = hnsw.max_level; j > level; --j) {
            candidates = search_level(hnsw, j, feature_vec, 1, candidates);
        }

        for (int j = level; j >= 0; --j) {
            candidates = search_level(hnsw, j, feature_vec, l, candidates);
            for (const auto& candidate : find_top_K(candidates, M)) {
                node->neighbors[j].push_back(std::make_pair(candidate.first, candidate.second));

                std::vector<std::pair<int, float>> candidate_neighbors = hnsw.nodes[candidate.first].neighbors[j];
                candidate_neighbors.push_back(std::make_pair(node->id, candidate.second));
                hnsw.nodes[candidate.first].neighbors[j] = find_top_K(candidate_neighbors, M);
            }
            candidates = find_top_K(candidates, 1);
        }
        hnsw.nodes[i] = *node;
    }
}

int main(int argc, char** argv) {

    if (argc < 9) {
        std::cerr << "Usage: " << argv[0] << " <input_filepath> <input_size> <dimension> <num_of_levels> <l> <M> <num_threads> <query_input_filepath>" << std::endl;
        return 1;
    }
    
    std::string input_filepath = argv[1];
    int input_size = std::stoi(argv[2]);
    int dimension = std::stoi(argv[3]);
    int num_of_levels = std::stoi(argv[4]);
    int l = std::stoi(argv[5]);
    int M = std::stoi(argv[6]);
    int p = std::stoi(argv[7]);
    std::string query_input_filepath = argv[8];

    ValueType2DVector<float> datamatrix;
    read_txt(input_filepath, &datamatrix);

    double hnsw_build_start = omp_get_wtime();
    
    HNSW hnsw;
    hnsw.max_level = num_of_levels - 1;
    hnsw.nodes.resize(input_size);

    build_hnsw(hnsw, input_size, datamatrix, l, M);

    double hnsw_build_end = omp_get_wtime();
    double hnsw_build_duration = hnsw_build_end - hnsw_build_start;
    std::cout << "Time taken to build HNSW index: " << hnsw_build_duration << " seconds\n";
    
    ValueType2DVector<float> query_datamatrix;
    read_txt(query_input_filepath, &query_datamatrix);

    double search_start = omp_get_wtime();
    
    int query_input_size = query_datamatrix.size();
    std::vector<int> hnsw_ids(query_input_size);
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < query_input_size; ++i) {
        std::vector<float> query = query_datamatrix[i];
        std::vector<std::pair<int, float>> results = query_hnsw(hnsw, query, 1, l);
        hnsw_ids[i] = results[0].first;
    }

    double search_end = omp_get_wtime();
    double search_duration = search_end - search_start;
    std::cout << "Time taken for search: " << search_duration << " seconds\n";

    float correct = 0;
    #pragma omp parallel for num_threads(p) reduction(+:correct)
    for (int i = 0; i < query_input_size; ++i) {
        std::vector<float> query = query_datamatrix[i];
        int hnsw_id = hnsw_ids[i];
        int brute_force_id = query_brute_force(query, datamatrix);
        if (brute_force_id == hnsw_id) {
            correct++;
        }
    }

    float mean_recall = correct / query_input_size;
    std::cout << "Mean Recall: " << mean_recall << std::endl;

    return 0;
}

