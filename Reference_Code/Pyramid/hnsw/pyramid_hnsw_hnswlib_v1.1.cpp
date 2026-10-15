#include <opencv2/opencv.hpp>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <sstream>
#include <string>
#include <mpi.h>
#include "../../hnswlib/hnswlib/hnswlib.h"

void read_txt(std::string filename, float *data, int input_size, int dimension)
{

    std::ifstream infile(filename); // Open the file for reading.

    if (infile)
    {
        std::string line;
        int i = 0;
        while (std::getline(infile, line))
        {
            std::istringstream iss(line);
            float value;
            int j = 0;

            while (iss >> value)
            {
                data[i * dimension + j] = value;
                j++;
            }
            i++;
        }
    }
    else
    {
        std::cerr << "Error opening file: " << filename << std::endl;
    }
}

void write_to_output(const std::string& filepath, int input_size, int world_size, int sample_size, int m, int branching_factor, 
                    float index_time, float search_time, double recall, double activation_rate) {
    
    std::ofstream file(filepath);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filepath << " for writing.\n";
        return;
    }

    file << input_size << "," 
         << world_size << ","
         << sample_size << ","
         << m << ","
         << branching_factor << ","
         << index_time << ","
         << search_time << ","
         << recall << ","
         << activation_rate << "\n";

    file.close();
}

void sample_input(float *datamatrix, int input_size, int dim,
                  int num_samples, cv::Mat &sampled_data)
{

    // Initialize random number generator.
    std::random_device rd;
    std::mt19937 rng(rd());

    // Create a vector of indices and shuffle.
    std::vector<int> indices(input_size);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), rng);

    // Initialize the output cv::Mat.
    sampled_data = cv::Mat(num_samples, dim, CV_32F);

    // Fill sampled_data.
    for (int i = 0; i < num_samples; ++i)
    {
        int idx = indices[i];
        // Use memcpy to copy the entire row at once.
        std::memcpy(sampled_data.ptr<float>(i), datamatrix + idx * dim, dim * sizeof(float));
    }
}

float euclidean_distance(const cv::Mat &a, const cv::Mat &b)
{

    CV_Assert(a.cols == b.cols && a.rows == 1 && b.rows == 1);
    float dist = 0.0f;
    for (int d = 0; d < a.cols; ++d)
    {
        float diff = a.at<float>(0, d) - b.at<float>(0, d);
        dist += diff * diff;
    }
    return std::sqrt(dist);
}

void greedy_grouping(const cv::Mat &centers, int w, hnswlib::HierarchicalNSW<float> &meta_hnsw,
                     int k_neighbors, std::vector<int> &center_to_group)
{

    int m = centers.rows;
    center_to_group.assign(m, -1);

    std::vector<int> group_sizes(w, 0);
    int max_group_size = (m + w - 1) / w;

    // Initializes the first w centers, each to a unique group.
    for (int i = 0; i < std::min(w, m); ++i)
    {
        center_to_group[i] = i;
        group_sizes[i]++;
    }

    // Assign remaining centers to groups based on their neighbors.
    for (int i = w; i < m; ++i)
    {
        std::vector<int> scores(w, 0);
        std::priority_queue<std::pair<float, hnswlib::labeltype>> neighbors = meta_hnsw.searchKnn(centers.ptr<float>(i), k_neighbors);
        while (!neighbors.empty())
        {
            int neighbor = neighbors.top().second;
            neighbors.pop();
            if (neighbor == i)
            {
                continue;
            }

            int group = center_to_group[neighbor];
            if (group != -1 && group_sizes[group] < max_group_size)
                scores[group]++;
        }

        std::priority_queue<std::pair<int, int>> best_groups;
        for (int group = 0; group < w; ++group)
        {
            if (group_sizes[group] < max_group_size)
            {
                best_groups.push({scores[group], group});
            }
        }

        bool best_group_found = false;
        while (!best_groups.empty())
        {
            int best_group = best_groups.top().second;
            best_groups.pop();

            if (group_sizes[best_group] < max_group_size)
            {
                center_to_group[i] = best_group;
                group_sizes[best_group]++;
                best_group_found = true;
            }

            if (best_group_found)
                break;
        }

        if (!best_group_found)
        {
            int best_group = std::min_element(group_sizes.begin(), group_sizes.end()) - group_sizes.begin();
            center_to_group[i] = best_group;
            group_sizes[best_group]++;
        }
    }
}

int main(int argc, char **argv)
{

    MPI_Init(&argc, &argv);

    int rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (argc < 9)
    {
        std::cerr << "Usage: " << argv[0] << " <input_filepath> <input_size> <dimension> <sample_size> <m> <branching_factor> <M> <ef_construction> <output_filepath>" << std::endl;
        return 1;
    }

    // Parse command line arguments into variables.
    std::string input_filepath = argv[1];
    int input_size = std::stoi(argv[2]);
    int dimension = std::stoi(argv[3]);
    int sample_size = std::stoi(argv[4]);
    int m = std::stoi(argv[5]);
    int k = std::stoi(argv[6]);
    int M = std::stoi(argv[7]);
    int ef_construction = std::stoi(argv[8]);
    std::string output_filepath = argv[9];

    float *data;
    int local_input_size;
    float *local_data;
    std::vector<int> local_data_ids(world_size);

    hnswlib::L2Space meta_space(dimension);
    hnswlib::HierarchicalNSW<float> *meta_hnsw;

    std::vector<int> center_to_group(m);

    if (rank == 0)
    {
        data = new float[input_size * dimension];
        read_txt(input_filepath, data, input_size, dimension);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double hnsw_build_start = MPI_Wtime();

    if (rank == 0)
    {
        cv::Mat sampled_data;
        sample_input(data, input_size, dimension, sample_size, sampled_data);

        std::cout << "Sampled data size: " << sample_size << " x " << dimension << std::endl;

        // Number of groups.
        int w = world_size;

        // Output labels and centers.
        cv::Mat labels;
        cv::Mat centers;

        float *m_centers;

        // kmeans flags
        cv::TermCriteria criteria(cv::TermCriteria::EPS + cv::TermCriteria::MAX_ITER, 100, 0.1);
        int attempts = 3;
        int flags = cv::KMEANS_PP_CENTERS;

        // Run kmeans
        cv::kmeans(sampled_data, m, labels, criteria, attempts, flags, centers);

        meta_hnsw = new hnswlib::HierarchicalNSW<float>(&meta_space, m, M, ef_construction);

        for (int i = 0; i < m; i++)
        {
            meta_hnsw->addPoint(centers.ptr<float>(i), i);
        }

        std::cout << "Meta HNSW index built with " << m << " centers.\n";

        greedy_grouping(centers, w, *meta_hnsw, 5, center_to_group);

        std::cout << "Greedy grouping completed.\n";

        std::vector<std::vector<float *>> data_to_send(world_size);
        std::vector<std::vector<int>> data_ids_to_send(world_size);
        for (int i = 0; i < input_size; i++)
        {
            std::priority_queue<std::pair<float, hnswlib::labeltype>> centers = meta_hnsw->searchKnn(data + i * dimension, k);
            std::unordered_set<int> visited_groups;
            while (!centers.empty())
            {
                int center = centers.top().second;
                centers.pop();
                int group = center_to_group[center];

                if (visited_groups.find(group) != visited_groups.end())
                {
                    continue;
                }

                data_ids_to_send[group].push_back(i);
                data_to_send[group].push_back(data + i * dimension);
                visited_groups.insert(group);
            }
        }

        for (int i = 1; i < world_size; i++)
        {
            int data_size_to_send_i = data_to_send[i].size();
            std::vector<int> &data_ids_to_send_i = data_ids_to_send[i];
            float **data_ptrs = data_to_send[i].data();
            float *data_to_send_i = new float[data_size_to_send_i * dimension];
            
            for (int j = 0; j < data_size_to_send_i; ++j)
            {
                std::memcpy(data_to_send_i + j * dimension, data_ptrs[j], dimension * sizeof(float));
            }

            MPI_Send(&data_size_to_send_i, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(data_to_send_i, data_size_to_send_i * dimension, MPI_FLOAT, i, 0, MPI_COMM_WORLD);
            MPI_Send(data_ids_to_send_i.data(), data_size_to_send_i, MPI_INT, i, 0, MPI_COMM_WORLD);
        }

        // Allocate local data for rank 0.
        local_input_size = data_to_send[0].size();
        local_data_ids = data_ids_to_send[0];
        local_data = new float[local_input_size * dimension];
        float **data_ptrs = data_to_send[0].data();
        
        for (int i = 0; i < local_input_size; ++i)
        {
            std::memcpy(local_data + i * dimension, data_ptrs[i], dimension * sizeof(float));
        }
    }
    else
    {
        // Receiving data from process 0.
        MPI_Recv(&local_input_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        local_data = new float[local_input_size * dimension];
        MPI_Recv(local_data, local_input_size * dimension, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        local_data_ids.resize(local_input_size);
        MPI_Recv(local_data_ids.data(), local_input_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    // Initiate hnsw index.
    hnswlib::L2Space space(dimension);
    hnswlib::HierarchicalNSW<float> *local_hnsw = new hnswlib::HierarchicalNSW<float>(&space, local_input_size, M, ef_construction);

    // Add data to hnsw index.
    for (int i = 0; i < local_input_size; i++)
    {
        local_hnsw->addPoint(local_data + i * dimension, local_data_ids[i]);
    }

    double hnsw_build_end = MPI_Wtime();
    double local_hnsw_build_duration = hnsw_build_end - hnsw_build_start;
    double global_hnsw_build_duration;
    MPI_Reduce(&local_hnsw_build_duration, &global_hnsw_build_duration, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        std::cout << "Time taken to build HNSW index: " << global_hnsw_build_duration << " seconds\n";
    }

    int query_input_size = input_size;
    float *query_data;
    if (rank == 0)
    {
        query_data = data;
    }

    int local_query_input_size;
    float *local_query_data;
    std::vector<int> local_query_indices;

    double activation_rate = 0.0;

    MPI_Barrier(MPI_COMM_WORLD);
    double search_start = MPI_Wtime();

    if (rank == 0)
    {
        std::vector<std::vector<int>> query_indices(world_size);
        std::vector<std::vector<float *>> query_data_to_send(world_size);
        double activations = 0.0;
        for (int i = 0; i < query_input_size; ++i)
        {
            float *query = query_data + i * dimension;
            std::priority_queue<std::pair<float, hnswlib::labeltype>> result = meta_hnsw->searchKnn(query, k);

            std::vector<int> visited_groups;
            while (result.size() > 0)
            {
                int center = result.top().second;
                result.pop();

                int group = center_to_group[center];
                if (std::find(visited_groups.begin(), visited_groups.end(), group) != visited_groups.end())
                {
                    continue;
                }

                query_indices[group].push_back(i);
                query_data_to_send[group].push_back(query);
                visited_groups.push_back(group);
                activations++;
            }
        }
        for (int i = 1; i < world_size; ++i)
        {
            int query_data_size_to_send_i = query_indices[i].size();
            MPI_Send(&query_data_size_to_send_i, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            if (query_data_size_to_send_i > 0)
            {
                float **query_ptrs = query_data_to_send[i].data();
                float *query_data_to_send_i = new float[query_data_size_to_send_i * dimension];
                for (int j = 0; j < query_data_size_to_send_i; ++j)
                {
                    std::memcpy(query_data_to_send_i + j * dimension, query_ptrs[j], dimension * sizeof(float));
                }

                MPI_Send(query_indices[i].data(), query_data_size_to_send_i, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(query_data_to_send_i, query_data_size_to_send_i * dimension, MPI_FLOAT, i, 0, MPI_COMM_WORLD);
            }
        }

        local_query_input_size = query_indices[0].size();
        if (local_query_input_size > 0)
        {
            local_query_indices.resize(local_query_input_size);
            std::copy(query_indices[0].begin(), query_indices[0].end(), local_query_indices.begin());

            local_query_data = new float[local_query_input_size * dimension];
            float **query_ptrs = query_data_to_send[0].data();
            for (int j = 0; j < local_query_input_size; ++j)
            {
                std::memcpy(local_query_data + j * dimension, query_ptrs[j], dimension * sizeof(float));
            }
        }

        std::cout << "Query data distibution is completed.\n";

        activation_rate = activations / (query_input_size * world_size);
        std::cout << "Activation rate: " << activation_rate << std::endl;
    }
    else
    {
        MPI_Recv(&local_query_input_size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (local_query_input_size > 0)
        {
            local_query_indices.resize(local_query_input_size);
            MPI_Recv(local_query_indices.data(), local_query_input_size, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            local_query_data = new float[local_query_input_size * dimension];
            MPI_Recv(local_query_data, local_query_input_size * dimension, MPI_FLOAT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }

    struct
    {
        float value;
        int id;
    } local_results[query_input_size], global_results[query_input_size];

    for (int i = 0; i < query_input_size; ++i)
    {
        local_results[i].id = -1;
        local_results[i].value = std::numeric_limits<float>::max();
    }

    if (local_query_input_size > 0)
    {
        // Find nearest neighbors of the queries using HNSW.
        for (int i = 0; i < local_query_input_size; ++i)
        {
            int query_index = local_query_indices[i];
            float *query = local_query_data + i * dimension;
            std::priority_queue<std::pair<float, hnswlib::labeltype>> result = local_hnsw->searchKnn(query, 1);
            float distance = result.top().first;
            int label_id = result.top().second;

            local_results[query_index].value = distance;
            local_results[query_index].id = label_id;
        }
    }

    // Gather results from all processes.
    MPI_Reduce(local_results, global_results, query_input_size, MPI_FLOAT_INT, MPI_MINLOC, 0, MPI_COMM_WORLD);

    double search_end = MPI_Wtime();
    double local_search_duration = search_end - search_start;
    double global_search_duration;
    MPI_Reduce(&local_search_duration, &global_search_duration, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        std::cout << "Time taken for search: " << global_search_duration << " seconds\n";

        double correct = 0;
        for (int i = 0; i < query_input_size; i++)
        {
            if (global_results[i].id == i)
            {
                correct++;
            }
        }

        float recall = correct / query_input_size;
        std::cout << "Recall: " << recall << std::endl;

        write_to_output(output_filepath, input_size, world_size, sample_size, m, k, global_hnsw_build_duration, global_search_duration, recall, activation_rate);
    }

    MPI_Finalize();

    return 0;
}
