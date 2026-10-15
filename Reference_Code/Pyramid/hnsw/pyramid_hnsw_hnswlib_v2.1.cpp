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

using Float2DVector = std::vector<std::vector<float>>;
using Float2DPairVector = std::vector<std::pair<int, std::vector<float>>>;

void read_txt(std::string filename, Float2DVector *datamatrix,
             int no_of_datapoints, int dim, int rank, int world_size, int offset = 0)
{

    std::ifstream infile(filename); // Open the file for reading.
    if (!infile.is_open())
    {
        // Handle file opening error.
        std::cerr << "Error: Unable to open the file " << filename << std::endl;
        return;
    }

    int chunk_size = no_of_datapoints / world_size;
    int start_idx = rank * chunk_size;

    if (rank == world_size - 1)
    {
        chunk_size = no_of_datapoints - rank * chunk_size;
    }

    // Skip lines up to start_idx.
    std::string line;
    for (int i = 0; i < start_idx + offset && std::getline(infile, line); ++i);

    for (int i = 0; i < chunk_size; ++i)
    {
        if (std::getline(infile, line))
        {
            std::vector<float> row;
            std::istringstream iss(line);
            float value;

            // Read each value (label followed by features).
            while (iss >> value)
            {
                row.push_back(value);
            }
            (*datamatrix).push_back(row);
        }
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

std::unique_ptr<float[]> sample_input(const std::vector<std::vector<float>> &datamatrix, 
    int input_size, int dim, int num_samples)
{

     // Initialize random number generator.
     std::random_device rd;
     std::mt19937 rng(rd());
 
     // Create a vector of indices and shuffle.
     std::vector<int> indices(input_size);
     std::iota(indices.begin(), indices.end(), 0);
     std::shuffle(indices.begin(), indices.end(), rng);
 
     // Allocate memory safely.
     std::unique_ptr<float[]> sampled_data(new float[num_samples * dim]);
 
     // Fill sampled_data.
     for (int i = 0; i < num_samples; ++i)
     {
         int idx = indices[i];
         std::memcpy(sampled_data.get() + i * dim, datamatrix[idx].data(), dim * sizeof(float));
     }
 
     return sampled_data;
}

void greedy_grouping(const std::vector<float> &centers, int w, int m, int dim, hnswlib::HierarchicalNSW<float> &meta_hnsw,
                     int k_neighbors, std::vector<int> &center_to_group)
{

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
        std::priority_queue<std::pair<float, hnswlib::labeltype>> neighbors = meta_hnsw.searchKnn(centers.data() + i * dim, k_neighbors);
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

double distribute_data_matrix(Float2DVector &datamatrix, Float2DPairVector &local_datamatrix, hnswlib::HierarchicalNSW<float> &meta_hnsw,
                            std::vector<int> &center_to_group, int k, int input_size, int dim, int rank, int world_size)
{
    
    int label_offset = rank * (input_size / world_size);
    Float2DVector data_to_send(world_size);

    double activations = 0.0;
    for (int i = 0; i < datamatrix.size(); ++i)
    {
        std::priority_queue<std::pair<float, hnswlib::labeltype>> centers = meta_hnsw.searchKnn(datamatrix[i].data(), k);

        std::unordered_set<int> visited_groups;
        int label = label_offset + i;
        while (!centers.empty())
        {
            int center = centers.top().second;
            centers.pop();
            int group = center_to_group[center];

            if (visited_groups.find(group) != visited_groups.end())
            {
                continue;
            }

            // Vector to be sent to a process -> a set of vectors of (label + data vector).
            data_to_send[group].push_back(static_cast<float>(label));
            data_to_send[group].insert(data_to_send[group].end(), datamatrix[i].begin(), datamatrix[i].end());
            visited_groups.insert(group);
            activations++;
        }
    }

    std::vector<float> send_buffer;
    std::vector<int> send_counts(world_size), recv_counts(world_size);
    // Flatten the data_to_send vector into send_buffer.
    for (int i = 0; i < world_size; ++i)
    {
        send_counts[i] = data_to_send[i].size();
        send_buffer.insert(send_buffer.end(), data_to_send[i].begin(), data_to_send[i].end());
    }

    // Gather the sizes of the data to be received from each process.
    MPI_Alltoall(send_counts.data(), 1, MPI_INT, recv_counts.data(), 1, MPI_INT, MPI_COMM_WORLD);

    // Calculate the displacements (offsets) for send and receive buffers for each processor.
    std::vector<int> send_offsets(world_size, 0), recv_offsets(world_size, 0);
    for (int i = 1; i < world_size; ++i)
    {
        send_offsets[i] = send_offsets[i - 1] + send_counts[i - 1];
        recv_offsets[i] = recv_offsets[i - 1] + recv_counts[i - 1];
    }

    int total_recv_count = recv_offsets.back() + recv_counts.back();
    std::vector<float> recv_buffer(total_recv_count);

    MPI_Alltoallv(send_buffer.data(), send_counts.data(), send_offsets.data(), MPI_FLOAT,
                  recv_buffer.data(), recv_counts.data(), recv_offsets.data(), MPI_FLOAT,
                  MPI_COMM_WORLD);
    
    // Process the received flattened data and fill the local_datamatrix.
    for (int i = 0; i < total_recv_count; i += dim + 1)  // label + data vector -> dim + 1
    {
        int label = static_cast<int>(recv_buffer[i]);
        std::vector<float> features(recv_buffer.begin() + i + 1, recv_buffer.begin() + i + 1 + dim);
        local_datamatrix.emplace_back(label, features);

    }

    return activations;
}

int main(int argc, char **argv)
{

    MPI_Init(&argc, &argv);

    int rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (argc < 9)
    {
        std::cerr << "Usage: " << argv[0] << " <input_filepath> <input_size> <dimension> <global_sample_size> <m> <branching_factor> <M> <ef_construction> <output_filepath>" << std::endl;
        return 1;
    }

    // Parse command line arguments into variables.
    std::string input_filepath = argv[1];
    int input_size = std::stoi(argv[2]);
    int dimension = std::stoi(argv[3]);
    int global_sample_size = std::stoi(argv[4]);
    int m = std::stoi(argv[5]);
    int k = std::stoi(argv[6]);
    int M = std::stoi(argv[7]);
    int ef_construction = std::stoi(argv[8]);
    std::string output_filepath = argv[9];

    Float2DVector datamatrix;
    read_txt(input_filepath, &datamatrix, input_size, dimension, rank, world_size);
    int chunk_size = datamatrix.size();

    hnswlib::L2Space meta_space(dimension);
    hnswlib::HierarchicalNSW<float> *meta_hnsw;
    std::string meta_hnsw_path = "meta_hnsw.bin";

    MPI_Barrier(MPI_COMM_WORLD);
    double hnsw_build_start = MPI_Wtime();

    int sample_size = global_sample_size / world_size;
    std::unique_ptr<float[]> sampled_data = sample_input(datamatrix, chunk_size, dimension, sample_size);

    std::unique_ptr<float[]> global_sampled_data;
    if (rank == 0) {
        global_sampled_data = std::make_unique<float[]>(global_sample_size * dimension);
    }
    MPI_Gather(sampled_data.get(), sample_size * dimension, MPI_FLOAT,
            global_sampled_data.get(), sample_size * dimension, MPI_FLOAT,
            0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        std::cout << "Sampled data size: " << global_sample_size << " x " << dimension << std::endl;
    }

    std::vector<int> center_to_group(m);
    std::vector<float> m_centers(m * dimension);

    if (rank == 0)
    {
        cv::Mat sampled_data_mat(global_sample_size, dimension, CV_32F, global_sampled_data.get());

        int w = world_size;

        // Output labels and centers.
        cv::Mat labels;
        cv::Mat centers;

        // Declare kmeans flags.
        cv::TermCriteria criteria(cv::TermCriteria::EPS + cv::TermCriteria::MAX_ITER, 100, 0.1);
        int attempts = 3;
        int flags = cv::KMEANS_PP_CENTERS;

        // Run kmeans for m centers.
        cv::kmeans(sampled_data_mat, m, labels, criteria, attempts, flags, centers);

        for (int i = 0; i < m; ++i)
        {
            std::memcpy(m_centers.data() + i * dimension, centers.ptr<float>(i), dimension * sizeof(float));
        }
    }

    MPI_Bcast(m_centers.data(), m * dimension, MPI_FLOAT, 0, MPI_COMM_WORLD);

    meta_hnsw = new hnswlib::HierarchicalNSW<float>(&meta_space, m, M, ef_construction);
    for (int i = 0; i < m; i++)
    {
        meta_hnsw->addPoint(m_centers.data() + i * dimension, i);
    }

    if (rank == 0)
    {
        std::cout << "Meta HNSW index built with " << m << " centers.\n";
    }

    if (rank == 0)
    {
        greedy_grouping(m_centers, world_size, m, dimension, *meta_hnsw, k, center_to_group);
    }
    MPI_Bcast(center_to_group.data(), m, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        std::cout << "Greedy grouping completed.\n";
    }

    Float2DPairVector local_datamatrix;
    double _ = distribute_data_matrix(datamatrix, local_datamatrix, *meta_hnsw, center_to_group, k, input_size, dimension, rank, world_size);

    int local_input_size = local_datamatrix.size();

    // Initiate hnsw index.
    hnswlib::L2Space space(dimension);
    hnswlib::HierarchicalNSW<float> *local_hnsw = new hnswlib::HierarchicalNSW<float>(&space, local_input_size, M, ef_construction);

    // Add data to hnsw index.
    for (int i = 0; i < local_input_size; i++)
    {
        local_hnsw->addPoint(local_datamatrix[i].second.data(), local_datamatrix[i].first);
    }

    double hnsw_build_end = MPI_Wtime();
    double local_hnsw_build_duration = hnsw_build_end - hnsw_build_start;
    double global_hnsw_build_duration;
    MPI_Reduce(&local_hnsw_build_duration, &global_hnsw_build_duration, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    if (rank == 0)
    {
        std::cout << "Time taken to build HNSW index: " << global_hnsw_build_duration << " seconds\n";
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double search_start = MPI_Wtime();

    int query_input_size = input_size;
    Float2DPairVector local_query_datamatrix;
    double activations = distribute_data_matrix(datamatrix, local_query_datamatrix, *meta_hnsw, center_to_group, k, query_input_size, dimension, rank, world_size);

    if (rank == 0)
    {
        std::cout << "Query data distibution is completed.\n";
    }

    int local_query_input_size = local_query_datamatrix.size();

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
            std::vector<float> &query = local_query_datamatrix[i].second;
            std::priority_queue<std::pair<float, hnswlib::labeltype>> results = local_hnsw->searchKnn(query.data(), 1);
            float distance = results.top().first;
            int label = results.top().second;

            int query_label = local_query_datamatrix[i].first;
            local_results[query_label].value = distance;
            local_results[query_label].id = label;
        }
    }

    // Gather results from all processes.
    MPI_Reduce(local_results, global_results, query_input_size, MPI_FLOAT_INT, MPI_MINLOC, 0, MPI_COMM_WORLD);
    
    double search_end = MPI_Wtime();
    double local_search_duration = search_end - search_start;
    double global_search_duration;
    MPI_Reduce(&local_search_duration, &global_search_duration, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);

    double global_activations;
    MPI_Reduce(&activations, &global_activations, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0)
    {
        double global_activation_rate = global_activations / (query_input_size * world_size);
        std::cout << "Activation rate: " << global_activation_rate << std::endl;
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

        write_to_output(output_filepath, input_size, world_size, global_sample_size, m, k, global_hnsw_build_duration, global_search_duration, recall, global_activation_rate);
    }

    MPI_Finalize();

    return 0;
}
