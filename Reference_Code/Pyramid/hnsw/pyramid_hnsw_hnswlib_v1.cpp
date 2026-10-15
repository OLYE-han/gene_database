#include <opencv2/opencv.hpp>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <algorithm>
#include <numeric>
#include <mpi.h>
#include <omp.h>
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

void sample_input(float *datamatrix, int input_size, int dim,
                  int num_samples, cv::Mat &sampled_data)
{

    srand(time(0));
    sampled_data = cv::Mat(num_samples, dim, CV_32F);

    std::vector<int> indices(input_size);
    iota(indices.begin(), indices.end(), 0);
    random_shuffle(indices.begin(), indices.end());

    for (int i = 0; i < num_samples; ++i)
    {
        int idx = indices[i];
        for (int j = 0; j < dim; ++j)
        {
            sampled_data.at<float>(i, j) = datamatrix[idx * dim + j];
        }
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
                     int k_neighbors, std::vector<int> &center_to_group, int p)
{

    int m = centers.rows;
    center_to_group.assign(m, -1);

    std::vector<int> group_sizes(w, 0);
    int max_group_size = (m + w - 1) / w;

    // Initializes the first w centers, each to a unique group.
    #pragma omp parallel for num_threads(p)
    for (int i = 0; i < std::min(w, m); ++i)
    {
        center_to_group[i] = i;
        group_sizes[i]++;
    }

    omp_lock_t locks[w];
    for (int i = 0; i < w; ++i)
        omp_init_lock(&locks[i]);

    // Assign remaining centers to groups based on their neighbors.
    #pragma omp parallel for num_threads(p)
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

            omp_set_lock(&locks[best_group]);
            if (group_sizes[best_group] < max_group_size)
            {
                center_to_group[i] = best_group;
                group_sizes[best_group]++;
                best_group_found = true;
            }
            omp_unset_lock(&locks[best_group]);

            if (best_group_found)
                break;
        }

        if (!best_group_found)
        {
            int best_group = std::min_element(group_sizes.begin(), group_sizes.end()) - group_sizes.begin();
            center_to_group[i] = best_group;

            omp_set_lock(&locks[best_group]);
            group_sizes[best_group]++;
            omp_unset_lock(&locks[best_group]);
        }
    }
}

int main(int argc, char **argv)
{

    MPI_Init(&argc, &argv);

    int rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    if (argc < 12)
    {
        std::cerr << "Usage: " << argv[0] << " <input_filepath> <input_size> <dimension> <sample_size> <m> <branching_factor> <M> <ef_construction> <num_threads> <query_input_filepath> <query_input_size>" << std::endl;
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
    int p = std::stoi(argv[9]);
    std::string query_input_filepath = argv[10];
    int query_input_size = std::stoi(argv[11]);

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

        #pragma omp parallel for num_threads(p)
        for (int i = 0; i < m; i++)
        {
            meta_hnsw->addPoint(centers.ptr<float>(i), i);
        }

        greedy_grouping(centers, w, *meta_hnsw, 5, center_to_group, p);

        std::vector<std::vector<float *>> data_to_send(world_size);
        std::vector<std::vector<int>> data_ids_to_send(world_size);
        for (int i = 0; i < input_size; i++)
        {
            std::priority_queue<std::pair<float, hnswlib::labeltype>> result = meta_hnsw->searchKnn(data + i * dimension, 1);
            int center = result.top().second;
            int group = center_to_group[center];

            data_ids_to_send[group].push_back(i);
            data_to_send[group].push_back(data + i * dimension);
        }

        for (int i = 1; i < world_size; i++)
        {
            int data_size_to_send_i = data_to_send[i].size();
            std::vector<int> &data_ids_to_send_i = data_ids_to_send[i];
            float **data_ptrs = data_to_send[i].data();
            float *data_to_send_i = new float[data_size_to_send_i * dimension];
            
            #pragma omp parallel for num_threads(p)
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
        
        #pragma omp parallel for num_threads(p)
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
    #pragma omp parallel for num_threads(p)
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

    float *query_data;
    if (rank == 0)
    {
        query_data = new float[query_input_size * dimension];
        read_txt(query_input_filepath, query_data, query_input_size, dimension);
    }

    int local_query_input_size;
    float *local_query_data;
    std::vector<int> local_query_indices;

    MPI_Barrier(MPI_COMM_WORLD);
    double search_start = MPI_Wtime();

    if (rank == 0)
    {
        std::vector<std::vector<int>> query_indices(world_size);
        std::vector<std::vector<float *>> query_data_to_send(world_size);
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
                #pragma omp parallel for num_threads(p)
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
            #pragma omp parallel for num_threads(p)
            for (int j = 0; j < local_query_input_size; ++j)
            {
                std::memcpy(local_query_data + j * dimension, query_ptrs[j], dimension * sizeof(float));
            }
        }
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
        #pragma omp parallel for num_threads(p)
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

        hnswlib::BruteforceSearch<float> *alg_brute = new hnswlib::BruteforceSearch<float>(&space, input_size);
        #pragma omp parallel for num_threads(p)
        for (int i = 0; i < input_size; i++)
        {
            alg_brute->addPoint(data + i * dimension, i);
        }

        double correct = 0;
        #pragma omp parallel for num_threads(p) reduction(+ : correct)
        for (int i = 0; i < query_input_size; i++)
        {
            std::priority_queue<std::pair<float, hnswlib::labeltype>> result = alg_brute->searchKnn(query_data + i * dimension, 1);
            int brute_force_id = result.top().second;
            if (global_results[i].id == brute_force_id)
            {
                correct++;
            }
        }

        float recall = correct / query_input_size;
        std::cout << "Recall: " << recall << std::endl;
    }

    MPI_Finalize();

    return 0;
}
