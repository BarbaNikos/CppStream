#include <iostream>
#include <ctime>
#include <fstream>
#include <string>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <thread>
#include <future>
#include <fstream>
#include <functional>
#include <numeric>
#include <algorithm>

#ifndef C_HLL_H_
#include "../include/c_hll.h"
#endif // !C_HLL_H_

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // !DEBS_QUERY_LIB_H_

#ifndef EXPERIMENT_LOGNORMAL_SIMULATION_H_
#include "../include/lognormal_experiment.h"
#endif // !EXPERIMENT_LOGNORMAL_SIMULATION_H_

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

#ifndef TPCH_QUERY_LIB_H_
#include "../include/tpch_query_lib.h"
#endif // !TPCH_QUERY_LIB_H_


void log_normal_simulation(std::string input_file);

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		std::cout << "usage: <input-file-1> optional: <input-file-2>\n";
		exit(1);
	}
	std::string input_file_name = argv[1];
	std::string input_file_name_2;
	if (argc > 2)
	{
		input_file_name_2 = argv[2];
	}
	/*
	 * TPC-H query
	 */
	std::vector<Experiment::Tpch::lineitem> lineitem_table;
	std::vector<Experiment::Tpch::order> order_table;
	Experiment::Tpch::DataParser::parse_tpch_lineitem(input_file_name, lineitem_table);
	Experiment::Tpch::DataParser::parse_tpch_order(input_file_name_2, order_table);
	Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 10);
	Experiment::Tpch::LineitemOrderPartition::lineitem_order_join_simulation(lineitem_table, order_table, 10);

	/*
	 * DEBS queries
	 */
	//Experiment::DebsChallenge::FrequentRoutePartition debs_experiment_frequent_route;
	//std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 500, 300);
	//debs_experiment_frequent_route.produce_compact_ride_file(input_file_name, "debs_frequent_route_compact_ride.csv", 500, 300);
	//debs_experiment_frequent_route.produce_compact_ride_file(input_file_name, "debs_profitable_cell_compact_ride.csv", 250, 600);
	// std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table;
	//debs_experiment_frequent_route.parse_debs_rides_with_to_string(input_file_name, &frequent_ride_table);
	//debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table);
	//frequent_ride_table.clear();
	//
	//Experiment::DebsChallenge::ProfitableAreaPartition profitable_area;
	//std::vector<Experiment::DebsChallenge::CompactRide> most_profitable_cell_table;
	//std::vector<Experiment::DebsChallenge::CompactRide> most_profitable_cell_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 250, 600);
	//debs_experiment_frequent_route.parse_debs_rides_with_to_string(input_file_name, &most_profitable_cell_table);
	//profitable_area.most_profitable_cell_simulation(most_profitable_cell_table);
	//most_profitable_cell_table.clear();

	//log_normal_simulation("Z:\\Documents\\ln1_stream.tbl");
	/*
	 * Upper bound benefit experiment
	 */
	//upper_bound_experiment(input_file_name);
	//const unsigned int p = 16;
	//plot_cardinality_estimation_correctness(p, 10, (uint64_t)1e+5, (size_t)1e+6);

	/*
	 * GOOGLE-MONITOR-CLUSTER queries
	 */
	//std::vector<Experiment::GoogleClusterMonitor::task_event> task_event_table;
	////Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events(input_file_name, task_event_table);
	//Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events_from_directory(input_file_name, task_event_table);
	//Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_simulation(task_event_table, 10);
	//Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_simulation(task_event_table, 10);
	//task_event_table.clear();

	return 0;
}

void log_normal_simulation(std::string input_file)
{
	Experiment::LogNormalSimulation simulation;
	//simulation.sort_to_plot(input_file);
	uint16_t task_num[] = { 10, 50, 100 };
	for (size_t i = 0; i < 3; i++)
	{
	std::vector<uint16_t> tasks;
	for (size_t j = 0; j < task_num[i]; j++)
	{
	tasks.push_back(uint16_t(j));
	}
	tasks.shrink_to_fit();
	std::cout << "## Tasks: " << task_num[i] << ".\n";
	simulation.simulate(tasks, input_file);
	}
	std::cout << "********* END ***********\n";
}

double get_cardinality_average(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	double sum = 0.0;
	for (std::vector<std::unordered_set<uint32_t>>::const_iterator it = task_cardinality.cbegin(); it != task_cardinality.cend(); ++it)
	{
		sum += it->size();
	}
	return sum / task_cardinality.size();
}

double get_cardinality_max(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	double max = 0.0;
	for (std::vector<std::unordered_set<uint32_t>>::const_iterator it = task_cardinality.cbegin(); it != task_cardinality.cend(); ++it)
	{
		max = max < it->size() ? it->size() : max;
	}
	return max;
}

double get_imbalance(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	return get_cardinality_max(task_cardinality) - get_cardinality_average(task_cardinality);
}

double get_imbalance(const std::vector<uint32_t>& task_cardinality)
{
	uint32_t max_card = *std::max_element(task_cardinality.begin(), task_cardinality.end());
	double sum_card = std::accumulate(task_cardinality.begin(), task_cardinality.end(), 0);
	return max_card - (sum_card / task_cardinality.size());
}

void plot_cardinality_estimation_correctness(const unsigned int p, const size_t task_number, const uint64_t cardinality, const size_t stream_length)
{
	std::ofstream imbalance_plot("imbalance_plot.csv");
	//imbalance_plot << "tuple-id";
	std::vector<uint16_t> tasks;
	uint64_t* stream;
	hll_8** opt_cardinality_estimator = (hll_8**)malloc(sizeof(hll_8*) * task_number);
	hll_32** opt_32_cardinality_estimator = (hll_32**)malloc(sizeof(hll_32*) * task_number);
	Cardinality_Estimation_Utils::ProbCount** pc = new Cardinality_Estimation_Utils::ProbCount*[task_number];

	srand(time(NULL));

	//for (size_t i = 0; i < task_number; i++)
	//{
	//	if (i < task_number - 1)
	//	{
	//		//imbalance_plot << std::to_string(i) << "-err,";
	//	}
	//	else
	//	{
	//		//imbalance_plot << std::to_string(i) << "-err";
	//	}
	//}
	//imbalance_plot << "\n";

	for (size_t i = 0; i < task_number; i++)
	{
		tasks.push_back(uint16_t(i));
		opt_cardinality_estimator[i] = (hll_8*)malloc(sizeof(hll_8));
		init_8(opt_cardinality_estimator[i], p, sizeof(uint64_t));
		opt_32_cardinality_estimator[i] = (hll_32*)malloc(sizeof(hll_32));
		init_32(opt_32_cardinality_estimator[i], p, sizeof(uint64_t));
		pc[i] = new Cardinality_Estimation_Utils::ProbCount(64);
	}
	CardinalityAwarePolicy ca;
	CaPartitionLib::CA_Exact_Partitioner cag_naive(tasks, ca);
	stream = new uint64_t[stream_length];
	for (size_t i = 0; i < stream_length; ++i)
	{
		stream[i] = rand() % cardinality;
	}
	std::cout << "Generated stream.\n";

	for (size_t i = 0; i < stream_length; ++i)
	{
		uint64_t element = stream[i];
		uint64_t long_code[2];
		MurmurHash3_x64_128(&element, sizeof(uint64_t), 13, &long_code);
		uint64_t xor_long_code = long_code[0] ^ long_code[1];
		uint16_t naive_choice = cag_naive.partition_next(&element, sizeof(uint64_t));
		opt_update_8(opt_cardinality_estimator[naive_choice], xor_long_code);
		opt_update_32(opt_32_cardinality_estimator[naive_choice], xor_long_code);
		pc[naive_choice]->update_bitmap_with_hashed_value_64(xor_long_code);
		std::vector<unsigned long long> cag_naive_cardinality_vector;
		cag_naive.get_cardinality_vector(cag_naive_cardinality_vector);
		imbalance_plot << std::to_string(i) << ",";
		for (size_t i = 0; i < task_number; i++)
		{
			//uint64_t opt_estimate = opt_cardinality_estimation_8(opt_cardinality_estimator[i]);
			uint64_t opt_32_estimate = opt_cardinality_estimation_32(opt_32_cardinality_estimator[i]);
			//uint16_t pc_estimate = pc[i]->cardinality_estimation_64();
			//int64_t opt_diff = cag_naive_cardinality_vector[i] - opt_estimate;
			int64_t opt_32_diff = cag_naive_cardinality_vector[i] - opt_32_estimate;
			/*if (cag_naive_cardinality_vector[i])
			{
			double relative_diff = double(opt_diff) / cag_naive_cardinality_vector[i];
			if (i < task_number - 1)
			{
			imbalance_plot << std::to_string(relative_diff) << ",";
			}
			else
			{
			imbalance_plot << std::to_string(relative_diff);
			}
			}
			else
			{
			double relative_diff = 0;
			if (i < task_number - 1)
			{
			imbalance_plot << std::to_string(relative_diff) << ",";
			}
			else
			{
			imbalance_plot << std::to_string(relative_diff);
			}
			}*/
			if (i < task_number - 1)
			{
				imbalance_plot << std::to_string(abs(opt_32_diff)) << ",";
			}
			else
			{
				imbalance_plot << std::to_string(abs(opt_32_diff));
			}
		}
		imbalance_plot << "\n";
	}
	std::cout << "Finished the partitioning and the output of the plot.\n";
	tasks.clear();
	delete[] stream;
	imbalance_plot.flush();
	imbalance_plot.close();
	for (size_t i = 0; i < task_number; i++)
	{
		destroy_8(opt_cardinality_estimator[i]);
		free(opt_cardinality_estimator[i]);
		delete pc[i];
	}
	delete[] pc;
	free(opt_cardinality_estimator);
}