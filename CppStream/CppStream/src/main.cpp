#pragma once
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

#include "../include/c_hll.h"

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // !DEBS_QUERY_LIB_H_

#ifndef HASH_FLD_PARTITIONER_H_
#include "../include/hash_fld_partitioner.h"
#endif // !HASH_FLD_PARTITIONER_H_

#ifndef PKG_PARTITIONER_H_
#include "../include/pkg_partitioner.h"
#endif // !PKG_PARTITIONER_H_

#ifndef CAG_PARTITIONER_H_
#include "../include/cag_partitioner.h"
#endif // !CAG_PARTITIONER_H_

#ifndef EXPERIMENT_LOGNORMAL_SIMULATION_H_
#include "../include/lognormal_experiment.h"
#endif // !EXPERIMENT_LOGNORMAL_SIMULATION_H_

void debs_check_hash_result_values(const std::string& out_file_name, const std::vector<Experiment::DebsChallenge::Ride>& ride_table);
void debs_cardinality_estimation(const std::string& out_file_name, const std::vector<Experiment::DebsChallenge::Ride>& ride_table);
void debs_all_test(const std::string input_file_name, size_t max_queue_size);
void log_normal_simulation(std::string input_file);
void upper_bound_experiment(const std::string input_file_name);
void upper_bound_performance_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks,
	Partitioner& partitioner, const std::string partitioner_name);

double get_cardinality_average(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	double sum = 0.0;
	for each (std::unordered_set<uint32_t> cardinality in task_cardinality)
	{
		sum += cardinality.size();
	}
	return sum / task_cardinality.size();
}

double get_cardinality_max(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	double max = 0.0;
	for each (std::unordered_set<uint32_t> cardinality in task_cardinality)
	{
		max = max < cardinality.size() ? cardinality.size() : max;
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
	const double error = 1.06 / sqrt(std::pow(2, 10));
	std::ofstream imbalance_plot("imbalance_plot.csv");
	//imbalance_plot << "tuple-id";
	std::vector<uint16_t> tasks;
	uint64_t* stream;
	hll_8** opt_cardinality_estimator = (hll_8**)malloc(sizeof(hll_8*) * task_number);
	srand(time(NULL));

	for (size_t i = 0; i < task_number; i++)
	{
		if (i < task_number - 1)
		{
			//imbalance_plot << std::to_string(i) << "-err,";
		}
		else
		{
			//imbalance_plot << std::to_string(i) << "-err";
		}
	}
	//imbalance_plot << "\n";
	
	for (size_t i = 0; i < task_number; i++)
	{
		tasks.push_back(uint16_t(i));
		opt_cardinality_estimator[i] = (hll_8*)malloc(sizeof(hll_8));
		init_8(opt_cardinality_estimator[i], p);
	}
	CardinalityAwarePolicy cag;
	CagPartitionLib::CagNaivePartitioner cag_naive(tasks, cag);
	stream = new uint64_t[stream_length];
	for (size_t i = 0; i < stream_length; ++i)
	{
		stream[i] = rand() % cardinality;
	}
	std::cout << "Generated stream.\n";

	for (size_t i = 0; i < stream_length; ++i)
	{
		uint64_t element = stream[i];
		uint32_t code;
		MurmurHash3_x86_32(&element, sizeof(uint64_t), 13, &code);
		uint16_t naive_choice = cag_naive.partition_next(&element, sizeof(uint64_t));
		opt_update_8(opt_cardinality_estimator[naive_choice], code);
		std::vector<uint32_t> cag_naive_cardinality_vector;
		cag_naive.get_cardinality_vector(cag_naive_cardinality_vector);
		imbalance_plot << std::to_string(i) << ",";
		for (size_t i = 0; i < task_number; i++)
		{
			uint32_t opt_estimate = opt_cardinality_estimation_8(opt_cardinality_estimator[i]);
			int opt_diff = cag_naive_cardinality_vector[i] - opt_estimate;
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
				imbalance_plot << std::to_string(abs(opt_diff)) << ",";
			}
			else
			{
				imbalance_plot << std::to_string(abs(opt_diff));
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
	}
	free(opt_cardinality_estimator);
}

int main(int argc, char** argv)
{
	char ch;

	if (argc < 2)
	{
		std::cout << "usage: <input-file>\n";
		exit(1);
	}
	std::string input_file_name = argv[1];
	/*
	 * TPC-H query
	 */
	// Experiment::Tpch::DataParser::parse_all_files_test("C:\\Users\\nickk\\Documents\\tpc_h_2_17\\dbgen");
	/*
	 * DEBS queries
	 */
	//debs_all_test(input_file_name, max_queue_size);
	//log_normal_simulation("Z:\\Documents\\ln1_stream.tbl");
	/*
	 * Upper bound benefit experiment
	 */
	//upper_bound_experiment(input_file_name);
	const unsigned int p = 12;
	plot_cardinality_estimation_correctness(p, 10, 1e+6, 1e+7);
	std::cout << "Press any key to continue...\n";
	std::cin >> ch;
	return 0;
}

void debs_check_hash_result_values(const std::string& out_file_name, const std::vector<Experiment::DebsChallenge::Ride>& ride_table)
{
	std::string out_hash_one = out_file_name + "_" + std::to_string(13);
	std::string out_hash_two = out_file_name + "_" + std::to_string(17);
	std::ofstream output_file_1(out_hash_one);
	std::ofstream output_file_2(out_hash_two);
	if (output_file_1.is_open() == false || output_file_2.is_open() == false)
	{
		std::cout << "failed to create " << out_file_name << " file.\n";
		exit(1);
	}
	std::map<uint32_t, uint32_t> hash_frequency_1;
	std::map<uint32_t, uint32_t> hash_frequency_2;
	for (auto it = ride_table.cbegin(); it != ride_table.cend(); ++it)
	{
		uint32_t value_1, value_2;
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		MurmurHash3_x86_32(key.c_str(), key.length(), 13, &value_1);
		MurmurHash3_x86_32(key.c_str(), key.length(), 17, &value_2);
		auto it_1 = hash_frequency_1.find(value_1);
		auto it_2 = hash_frequency_2.find(value_2);
		if (it_1 != hash_frequency_1.end())
		{
			it_1->second++;
		}
		else
		{
			hash_frequency_1[value_1] = uint32_t(1);
		}
		if (it_2 != hash_frequency_2.end())
		{
			it_2->second++;
		}
		else
		{
			hash_frequency_2[value_2] = uint32_t(1);
		}
	}
	for (auto it = hash_frequency_1.cbegin(); it != hash_frequency_1.cend(); ++it)
	{
		output_file_1 << it->first << "," << it->second << "\n";
	}
	output_file_1.flush();
	output_file_1.close();
	for (auto it = hash_frequency_2.cbegin(); it != hash_frequency_2.cend(); ++it)
	{
		output_file_2 << it->first << "," << it->second << "\n";
	}
	output_file_2.flush();
	output_file_2.close();
	std::cout << "wrote hash frequencies on both files.\n";
}

void debs_cardinality_estimation(const std::string& out_file_name, const std::vector<Experiment::DebsChallenge::Ride>& ride_table)
{
	std::unordered_set<uint32_t> actual_cardinality;
	CardinalityEstimator::ProbCount prob_count;
	CardinalityEstimator::HyperLoglog hyper_loglog_s(5);
	CardinalityEstimator::HyperLoglog hyper_loglog_l(10);
	uint64_t counter = 0;
	std::ofstream output_file_2(out_file_name);
	for (auto it = ride_table.cbegin(); it != ride_table.cend(); ++it)
	{
		uint32_t value_1;
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		MurmurHash3_x86_32(key.c_str(), key.length(), 13, &value_1);
		actual_cardinality.insert(value_1);
		prob_count.update_bitmap(value_1);
		hyper_loglog_s.update_bitmap(value_1);
		hyper_loglog_l.update_bitmap(value_1);
		output_file_2 << counter << "," << actual_cardinality.size() << "," << prob_count.cardinality_estimation() << "," <<
			hyper_loglog_s.cardinality_estimation() << "," << hyper_loglog_l.cardinality_estimation() << "\n";
		counter++;
	}
	output_file_2.flush();
	output_file_2.close();
}

void debs_all_test(const std::string input_file_name, size_t max_queue_size)
{
	std::vector<uint16_t> tasks;
	/*
	* DEBS query
	*/
	Experiment::DebsChallenge::FrequentRoutePartition debs_experiment_frequent_route;
	std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_ride_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 500, 300);
	std::cout << "Parsed frequent-ride table.\n";
	// debs_experiment.debs_compare_cag_correctness(tasks, ride_table);
	// debs_check_hash_result_values("hash_result.csv", ride_table);
	// debs_cardinality_estimation("cardinality_est_perf.csv", ride_table);
	/*
	* Partition latency
	*/
	/*debs_experiment_frequent_route.debs_partition_performance(tasks, fld_partitioner, "FLD", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, pkg_partitioner, "PKG", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, cag_naive_partitioner, "CAG-naive", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, lag_naive_partitioner, "LAG-naive", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, cag_pc_partitioner, "CAG-pc", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, lag_pc_partitioner, "LAG-pc", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, cag_hll_partitioner, "CAG-hll", ride_table);
	debs_hll_partition_performance_estimate(tasks, cag_hll_est_partitioner, "CAG-hll-est", ride_table);
	debs_experiment_frequent_route.debs_partition_performance(tasks, lag_hll_partitioner, "LAG-hll", ride_table);*/
	/*
	* End-to-end Performance
	*/
	std::cout << "***** FREQUENT ROUTE ****\n";
	for (size_t i = 5; i <= 20; i = i + 5)
	{
		tasks.clear();
		for (uint16_t j = 0; j < i; ++j)
		{
			tasks.push_back(j);
		}
		tasks.shrink_to_fit();
		std::cout << "# of tasks: " << tasks.size() << ".\n";
		CardinalityAwarePolicy cag_policy;
		LoadAwarePolicy lag_policy;
		double avg_runtime[9];
		for (size_t i = 0; i < 9; ++i)
		{
			avg_runtime[i] = 0;
		}
		for (size_t iter = 0; iter < 4; ++iter)
		{
			HashFieldPartitioner* fld_partitioner = new HashFieldPartitioner(tasks);
			PkgPartitioner* pkg_partitioner = new PkgPartitioner(tasks);
			CagPartitionLib::CagNaivePartitioner* cag_naive_partitioner = new CagPartitionLib::CagNaivePartitioner(tasks, cag_policy);
			CagPartitionLib::CagNaivePartitioner* lag_naive_partitioner = new CagPartitionLib::CagNaivePartitioner(tasks, lag_policy);
			CagPartitionLib::CagPcPartitioner* cag_pc_partitioner = new CagPartitionLib::CagPcPartitioner(tasks, cag_policy);
			CagPartitionLib::CagPcPartitioner* lag_pc_partitioner = new CagPartitionLib::CagPcPartitioner(tasks, lag_policy);
			CagPartitionLib::CagHllPartitioner* cag_hll_partitioner = new CagPartitionLib::CagHllPartitioner(tasks, cag_policy, 10);
			CagPartitionLib::CagHllEstPartitioner* cag_hll_est_partitioner = new CagPartitionLib::CagHllEstPartitioner(tasks, 10);
			CagPartitionLib::CagHllPartitioner* lag_hll_partitioner = new CagPartitionLib::CagHllPartitioner(tasks, lag_policy, 10);
			avg_runtime[0] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *fld_partitioner, "FLD", max_queue_size);
			avg_runtime[1] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *pkg_partitioner, "PKG", max_queue_size);
			avg_runtime[2] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *cag_naive_partitioner, "CAG-naive", max_queue_size);
			avg_runtime[3] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *lag_naive_partitioner, "LAG-naive", max_queue_size);
			avg_runtime[4] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *cag_pc_partitioner, "CAG-pc", max_queue_size);
			avg_runtime[5] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *lag_pc_partitioner, "LAG-pc", max_queue_size);
			avg_runtime[6] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *cag_hll_partitioner, "CAG-hll", max_queue_size);
			avg_runtime[7] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *cag_hll_est_partitioner, "CAG-hll-est", max_queue_size);
			avg_runtime[8] += debs_experiment_frequent_route.debs_concurrent_partition(tasks, frequent_ride_ride_table, *lag_hll_partitioner, "LAG-hll", max_queue_size);
			delete fld_partitioner;
			delete pkg_partitioner;
			delete cag_naive_partitioner;
			delete lag_naive_partitioner;
			delete cag_pc_partitioner;
			delete lag_pc_partitioner;
			delete cag_hll_partitioner;
			delete cag_hll_est_partitioner;
			delete lag_hll_partitioner;
		}
		for (size_t i = 0; i < 9; ++i)
		{
			avg_runtime[i] = avg_runtime[i] / 4;
		}
		std::cout << "FLD total partition time: " << avg_runtime[0] << " (msec) (frequent-routes).\n";
		std::cout << "PKG total partition time: " << avg_runtime[1] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-naive total partition time: " << avg_runtime[2] << " (msec) (frequent-routes).\n";
		std::cout << "LAG-naive total partition time: " << avg_runtime[3] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-pc* total partition time: " << avg_runtime[4] << " (msec) (frequent-routes).\n";
		std::cout << "LAG-pc* total partition time: " << avg_runtime[5] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-hll total partition time: " << avg_runtime[6] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-hll-est total partition time: " << avg_runtime[7] << " (msec) (frequent-routes).\n";
		std::cout << "LAG-hll total partition time: " << avg_runtime[8] << " (msec) (frequent-routes).\n";
	}
	frequent_ride_ride_table.clear();

	Experiment::DebsChallenge::ProfitableAreaPartition debs_experiment_profit_area;
	std::vector < Experiment::DebsChallenge::CompactRide> profitable_area_ride_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 250, 600);
	std::cout << "***** PROFITABLE AREA ****\n";
	for (size_t i = 5; i <= 20; i = i + 5)
	{
		tasks.clear();
		for (uint16_t j = 0; j < i; ++j)
		{
			tasks.push_back(j);
		}
		tasks.shrink_to_fit();
		std::cout << "# of tasks: " << tasks.size() << ".\n";
		CardinalityAwarePolicy cag_policy;
		LoadAwarePolicy lag_policy;
		double avg_runtime[9];
		for (size_t i = 0; i < 9; ++i)
		{
			avg_runtime[i] = 0;
		}
		for (size_t i = 0; i < 4; ++i)
		{
			HashFieldPartitioner* fld_partitioner = new HashFieldPartitioner(tasks);
			PkgPartitioner* pkg_partitioner = new PkgPartitioner(tasks);
			CagPartitionLib::CagNaivePartitioner* cag_naive_partitioner = new CagPartitionLib::CagNaivePartitioner(tasks, cag_policy);
			CagPartitionLib::CagNaivePartitioner* lag_naive_partitioner = new CagPartitionLib::CagNaivePartitioner(tasks, lag_policy);
			CagPartitionLib::CagPcPartitioner* cag_pc_partitioner = new CagPartitionLib::CagPcPartitioner(tasks, cag_policy);
			CagPartitionLib::CagPcPartitioner* lag_pc_partitioner = new CagPartitionLib::CagPcPartitioner(tasks, lag_policy);
			CagPartitionLib::CagHllPartitioner* cag_hll_partitioner = new CagPartitionLib::CagHllPartitioner(tasks, cag_policy, 10);
			CagPartitionLib::CagHllEstPartitioner* cag_hll_est_partitioner = new CagPartitionLib::CagHllEstPartitioner(tasks, 10);
			CagPartitionLib::CagHllPartitioner* lag_hll_partitioner = new CagPartitionLib::CagHllPartitioner(tasks, lag_policy, 10);
			avg_runtime[0] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *fld_partitioner, "FLD", max_queue_size);
			avg_runtime[1] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *pkg_partitioner, "PKG", max_queue_size);
			avg_runtime[2] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *cag_naive_partitioner, "CAG-naive", max_queue_size);
			avg_runtime[3] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *lag_naive_partitioner, "LAG-naive", max_queue_size);
			avg_runtime[4] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *cag_pc_partitioner, "CAG-pc", max_queue_size);
			avg_runtime[5] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *lag_pc_partitioner, "LAG-pc", max_queue_size);
			avg_runtime[6] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *cag_hll_partitioner, "CAG-hll", max_queue_size);
			avg_runtime[7] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *cag_hll_est_partitioner, "CAG-hll-est", max_queue_size);
			avg_runtime[8] += debs_experiment_profit_area.debs_concurrent_partition(tasks, profitable_area_ride_table, *lag_hll_partitioner, "LAG-hll", max_queue_size);
			delete fld_partitioner;
			delete pkg_partitioner;
			delete cag_naive_partitioner;
			delete lag_naive_partitioner;
			delete cag_pc_partitioner;
			delete lag_pc_partitioner;
			delete cag_hll_partitioner;
			delete cag_hll_est_partitioner;
			delete lag_hll_partitioner;
		}
		for (size_t i = 0; i < 9; ++i)
		{
			avg_runtime[i] = avg_runtime[i] / 4;
		}
		std::cout << "FLD total partition time: " << avg_runtime[0] << " (msec) (frequent-routes).\n";
		std::cout << "PKG total partition time: " << avg_runtime[1] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-naive total partition time: " << avg_runtime[2] << " (msec) (frequent-routes).\n";
		std::cout << "LAG-naive total partition time: " << avg_runtime[3] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-pc* total partition time: " << avg_runtime[4] << " (msec) (frequent-routes).\n";
		std::cout << "LAG-pc* total partition time: " << avg_runtime[5] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-hll total partition time: " << avg_runtime[6] << " (msec) (frequent-routes).\n";
		std::cout << "CAG-hll-est total partition time: " << avg_runtime[7] << " (msec) (frequent-routes).\n";
		std::cout << "LAG-hll total partition time: " << avg_runtime[8] << " (msec) (frequent-routes).\n";
	}
	std::cout << "**** DONE ****\n";
}

void log_normal_simulation(std::string input_file)
{
	Experiment::LogNormalSimulation simulation;
	//simulation.sort_to_plot(input_file);
	uint16_t task_num[] = { 5, 10, 50, 100 };
	for (size_t i = 0; i < 4; i++)
	{
	std::vector<uint16_t> tasks;
	for (size_t j = 0; j < task_num[i]; j++)
	{
	tasks.push_back(j);
	}
	tasks.shrink_to_fit();
	std::cout << "## Tasks: " << task_num[i] << ".\n";
	simulation.simulate(tasks, input_file);
	}
	std::cout << "********* END ***********\n";
}

void upper_bound_experiment(const std::string input_file_name)
{
	Experiment::DebsChallenge::FrequentRoutePartition experiment;
	std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
	std::vector<Experiment::DebsChallenge::CompactRide>* lines = new std::vector<Experiment::DebsChallenge::CompactRide>();
	experiment.parse_debs_rides_with_to_string(input_file_name, lines);
	std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> scan_duration = end - start;
	std::cout << "scanned and parsed the whole life.. (time: " << scan_duration.count() << " msec).\n";
	// tasks: 5
	std::vector<uint16_t> tasks;
	for (uint16_t i = 0; i < 5; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "Tasks: " << tasks.size() << ".\n";
	PkgPartitioner* pkg = new PkgPartitioner(tasks);
	HashFieldPartitioner* fld = new HashFieldPartitioner(tasks);
	CardinalityAwarePolicy cag_policy;
	CagPartitionLib::CagNaivePartitioner* cag_naive = new CagPartitionLib::CagNaivePartitioner(tasks, cag_policy);
	upper_bound_performance_simulation(*lines, tasks, *pkg, "pkg");
	upper_bound_performance_simulation(*lines, tasks, *fld, "fld");
	upper_bound_performance_simulation(*lines, tasks, *cag_naive, "cag-naive");

	delete pkg;
	delete fld;
	delete cag_naive;
	tasks.clear();
	// tasks: 10
	for (uint16_t i = 0; i < 10; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "Tasks: " << tasks.size() << ".\n";
	pkg = new PkgPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	cag_naive = new CagPartitionLib::CagNaivePartitioner(tasks, cag_policy);
	upper_bound_performance_simulation(*lines, tasks, *pkg, "pkg");
	upper_bound_performance_simulation(*lines, tasks, *fld, "fld");
	upper_bound_performance_simulation(*lines, tasks, *cag_naive, "cag-naive");

	delete pkg;
	delete fld;
	delete cag_naive;
	tasks.clear();
	// tasks: 100
	for (uint16_t i = 0; i < 100; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "Tasks: " << tasks.size() << ".\n";
	pkg = new PkgPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	cag_naive = new CagPartitionLib::CagNaivePartitioner(tasks, cag_policy);
	upper_bound_performance_simulation(*lines, tasks, *pkg, "pkg");
	upper_bound_performance_simulation(*lines, tasks, *fld, "fld");
	upper_bound_performance_simulation(*lines, tasks, *cag_naive, "cag-naive");

	delete pkg;
	delete fld;
	delete cag_naive;
	tasks.clear();
	// cleaning up buffer
	lines->clear();
	delete lines;
}

void upper_bound_performance_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks,
	Partitioner& partitioner, const std::string partitioner_name)
{
	// first read the input file and generate sub-files with 
	// the tuples that will be handled by each worker
	Experiment::DebsChallenge::FrequentRoutePartition experiment;
	std::ofstream** out_file;
	out_file = new std::ofstream*[tasks.size()];
	// create files
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i] = new std::ofstream(partitioner_name + "_" + std::to_string(i) + ".csv");
	}
	// distribute tuples
	for (auto it = rides.cbegin(); it != rides.cend(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		*out_file[task] << it->to_string() << "\n";
	}
	// get maximum and minimum running times
	double min_duration = 0, max_duration = 0;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		out_file[i]->flush();
		out_file[i]->close();
		std::vector<Experiment::DebsChallenge::CompactRide>* task_lines = new std::vector<Experiment::DebsChallenge::CompactRide>();
		experiment.parse_debs_rides_with_to_string(partitioner_name + "_" + std::to_string(i) + ".csv", task_lines);
		// feed the worker
		double average_execution_time = 0.0;
		for (short r = 0; r < 3; ++r)
		{
			std::queue<Experiment::DebsChallenge::CompactRide> queue;
			std::mutex mu;
			std::condition_variable cond;
			Experiment::DebsChallenge::FrequentRouteWorkerThread worker(nullptr, nullptr, nullptr, &queue, &mu, &cond);
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			for (auto it = task_lines->begin(); it != task_lines->end(); ++it)
			{
				worker.update(*it);
			}
			worker.finalize();
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			std::chrono::duration<double, std::milli> execution_time = end - start;
			average_execution_time += execution_time.count();
		}
		// clean up the memory after the work is done
		task_lines->clear();
		delete task_lines;
		average_execution_time = average_execution_time / 3;
		if (i == 0)
		{
			min_duration = average_execution_time;
			max_duration = average_execution_time;
		}
		else
		{
			if (min_duration > average_execution_time)
			{
				min_duration = average_execution_time;
			}
			if (max_duration < average_execution_time)
			{
				max_duration = average_execution_time;
			}
		}
		delete out_file[i];
		std::string f_name = partitioner_name + "_" + std::to_string(i) + ".csv";
		std::remove(f_name.c_str());
	}
	delete[] out_file;
	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << "\n";
}

