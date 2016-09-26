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

#include "../include/BitTrickBox.h"
#include "../include/cag_partitioner.h"
#include "../include/DebsTaxiChallenge.h"
#include "../include/TpchExperiment.h"

void card_estimate_example()
{
	std::unordered_set<uint32_t> naive_set;
	CardinalityEstimator::ProbCount prob_count;
	CardinalityEstimator::HyperLoglog hyper_loglog(5);
	for (size_t i = 0; i < 1000000; ++i)
	{
		size_t naive_diff = naive_set.size();
		naive_set.insert(uint32_t(i));
		naive_diff = naive_set.size() - naive_diff;
		size_t pc_prev = prob_count.cardinality_estimation();
		prob_count.update_bitmap(uint32_t(i));
		size_t pc_diff = prob_count.cardinality_estimation() - pc_prev;
		size_t hll_prev = hyper_loglog.cardinality_estimation();
		hyper_loglog.update_bitmap(uint32_t(i));
		size_t hll_diff = hyper_loglog.cardinality_estimation() - hll_prev;
		if (naive_diff < 0)
		{
			std::cout << "non-monotonicity detected in naive set\n";
			exit(1);
		}
		if (pc_diff < 0)
		{
			std::cout << "non-monotonicity detected in pc! value: " << i << ", previous: " << 
				pc_prev << ", new: " << prob_count.cardinality_estimation() << ", diff: " << pc_diff << ".\n";
			exit(1);
		}
		if (hll_diff < 0)
		{
			std::cout << "non-monotonicity detected in HLL! value: " << i << ", previous: " <<
				hll_prev << ", new: " << hyper_loglog.cardinality_estimation() << ", diff: " << hll_diff << ".\n";
			exit(1);
		}
	}
	std::cout << "Actual cardinality: " << naive_set.size() << "\n";
	std::cout << "Probabilistic Count: estimated cardinality: " << prob_count.cardinality_estimation() << "\n";
	std::cout << "Hyper-Loglog: estimated cardinality: " << hyper_loglog.cardinality_estimation() << "\n";
}

void debs_hll_partition_performance_estimate(const std::vector<uint16_t>& tasks, CagPartionLib::CagHllPartitioner& partitioner,
	const std::string partioner_name, std::vector<Experiment::DebsChallenge::Ride>& rides)
{
	std::vector<std::unordered_set<std::string>> key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		short task = partitioner.partition_next_with_estimate(key, key.length());
		key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> partition_time = part_end - part_start;
	size_t min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t max_cardinality = std::numeric_limits<uint64_t>::min();
	double average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (min_cardinality > key_per_task[i].size())
		{
			min_cardinality = key_per_task[i].size();
		}
		if (max_cardinality < key_per_task[i].size())
		{
			max_cardinality = key_per_task[i].size();
		}
		average_cardinality += key_per_task[i].size();
		std::cout << key_per_task[i].size() << " ";
		key_per_task[i].clear();
	}
	std::cout << "\n";
	average_cardinality = average_cardinality / tasks.size();
	key_per_task.clear();
	std::cout << "Time partition using " << partioner_name << ": " << partition_time.count() << " (msec). Min: " << min_cardinality <<
		", Max: " << max_cardinality << ", AVG: " << average_cardinality << "\n";
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

int main(int argc, char** argv)
{
	//char ch;
	if (argc < 4)
	{
		std::cout << "usage: <input-file> <worker-num> <max-queue-size>\n";
		exit(1);
	}
	//std::string lineitem_file_name = "D:\\tpch_2_17_0\\lineitem_sample.tbl";
	std::string input_file_name = argv[1];
	uint16_t task_num = std::stoi(argv[2]);
	size_t max_queue_size = std::stoi(argv[3]);
	std::vector<uint16_t> tasks;
	for (uint16_t i = 0; i < task_num; ++i)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();

	// bit_tricks_scenario();

	// card_estimate_example();

	// bit_tricks_correctness_test();

	// bit_tricks_performance_16();

	// bit_tricks_performance_32();

	// bit_tricks_performance_64();

	// debs_partition_performance("D:\\Downloads\\DEBS2015-Challenge\\test_set_small.csv");

	// tpch_q1_performance(lineitem_file_name);

	//std::vector<Tpch::lineitem> lineitem_table = parse_tpch_lineitem(lineitem_file_name);
	//pkg_concurrent_partition(lineitem_table);
	//cag_naive_concurrent_partition(lineitem_table);
	//lag_naive_concurrent_partition(lineitem_table);
	//tpch_q1_cag_pc_concurrent_partition(lineitem_table);
	//lag_pc_concurrent_partition(lineitem_table);
	//cag_hll_concurrent_partition(lineitem_table);
	//lag_hll_concurrent_partition(lineitem_table);
	Experiment::DebsChallenge::FrequentRoutePartition debs_experiment_frequent_route;

	Experiment::DebsChallenge::ProfitableAreaPartition debs_experiment_profit_area;

	//std::vector<Experiment::DebsChallenge::Ride> ride_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 500, 300);

	std::vector<Experiment::DebsChallenge::Ride> profit_area_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 250, 600);

	// debs_experiment.debs_compare_cag_correctness(tasks, ride_table);

	// debs_check_hash_result_values("hash_result.csv", ride_table);

	// debs_cardinality_estimation("cardinality_est_perf.csv", ride_table);
	
	HashFieldPartitioner fld_partitioner(tasks);
	PkgPartitioner pkg_partitioner(tasks);
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagNaivePartitioner cag_naive_partitioner(tasks, cag_policy);
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner lag_naive_partitioner(tasks, lag_policy);
	CagPartionLib::CagPcPartitioner cag_pc_partitioner(tasks, cag_policy);
	CagPartionLib::CagPcPartitioner lag_pc_partitioner(tasks, lag_policy);
	CagPartionLib::CagHllPartitioner cag_hll_partitioner(tasks, cag_policy, 10);
	CagPartionLib::CagHllPartitioner lag_hll_partitioner(tasks, lag_policy, 10);
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
	debs_experiment_frequent_route.debs_partition_performance(tasks, lag_hll_partitioner, "LAG-hll", ride_table);*/
	//debs_hll_partition_performance_estimate(tasks, cag_hll_partitioner, "CAG-hll", ride_table);
	/*
	 * End-to-end Performance
	 */
	/*debs_experiment.debs_concurrent_partition(tasks, ride_table, fld_partitioner, "FLD", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, pkg_partitioner, "PKG", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, cag_naive_partitioner, "CAG-naive", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, lag_naive_partitioner, "LAG-naive", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, cag_pc_partitioner, "CAG-pc", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, lag_pc_partitioner, "LAG-pc", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, cag_hll_partitioner, "CAG-hll", max_queue_size);
	debs_experiment.debs_concurrent_partition(tasks, ride_table, lag_hll_partitioner, "LAG-hll", max_queue_size);*/

	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, fld_partitioner, "FLD", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, pkg_partitioner, "PKG", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, cag_naive_partitioner, "CAG-naive", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, lag_naive_partitioner, "LAG-naive", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, cag_pc_partitioner, "CAG-pc", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, lag_pc_partitioner, "LAG-pc", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, cag_hll_partitioner, "CAG-hll", max_queue_size);
	debs_experiment_profit_area.debs_concurrent_partition(tasks, profit_area_table, lag_hll_partitioner, "LAG-hll", max_queue_size);

	/*std::cout << "Press any key to continue...\n";
	std::cin >> ch;*/
	return 0;
}
