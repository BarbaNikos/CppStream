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

#ifndef CA_PARTITION_LIB_H_
#include "../include/ca_partition_lib.h"
#endif // !CA_PARTITION_LIB_H_

#ifndef EXPERIMENT_LOGNORMAL_SIMULATION_H_
#include "../include/lognormal_experiment.h"
#endif // !EXPERIMENT_LOGNORMAL_SIMULATION_H_

#ifndef GOOGLE_CLUSTER_MONITOR_UTIL_H_
#include "../include/google_cluster_monitor_util.h"
#endif // !GOOGLE_CLUSTER_MONITOR_UTIL_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#include "../include/round_robin_partitioner.h"
#endif // !ROUND_ROBIN_PARTITIONER_H_


void debs_check_hash_result_values(const std::string& out_file_name, const std::vector<Experiment::DebsChallenge::Ride>& ride_table);
void debs_cardinality_estimation(const std::string& out_file_name, const std::vector<Experiment::DebsChallenge::Ride>& ride_table);
void debs_all_test(const std::string input_file_name, size_t max_queue_size);
void log_normal_simulation(std::string input_file);
void frequent_route_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines);
void most_profitable_cell_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines);
void debs_frequent_route_performance_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks,
	Partitioner& partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix);
void debs_most_profitable_cell_performance_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks,
	Partitioner& partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix);

int main(int argc, char** argv)
{
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
	Experiment::DebsChallenge::FrequentRoutePartition debs_experiment_frequent_route;
	//std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 500, 300);
	
	/*std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table;
	debs_experiment_frequent_route.parse_debs_rides_with_to_string(input_file_name, &frequent_ride_table);
	frequent_route_simulation(frequent_ride_table);
	frequent_ride_table.clear();*/

	//std::vector<Experiment::DebsChallenge::CompactRide> most_profitable_cell_table;
	std::vector<Experiment::DebsChallenge::CompactRide> most_profitable_cell_table = debs_experiment_frequent_route.parse_debs_rides(input_file_name, 250, 600);
	//debs_experiment_frequent_route.parse_debs_rides_with_to_string(input_file_name, &most_profitable_cell_table);
	most_profitable_cell_simulation(most_profitable_cell_table);
	most_profitable_cell_table.clear();

	//debs_all_test(input_file_name, max_queue_size);
	//log_normal_simulation("Z:\\Documents\\ln1_stream.tbl");
	/*
	 * Upper bound benefit experiment
	 */
	//upper_bound_experiment(input_file_name);
	//const unsigned int p = 16;
	//plot_cardinality_estimation_correctness(p, 10, (uint64_t)1e+5, (size_t)1e+6);
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
	Cardinality_Estimation_Utils::ProbCount prob_count(64);
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
		prob_count.update_bitmap_64(value_1);
		output_file_2 << counter << "," << actual_cardinality.size() << "," << prob_count.cardinality_estimation_64() << "\n";
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
			CaPartitionLib::CA_Exact_Partitioner* cag_naive_partitioner = new CaPartitionLib::CA_Exact_Partitioner(tasks, cag_policy);
			CaPartitionLib::CA_Exact_Partitioner* lag_naive_partitioner = new CaPartitionLib::CA_Exact_Partitioner(tasks, lag_policy);
			CaPartitionLib::CA_PC_Partitioner* cag_pc_partitioner = new CaPartitionLib::CA_PC_Partitioner(tasks, cag_policy);
			CaPartitionLib::CA_PC_Partitioner* lag_pc_partitioner = new CaPartitionLib::CA_PC_Partitioner(tasks, lag_policy);
			CaPartitionLib::CA_HLL_Partitioner* cag_hll_partitioner = new CaPartitionLib::CA_HLL_Partitioner(tasks, cag_policy, 10);
			CaPartitionLib::CA_HLL_Aff_Partitioner* cag_hll_est_partitioner = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 10);
			CaPartitionLib::CA_HLL_Partitioner* lag_hll_partitioner = new CaPartitionLib::CA_HLL_Partitioner(tasks, lag_policy, 10);
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
			CaPartitionLib::CA_Exact_Partitioner* cag_naive_partitioner = new CaPartitionLib::CA_Exact_Partitioner(tasks, cag_policy);
			CaPartitionLib::CA_Exact_Partitioner* lag_naive_partitioner = new CaPartitionLib::CA_Exact_Partitioner(tasks, lag_policy);
			CaPartitionLib::CA_PC_Partitioner* cag_pc_partitioner = new CaPartitionLib::CA_PC_Partitioner(tasks, cag_policy);
			CaPartitionLib::CA_PC_Partitioner* lag_pc_partitioner = new CaPartitionLib::CA_PC_Partitioner(tasks, lag_policy);
			CaPartitionLib::CA_HLL_Partitioner* cag_hll_partitioner = new CaPartitionLib::CA_HLL_Partitioner(tasks, cag_policy, 10);
			CaPartitionLib::CA_HLL_Aff_Partitioner* cag_hll_est_partitioner = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 10);
			CaPartitionLib::CA_HLL_Partitioner* lag_hll_partitioner = new CaPartitionLib::CA_HLL_Partitioner(tasks, lag_policy, 10);
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

void frequent_route_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines)
{
	RoundRobinPartitioner* rrg;
	PkgPartitioner* pkg;
	HashFieldPartitioner* fld;
	CardinalityAwarePolicy ca_policy;
	CaPartitionLib::CA_Exact_Partitioner* ca_naive;
	LoadAwarePolicy la_policy;
	CaPartitionLib::CA_Exact_Partitioner* la_naive;
	Experiment::DebsChallenge::FrequentRoutePartition experiment;
	std::vector<uint16_t> tasks;

	// tasks: 10
	for (uint16_t i = 0; i < 10; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	debs_frequent_route_performance_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	debs_frequent_route_performance_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	debs_frequent_route_performance_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	debs_frequent_route_performance_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	debs_frequent_route_performance_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
	// tasks: 100
	for (uint16_t i = 0; i < 100; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	debs_frequent_route_performance_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	debs_frequent_route_performance_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	debs_frequent_route_performance_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	debs_frequent_route_performance_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	debs_frequent_route_performance_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void most_profitable_cell_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines)
{
	RoundRobinPartitioner* rrg;
	PkgPartitioner* pkg;
	HashFieldPartitioner* fld;
	CardinalityAwarePolicy ca_policy;
	CaPartitionLib::CA_Exact_Partitioner* ca_naive;
	LoadAwarePolicy la_policy;
	CaPartitionLib::CA_Exact_Partitioner* la_naive;
	Experiment::DebsChallenge::FrequentRoutePartition experiment;
	std::vector<uint16_t> tasks;

	// tasks: 10
	for (uint16_t i = 0; i < 10; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	debs_most_profitable_cell_performance_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
	// tasks: 100
	/*for (uint16_t i = 0; i < 100; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	debs_most_profitable_cell_performance_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	debs_most_profitable_cell_performance_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();*/
}

void debs_frequent_route_performance_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks,
	Partitioner& partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix)
{
	// get maximum and minimum running times
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::vector<Experiment::DebsChallenge::frequent_route> partial_result;
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
	// write out files and clean up memory
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i]->flush();
		out_file[i]->close();
		delete out_file[i];
	}
	delete[] out_file;

	// for every task - calculate (partial) workload
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<Experiment::DebsChallenge::frequent_route> p;
		std::vector<Experiment::DebsChallenge::CompactRide>* task_lines = new std::vector<Experiment::DebsChallenge::CompactRide>();
		std::string workload_file_name = partitioner_name + "_" + std::to_string(i) + ".csv";
		experiment.parse_debs_rides_with_to_string(workload_file_name, task_lines);
		// feed the worker
		std::queue<Experiment::DebsChallenge::CompactRide> queue;
		std::mutex mu;
		std::condition_variable cond;
		Experiment::DebsChallenge::FrequentRouteWorkerThread worker(nullptr, nullptr, nullptr, &queue, &mu, &cond, 
			worker_output_file_name_prefix + "_" + std::to_string(i));

		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - START
		for (auto it = task_lines->begin(); it != task_lines->end(); ++it)
		{
			worker.update(*it);
		}
		
		worker.partial_finalize(p);
		partial_result.reserve(partial_result.size() + p.size());
		std::move(p.begin(), p.end(), std::inserter(partial_result, partial_result.end()));
		// TIME CRITICAL CODE - END
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		
		std::chrono::duration<double, std::milli> execution_time = end - start;

		sum_of_durations += execution_time.count();
		if (max_duration < execution_time.count())
		{
			max_duration = execution_time.count();
		}
		min_duration = i == 0 ? execution_time.count() : (min_duration > execution_time.count() ? execution_time.count() : min_duration);
		
		p.clear();
		task_lines->clear();
		delete task_lines;
		std::remove(workload_file_name.c_str());
	}

	Experiment::DebsChallenge::FrequentRouteOfflineAggregator aggregator;
	// TIME CRITICAL CODE - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_sort_final_aggregation(partial_result, partitioner_name + "_full_result.csv");
	}
	else
	{
		aggregator.sort_final_aggregation(partial_result, partitioner_name + "_full_result.csv");
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL CODE - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() << 
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";

	partial_result.clear();
}

void debs_most_profitable_cell_performance_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks,
	Partitioner& partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix)
{
	// get maximum and minimum running times
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::vector<Experiment::DebsChallenge::most_profitable_cell> partial_result;
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
	// distribute tuples - TODO: Finish it up!
	for (auto it = rides.cbegin(); it != rides.cend(); ++it)
	{
		std::string key = it->medallion;
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		*out_file[task] << it->to_string() << "\n";
	}
	// write out files and clean up memory
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i]->flush();
		out_file[i]->close();
		delete out_file[i];
	}
	delete[] out_file;

	// for every task - calculate (partial) workload
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<Experiment::DebsChallenge::most_profitable_cell> p;
		std::vector<Experiment::DebsChallenge::CompactRide>* task_lines = new std::vector<Experiment::DebsChallenge::CompactRide>();
		std::string workload_file_name = partitioner_name + "_" + std::to_string(i) + ".csv";
		experiment.parse_debs_rides_with_to_string(workload_file_name, task_lines);
		// feed the worker
		std::queue<Experiment::DebsChallenge::CompactRide> queue;
		std::mutex mu;
		std::condition_variable cond;
		Experiment::DebsChallenge::ProfitableArea worker(&queue, &mu, &cond, worker_output_file_name_prefix + "_" + std::to_string(i));

		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - START
		for (auto it = task_lines->begin(); it != task_lines->end(); ++it)
		{
			worker.update(*it);
		}

		worker.partial_finalize(p);
		partial_result.reserve(partial_result.size() + p.size());
		std::move(p.begin(), p.end(), std::inserter(partial_result, partial_result.end()));
		// TIME CRITICAL CODE - END
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();

		std::chrono::duration<double, std::milli> execution_time = end - start;

		sum_of_durations += execution_time.count();
		if (max_duration < execution_time.count())
		{
			max_duration = execution_time.count();
		}
		min_duration = i == 0 ? execution_time.count() : (min_duration > execution_time.count() ? execution_time.count() : min_duration);

		p.clear();
		task_lines->clear();
		delete task_lines;
		std::remove(workload_file_name.c_str());
	}

	Experiment::DebsChallenge::ProfitableAreaOfflineAggregator aggregator;
	// TIME CRITICAL CODE - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_sort_final_aggregation(partial_result, partitioner_name + "_full_result.csv");
	}
	else
	{
		aggregator.sort_final_aggregation(partial_result, partitioner_name + "_full_result.csv");
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL CODE - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";

	partial_result.clear();
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