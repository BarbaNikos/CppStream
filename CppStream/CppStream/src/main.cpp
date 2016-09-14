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

#include "../include/BasicWindow.h"
#include "../include/pkg_partitioner.h"
#include "../include/window_partitioner.h"
#include "../include/debs_challenge_util.h"
#include "../include/BitTrickBox.h"
#include "../include/cag_partitioner.h"
#include "../include/tpch_util.h"
#include "../include/tpch_query.h"

//const std::vector<uint16_t> tasks = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17 };
const std::vector<uint16_t> tasks = { 1, 2, 3, 4 };

void vanilla_main()
{
	std::vector<std::string> lines;
	std::string line;
	std::string input_file_name = "D:\\tpch_2_17_0\\dbgen\\orders.tbl";
	std::chrono::high_resolution_clock::time_point scan_start, scan_end;
	std::vector<uint16_t> tasks = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file." << std::endl;
		exit(1);
	}

	scan_start = std::chrono::high_resolution_clock::now();
	while (getline(file, line))
	{
		lines.push_back(line);
	}
	scan_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::micro> scan_time = scan_end - scan_start;

	lines.shrink_to_fit();
	file.close();
	std::cout << "Time to scan file: " << scan_time.count() << " (microsec). Total lines: " << lines.size() << std::endl;

	std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
	uint64_t len = 0;
	for (std::vector<std::string>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		len += it->length();
	}
	std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::nano> traverse_time = end - start;
	std::cout << "Time to traverse lines: " << traverse_time.count() << " (nanosec)." << std::endl;

	PkgPartitioner pkg(tasks);
	start = std::chrono::high_resolution_clock::now();
	for (std::vector<std::string>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		pkg.partition_next(*it, it->length());
	}
	end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::micro> partition_time = end - start;
	std::cout << "(A)-PKG Time to partition lines: " << partition_time.count() << " (microsec)." << std::endl;

	uint64_t window = 1000;
	uint64_t slide = 100;
	const size_t buffer_size = (size_t)ceil(window / slide);
	WindowPartitioner<std::string> window_partitioner(window, slide, tasks, 10);
	start = std::chrono::high_resolution_clock::now();
	for (std::vector<std::string>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		window_partitioner.partition_next(std::time(nullptr), *it, it->length());
	}
	end = std::chrono::high_resolution_clock::now();
	partition_time = end - start;
	std::cout << "(A)-LAG Time to partition lines: " << partition_time.count() << " (microsec)." << std::endl;
}

void debs_partition_performance(std::string input_file_name)
{
	std::vector<DebsChallenge::Ride> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	DebsChallenge::CellAssign cell_assign;
	std::chrono::system_clock::time_point scan_start = std::chrono::system_clock::now();
	while (getline(file, line))
	{
		DebsChallenge::Ride ride;
		cell_assign.parse_cells(line, ride);
		lines.push_back(ride);
	}
	std::chrono::system_clock::time_point scan_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	lines.shrink_to_fit();
	file.close();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";

	PkgPartitioner pkg(tasks);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> pkg_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		pkg_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = pkg.partition_next(key, key.length());
		pkg_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Time partition using PKG: " << pkg_partition_time.count() << " (msec).\n";

	CardinalityAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, policy);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> cag_naive_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_naive_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_naive.partition_next(key, key.length());
		cag_naive_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	std::cout << "Time partition using CAG(naive): " << cag_naive_partition_time.count() << " (msec).\n";

	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner lag_naive(tasks, lag_policy);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> lag_naive_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		lag_naive_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = lag_naive.partition_next(key, key.length());
		lag_naive_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	std::cout << "Time partition using LAG(naive): " << lag_naive_partition_time.count() << " (msec).\n";

	CagPartionLib::CagPcPartitioner cag_pc(tasks, policy);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> cag_pc_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_pc_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_pc.partition_next(key, key.length());
		cag_pc_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	std::cout << "Time partition using CAG(pc): " << cag_pc_partition_time.count() << " (msec).\n";

	CagPartionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> lag_pc_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		lag_pc_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = lag_pc.partition_next(key, key.length());
		lag_pc_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	std::cout << "Time partition using LAG(pc): " << lag_pc_partition_time.count() << " (msec).\n";

	CagPartionLib::CagHllPartitioner cag_hll(tasks, policy, 5);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> cag_hll_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_hll_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_hll.partition_next(key, key.length());
		cag_hll_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	std::cout << "Time partition using CAG(hll): " << cag_hll_partition_time.count() << " (msec).\n";

	CagPartionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
	std::unordered_map<uint16_t, std::unordered_set<std::string>> lag_hll_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		lag_hll_key_per_task[tasks[i]] = std::unordered_set<std::string>();
	}
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = lag_hll.partition_next(key, key.length());
		lag_hll_key_per_task[tasks[task]].insert(key);
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	std::cout << "Time partition using LAG(hll): " << lag_hll_partition_time.count() << " (msec).\n";

	//std::vector<uint32_t> cag_naive_cardinality;
	//cag_naive.get_cardinality_vector(cag_naive_cardinality);
	//std::vector<uint32_t> cag_pc_cardinality;
	//cag_pc.get_cardinality_vector(cag_pc_cardinality);
	//std::vector<uint32_t> cag_hll_cardinality;
	//cag_hll.get_cardinality_vector(cag_hll_cardinality);
	//std::cout << "cag-naive size: " << cag_naive_cardinality.size() << ", cag-pc size: " << cag_pc_cardinality.size() << ", cag-hll size: " << cag_hll_cardinality.size() << std::endl;
	//uint32_t pc_delta = 0, hll_delta = 0;
	//std::cout << "***** Result Cardinalities *****" << std::endl;
	//for (size_t i = 0; i < tasks.size(); ++i)
	//{
	//	std::cout << "task-" << i << " " << cag_naive_cardinality[i] << " " << cag_pc_cardinality[i] << " " << cag_hll_cardinality[i] << std::endl;
	//}
	//std::cout << "Delta-PC: " << pc_delta << ", Delta-HLL: " << hll_delta << "." << std::endl;
	std::cout << "------ END -----\n";
}

void card_estimate_example()
{
	CardinalityEstimator::ProbCount prob_count;
	CardinalityEstimator::HyperLoglog hyper_loglog(10);
	for (size_t i = 0; i < 1000000000; ++i)
	{
		prob_count.update_bitmap(uint32_t(i));
		hyper_loglog.update_bitmap(uint32_t(i));
	}
	std::cout << "Probabilistic Count: estimated cardinality: " << prob_count.cardinality_estimation() << "\n";
	std::cout << "Hyper-Loglog: estimated cardinality: " << hyper_loglog.cardinality_estimation() << "\n";
}

void log_perf_test()
{
	uint32_t value = 2;
	const uint32_t upper_limit = UINT32_MAX / 32;
	uint32_t sum = 0;
	std::chrono::system_clock::time_point std_start = std::chrono::system_clock::now();
	while (value < upper_limit)
	{
		sum += std::log2(value);
		value *= 2;
	}
	std::chrono::system_clock::time_point std_end = std::chrono::system_clock::now();

	uint32_t c_value = 2;
	uint32_t c_cum = 0;
	std::chrono::system_clock::time_point custom_start = std::chrono::system_clock::now();
	while (c_value < upper_limit)
	{
		c_cum += BitWizard::log_base_2_of_power_of_2_uint(c_value);
		c_value *= 2;
	}
	std::chrono::system_clock::time_point custom_end = std::chrono::system_clock::now();

	if (sum != c_cum)
	{
		std::cout << "Sums do not match! sum: " << sum << ", c_sum: " << c_cum << ".\n";
	}
	std::chrono::duration<double, std::micro> std_duration = std_end - std_start;
	std::chrono::duration<double, std::micro> custom_duration = custom_end - custom_start;
	std::cout << "STD: " << std_duration.count() << ", Custom: " << custom_duration.count() << " (msec).\n";
}

void bit_tricks_scenario()
{
	uint32_t i = 8; // 0x00000008
	uint32_t l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000008
	uint32_t f_l = BitWizard::lowest_order_bit_index(i);
	uint32_t h = BitWizard::highest_order_bit_index_slow(i); // 0x00000008
	uint32_t f_h = BitWizard::highest_order_bit_index(i);
	uint64_t h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	uint64_t f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	
	uint64_t f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	uint64_t f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	
	if (l != f_l)
	{
		throw new std::exception("first one failed on lowest");
	}
	if (h != f_h)
	{
		throw new std::exception("first one failed on highest");
	}
	if (h_64 != f_h_64)
	{
		throw new std::exception("first one failed on highest (x64)");
	}
	i = 15; // 0x0000000f
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x00000008
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h = BitWizard::highest_order_bit_index(i);
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "second one failed on lowest" << std::endl;
	}
	if (h != f_h)
	{
		std::cout << "second one failed on highest" << std::endl;
	}
	i = 123452; // 0x0001e23c
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000004
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x00010000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		throw new std::exception("third one failed on lowest");
	}
	if (h != f_h)
	{
		throw new std::exception("third one failed on highest");
	}
	i = uint32_t(0xffffffff);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		throw new std::exception("fourth one failed on lowest");
	}
	if (h != f_h)
	{
		throw new std::exception("fourth one failed on highest");
	}
	i = uint32_t(0x80000000);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x800000000
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		throw new std::exception("fifth one failed on lowest");
	}
	if (h != f_h)
	{
		throw new std::exception("fifth one failed on highest");
	}
	i = uint32_t(0x80000001);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		throw new std::exception("sixth one failed on lowest");
	}
	if (h != f_h)
	{
		throw new std::exception("sixth one failed on highest");
	}
	i = uint32_t(0x00000000);
	l = BitWizard::lowest_order_bit_index_slow(i);
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i);
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
}

void bit_tricks_performance_16()
{
	const unsigned long upper_limit = ULONG_MAX / 32;
	size_t sum = 0, f_sum = 0, f_arch_sum = 0;
	std::chrono::system_clock::time_point normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t l_16 = BitWizard::lowest_order_bit_index_slow(uint16_t(i));
		sum += l_16;
	}
	std::chrono::system_clock::time_point normal_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16 = BitWizard::lowest_order_bit_index(uint16_t(i));
		f_sum += f_l_16;
	}
	std::chrono::system_clock::time_point fast_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16_arch = BitWizard::lowest_order_bit_index_arch(uint16_t(i));
		f_arch_sum += f_l_16_arch;
	}
	std::chrono::system_clock::time_point fast_arch_end = std::chrono::system_clock::now();

	std::chrono::duration<double, std::milli> normal_duration = normal_end - normal_start;
	std::chrono::duration<double, std::milli> fast_duration = fast_end - fast_start;
	std::chrono::duration<double, std::milli> fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-16-bit :: normal: " << normal_duration.count() << ", fast: " << 
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16_arch = BitWizard::lowest_order_bit_index_arch(uint16_t(i));
		f_arch_sum += f_l_16_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-16-bit without branch for fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	// high-order
	sum = 0; f_sum = 0; f_arch_sum;
	normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t l_16 = BitWizard::highest_order_bit_index_slow(uint16_t(i));
		sum += l_16;
	}
	normal_end = std::chrono::system_clock::now();
	fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16 = BitWizard::highest_order_bit_index(uint16_t(i));
		f_sum += f_l_16;
	}
	fast_end = std::chrono::system_clock::now();
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16_arch = BitWizard::highest_order_bit_index_arch(uint16_t(i));
		f_arch_sum += f_l_16_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	normal_duration = normal_end - normal_start;
	fast_duration = fast_end - fast_start;
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-16-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
}

void bit_tricks_performance_32()
{
	const unsigned long upper_limit = ULONG_MAX / 32;
	size_t sum = 0, f_sum = 0, f_arch_sum = 0;
	std::chrono::system_clock::time_point normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t l_32 = BitWizard::lowest_order_bit_index_slow(uint32_t(i));
		sum += l_32;
	}
	std::chrono::system_clock::time_point normal_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32 = BitWizard::lowest_order_bit_index(uint32_t(i));
		f_sum += f_l_32;
	}
	std::chrono::system_clock::time_point fast_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32_arch = BitWizard::lowest_order_bit_index_arch(uint32_t(i));
		f_arch_sum += f_l_32_arch;
	}
	std::chrono::system_clock::time_point fast_arch_end = std::chrono::system_clock::now();

	std::chrono::duration<double, std::milli> normal_duration = normal_end - normal_start;
	std::chrono::duration<double, std::milli> fast_duration = fast_end - fast_start;
	std::chrono::duration<double, std::milli> fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-32-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	// high-order
	sum = 0; f_sum = 0; f_arch_sum;
	normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t l_32 = BitWizard::highest_order_bit_index_slow(uint32_t(i));
		sum += l_32;
	}
	normal_end = std::chrono::system_clock::now();
	fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32 = BitWizard::highest_order_bit_index(uint32_t(i));
		f_sum += f_l_32;
	}
	fast_end = std::chrono::system_clock::now();
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32_arch = BitWizard::highest_order_bit_index_arch(uint32_t(i));
		f_arch_sum += f_l_32_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	normal_duration = normal_end - normal_start;
	fast_duration = fast_end - fast_start;
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-32-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
}

void bit_tricks_performance_64()
{
	const unsigned long upper_limit = ULONG_MAX / 32;
	size_t sum = 0, f_sum = 0, f_arch_sum = 0;
	std::chrono::system_clock::time_point normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t l_64 = BitWizard::lowest_order_bit_index_slow(uint64_t(i));
		sum += l_64;
	}
	std::chrono::system_clock::time_point normal_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64 = BitWizard::lowest_order_bit_index(uint64_t(i));
		f_sum += f_l_64;
	}
	std::chrono::system_clock::time_point fast_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64_arch = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
		f_arch_sum += f_l_64_arch;
	}
	std::chrono::system_clock::time_point fast_arch_end = std::chrono::system_clock::now();

	std::chrono::duration<double, std::milli> normal_duration = normal_end - normal_start;
	std::chrono::duration<double, std::milli> fast_duration = fast_end - fast_start;
	std::chrono::duration<double, std::milli> fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-64-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	// high-order
	sum = 0; f_sum = 0; f_arch_sum;
	normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t l_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
		sum += l_64;
	}
	normal_end = std::chrono::system_clock::now();
	fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64 = BitWizard::highest_order_bit_index(uint64_t(i));
		f_sum += f_l_64;
	}
	fast_end = std::chrono::system_clock::now();
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64_arch = BitWizard::highest_order_bit_index_arch(uint64_t(i));
		f_arch_sum += f_l_64_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	normal_duration = normal_end - normal_start;
	fast_duration = fast_end - fast_start;
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-64-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64_arch = 0x8000000000000000 >> __lzcnt64(i);
		f_arch_sum += f_l_64_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-64-bit without branch: " << fast_arch_duration.count() << " (msec).\n";
}

int bit_tricks_correctness_test()
{
	for (unsigned long i = 0; i < ULONG_MAX; ++i)
	{
		uint16_t l_16 = BitWizard::lowest_order_bit_index_slow(uint16_t(i));
		uint16_t f_l_16 = BitWizard::lowest_order_bit_index(uint16_t(i));
		uint16_t f_l_arch_16 = BitWizard::lowest_order_bit_index_arch(uint16_t(i));

		uint16_t h_16 = BitWizard::highest_order_bit_index_slow(uint16_t(i));
		uint16_t f_h_16 = BitWizard::highest_order_bit_index(uint16_t(i));
		uint16_t f_h_arch_16 = BitWizard::highest_order_bit_index_arch(uint16_t(i));

		if (l_16 != f_l_16 || l_16 != f_l_arch_16)
		{
			std::cout << "for input number: " << uint16_t(i) << ", l_16: " << l_16 << ", f_l_16: " << f_l_16 << ", f_l_arch_16: " << f_l_arch_16 << "\n";
			return 1;
		}
		if (h_16 != f_h_16 || h_16 != f_h_arch_16)
		{
			std::cout << "for input number: " << uint16_t(i) << ", h_16: " << h_16 << ", f_h_16: " << f_h_16 << ", f_h_arch_16: " << f_h_arch_16 << "\n";
			return 1;
		}
		uint32_t l_32 = BitWizard::lowest_order_bit_index_slow(uint32_t(i));
		uint32_t f_l_32 = BitWizard::lowest_order_bit_index(uint32_t(i));
		uint32_t f_l_arch_32 = BitWizard::lowest_order_bit_index_arch(uint32_t(i));

		uint32_t h_32 = BitWizard::highest_order_bit_index_slow(uint32_t(i));
		uint32_t f_h_32 = BitWizard::highest_order_bit_index(uint32_t(i));
		uint32_t f_h_arch_32 = BitWizard::highest_order_bit_index_arch(uint32_t(i));

		if (l_32 != f_l_32 || l_32 != f_l_arch_32)
		{
			std::cout << "for input number: " << uint32_t(i) << ", l_32: " << l_32 << ", f_l_32: " << f_l_32 << ", f_l_arch_32: " << f_l_arch_32 << "\n";
			return 1;
		}
		if (h_32 != f_h_32 || h_32 != f_h_arch_32)
		{
			std::cout << "for input number: " << uint32_t(i) << ", h_32: " << h_32 << ", f_h_32: " << f_h_32 << ", f_h_arch_32: " << f_h_arch_32 << "\n";
			return 1;
		}
		uint64_t l_64 = BitWizard::lowest_order_bit_index_slow(uint64_t(i));
		uint64_t f_l_64 = BitWizard::lowest_order_bit_index(uint64_t(i));
		uint64_t f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));

		uint64_t h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
		uint64_t f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
		uint64_t f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));

		if (l_64 != f_l_64 || l_64 != f_l_arch_64)
		{
			std::cout << "for input number: " << uint64_t(i) << ", l_64: " << l_64 << ", f_l_64: " << f_l_64 << ", f_l_arch_64: " << f_l_arch_64 << "\n";
			return 1;
		}
		if (h_64 != f_h_64 || h_64 != f_h_arch_64)
		{
			std::cout << "for input number: " << uint64_t(i) << ", h_64: " << h_64 << ", f_h_64: " << f_h_64 << ", f_h_arch_64: " << f_h_arch_64 << "\n";
			return 1;
		}
	}
	std::cout << "All numbers passed...\n";
	return 0;
}

std::vector<Tpch::lineitem> parse_tpch_lineitem(std::string input_file_name)
{
	std::vector<Tpch::lineitem> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	std::chrono::high_resolution_clock::time_point scan_start = std::chrono::high_resolution_clock::now();
	while (getline(file, line))
	{
		Tpch::lineitem line_item;
		Tpch::DataParser::parse_lineitem(line, line_item);
		lines.push_back(line_item);
	}
	std::chrono::high_resolution_clock::time_point scan_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	file.close();
	lines.shrink_to_fit();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";
	return lines;
}

std::vector<Tpch::order> parse_tpch_order(std::string input_file_name)
{
	std::vector<Tpch::order> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	std::chrono::high_resolution_clock::time_point scan_start = std::chrono::high_resolution_clock::now();
	while (getline(file, line))
	{
		Tpch::order order;
		Tpch::DataParser::parse_order(line, order);
		lines.push_back(order);
	}
	file.close();
	std::chrono::high_resolution_clock::time_point scan_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	file.close();
	line.shrink_to_fit();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";
	return lines;
}

void tpch_q1_performance(std::string input_file)
{
	std::vector<Tpch::lineitem> lines = parse_tpch_lineitem(input_file);

	std::cout << "------- TPCH Q-1 partition performance --------\n";
	// PKG
	PkgPartitioner pkg(tasks);
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = pkg.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Time partition using PKG: " << pkg_partition_time.count() << " (msec).\n";
	// CAG - naive
	CardinalityAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, policy);
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_naive.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	std::cout << "Time partition using CAG(naive): " << cag_naive_partition_time.count() << " (msec).\n";
	// LAG - naive
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner lag_naive(tasks, lag_policy);
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_naive.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	std::cout << "Time partition using LAG(naive): " << lag_naive_partition_time.count() << " (msec).\n";
	// CAG - pc
	CagPartionLib::CagPcPartitioner cag_pc(tasks, policy);
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_pc.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	std::cout << "Time partition using CAG(pc): " << cag_pc_partition_time.count() << " (msec).\n";
	// LAG - pc
	CagPartionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_pc.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	std::cout << "Time partition using LAG(pc): " << lag_pc_partition_time.count() << " (msec).\n";
	// CAG - hll
	CagPartionLib::CagHllPartitioner cag_hll(tasks, policy, 5);
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_hll.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	std::cout << "Time partition using CAG(hll): " << cag_hll_partition_time.count() << " (msec).\n";
	// LAG - hll
	CagPartionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_hll.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	std::cout << "Time partition using LAG(hll): " << lag_hll_partition_time.count() << " (msec).\n";
	std::cout << "------ END -----\n";
}

void tpch_q1_worker(Tpch::QueryOne* query_one)
{
	query_one->operate();
}

void pkg_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	PkgPartitioner pkg(tasks);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";
	
	// start partitioning
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = pkg.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (PKG) total partition time: " << 
		pkg_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void cag_naive_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, cag_policy);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_naive.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (CAG-naive) total partition time: " <<
		cag_naive_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void lag_naive_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, lag_policy);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_naive.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (LAG-naive) total partition time: " <<
		lag_naive_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void cag_pc_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagPcPartitioner cag_pc(tasks, cag_policy);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_pc.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (CAG-PC) total partition time: " <<
		cag_pc_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void lag_pc_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_pc.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (LAG-PC) total partition time: " <<
		lag_pc_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void cag_hll_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagHllPartitioner cag_hll(tasks, cag_policy, 5);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_hll.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (CAG-HLL) total partition time: " <<
		cag_hll_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void lag_hll_concurrent_partition(const std::vector<Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
	std::cout << "Partitioner thread-" << std::this_thread::get_id() << " INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_hll.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	std::cout << "Partioner thread-" << std::this_thread::get_id() << " (LAG-HLL) total partition time: " <<
		lag_hll_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

int main(int argc, char** argv)
{
	std::string lineitem_file_name = "D:\\tpch_2_17_0\\lineitem_sample.tbl";

	char ch;
	
	// vanilla_main();

	// bit_tricks_scenario();

	// card_estimate_example();

	// bit_tricks_correctness_test();

	// bit_tricks_performance_16();

	// bit_tricks_performance_32();

	// bit_tricks_performance_64();

	// log_perf_test();

	debs_partition_performance("D:\\Downloads\\DEBS2015-Challenge\\test_set_small.csv");

	// tpch_q1_performance(lineitem_file_name);

	std::vector<Tpch::lineitem> lineitem_table = parse_tpch_lineitem(lineitem_file_name);
	pkg_concurrent_partition(lineitem_table);
	cag_naive_concurrent_partition(lineitem_table);
	lag_naive_concurrent_partition(lineitem_table);
	cag_pc_concurrent_partition(lineitem_table);
	lag_pc_concurrent_partition(lineitem_table);
	cag_hll_concurrent_partition(lineitem_table);
	lag_hll_concurrent_partition(lineitem_table);

	std::cout << "Press any key to continue...\n";
	std::cin >> ch;

	return 0;
}