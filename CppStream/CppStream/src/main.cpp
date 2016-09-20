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

void bit_tricks_scenario()
{
	uint32_t i = 8; // 0x00000008
	uint32_t l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000008
	uint32_t f_l = BitWizard::lowest_order_bit_index(i);
	uint32_t h = BitWizard::highest_order_bit_index_slow(i); // 0x00000008
	uint32_t f_h = BitWizard::highest_order_bit_index(i);
	uint64_t h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	uint64_t f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	
	//uint64_t f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//uint64_t f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	
	if (l != f_l)
	{
		std::cout << "first one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "first one failed on highest\n";
	}
	if (h_64 != f_h_64)
	{
		std::cout << "first one failed on highest (x64)\n";
	}
	i = 15; // 0x0000000f
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x00000008
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h = BitWizard::highest_order_bit_index(i);
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "second one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "second one failed on highest\n";
	}
	i = 123452; // 0x0001e23c
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000004
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x00010000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "third one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "third one failed on highest\n";
	}
	i = uint32_t(0xffffffff);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "fourth one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "fourth one failed on highest\n";
	}
	i = uint32_t(0x80000000);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x800000000
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "fifth one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "fifth one failed on highest\n";
	}
	i = uint32_t(0x80000001);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "sixth one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "sixth one failed on highest\n";
		return;
	}
	i = uint32_t(0x00000000);
	l = BitWizard::lowest_order_bit_index_slow(i);
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i);
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
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
	sum = 0; f_sum = 0; f_arch_sum = 0;
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
	sum = 0; f_sum = 0; f_arch_sum = 0;
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
	sum = 0; f_sum = 0; f_arch_sum = 0;
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
		//uint64_t f_l_64_arch = 0x8000000000000000 >> __lzcnt64(i);
		uint64_t f_l_64_arch = BitWizard::highest_order_bit_index_arch(uint64_t(i));
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

int main(int argc, char** argv)
{
	//char ch;
	if (argc < 2)
	{
		std::cout << "usage: <input-file> <worker-num>\n";
		exit(1);
	}
	//std::string lineitem_file_name = "D:\\tpch_2_17_0\\lineitem_sample.tbl";
	std::string lineitem_file_name = argv[1];
	uint16_t task_num = std::stoi(argv[2]);
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

	// std::cout << "size of CustomRide: " << sizeof(Experiment::DebsChallenge::CustomRide) << " bytes.\n";
	std::vector<Experiment::DebsChallenge::Ride> ride_table = Experiment::DebsChallenge::FrequentRoutePartition::parse_debs_rides(lineitem_file_name);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_compare_cag_correctness(tasks, ride_table);
	//Experiment::DebsChallenge::FrequentRoutePartition::debs_partition_performance(tasks, ride_table);
	/*Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_fld_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_pkg_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_cag_naive_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_lag_naive_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_cag_pc_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_lag_pc_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_cag_hll_concurrent_partition(tasks, ride_table);
	Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_lag_hll_concurrent_partition(tasks, ride_table);*/
	
	/*std::cout << "Press any key to continue...\n";
	std::cin >> ch;
*/
	return 0;
}
