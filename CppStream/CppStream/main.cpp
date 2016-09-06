#include <iostream>
#include <ctime>
#include <fstream>
#include <string>
#include <chrono>

#include "BasicWindow.h"
#include "pkg_partitioner.h"
#include "window_partitioner.h"
#include "debs_challenge_util.h"
#include "CardinalityEstimator.h"
#include "BitTrickBox.h"

const std::vector<uint16_t> tasks = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27 };

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

	std::cout << "Press any key to continue..." << std::endl;
}

void test_window_group()
{
	uint64_t window = 2;
	uint64_t slide = 1;
	std::vector<uint16_t> tasks = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
	WindowPartitioner<uint32_t> window_partitioner(window, slide, tasks, 2);
	auto s = uint32_t(1);
	time_t t = 1;
	window_partitioner.partition_next(t, s, sizeof(s));
	s = uint32_t(3);
	t = 3;
	window_partitioner.partition_next(t, s, sizeof(s));
	s = uint32_t(5);
	t = 5;
	window_partitioner.partition_next(t, s, sizeof(s));
}

void debs_parse_test()
{
	std::vector<DebsChallenge::Ride> lines;
	std::string line;
	std::string input_file_name = "D:\\Downloads\\DEBS2015-Challenge\\divided_data.small\\1\\1\\0.csv";
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file." << std::endl;
		exit(1);
	}
	DebsChallenge::CellAssign cell_assign;
	std::chrono::high_resolution_clock::time_point scan_start = std::chrono::high_resolution_clock::now();
	while (getline(file, line))
	{
		DebsChallenge::Ride ride;
		cell_assign.parse_cells(line, ride);
		lines.push_back(ride);
	}
	std::chrono::high_resolution_clock::time_point scan_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec)." << std::endl;

	PkgPartitioner pkg(tasks);
	std::chrono::high_resolution_clock::time_point pkg_start = std::chrono::high_resolution_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = pkg.partition_next(key, key.length());
	}
	std::chrono::high_resolution_clock::time_point pkg_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Time partition using PKG: " << pkg_partition_time.count() << " (msec)." << std::endl;

	uint64_t window = 1000 * 60 * 30; 
	uint64_t slide = 1000 * 60;
	const size_t buffer_size = (size_t)ceil(window / slide);
	WindowPartitioner<std::string> wag(window, slide, tasks, buffer_size);
	std::chrono::high_resolution_clock::time_point wag_start = std::chrono::high_resolution_clock::now();
	for (std::vector<DebsChallenge::Ride>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = wag.partition_next(it->dropoff_datetime, key, key.length());
	}
	std::chrono::high_resolution_clock::time_point wag_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> wag_partition_time = wag_end - wag_start;
	std::cout << "Time partition using WAG: " << wag_partition_time.count() << " (msec)." << std::endl;
}

void prob_count_example()
{
	/*CardinalityEstimator::ProbCount prob_count;
	prob_count.update_bitmap(1);*/
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

int main(int argc, char** argv)
{
	char ch;

	// test_window_group();

	// vanilla_main();

	// debs_parse_test();

	bit_tricks_scenario();

	prob_count_example();

	std::cin >> ch;

	return 0;
}