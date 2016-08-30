#include <iostream>
#include <ctime>
#include <fstream>
#include <string>
#include <chrono>

#include "BasicWindow.h"
#include "pkg_partitioner.h"

int t_main(int argc, char** argv)
{
	char ch;
	std::vector<std::string> lines;
	std::string line;
	std::string input_file_name = "D:\\tpch_2_17_0\\dbgen\\lineitem.tbl";
	std::chrono::high_resolution_clock::time_point scan_start, scan_end;
	std::array<uint16_t, 10> tasks = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
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
	
	start = std::chrono::high_resolution_clock::now();
	PkgPartitioner<tasks.size()> pkg(tasks);
	for (std::vector<std::string>::iterator it = lines.begin(); it != lines.end(); ++it)
	{
		pkg.partition_next(*it, it->length());
	}
	end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::micro> partition_time = end - start;
	std::cout << "Time to partition lines: " << partition_time.count() << " (microsec)." << std::endl;

	std::cout << "Press any key to continue..." << std::endl;
	std::cin >> ch;

	return 0;
}