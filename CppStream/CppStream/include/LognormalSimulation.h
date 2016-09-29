#pragma once
#include <vector>
#include <cinttypes>
#include <random>
#include <chrono>
#include <unordered_map>
#include <fstream>
#include <string>
#include <map>

#include "hash_fld_partitioner.h"
#include "pkg_partitioner.h"
#include "cag_partitioner.h"

#ifndef EXPERIMENT_LOGNORMAL_SIMULATION_H_
#define EXPERIMENT_LOGNORMAL_SIMULATION_H_
namespace Experiment
{
	class LogNormalSimulation
	{
	public:
		LogNormalSimulation();
		~LogNormalSimulation();
		void sort_to_plot(std::string input_file);
		void simulate(std::vector<uint16_t> tasks, std::string input_file);
		void measure_imbalance(const std::vector<uint32_t>& stream, const std::vector<uint16_t>& tasks, Partitioner& partitioner,
			const std::string partioner_name);
	};

	LogNormalSimulation::LogNormalSimulation()
	{
	}

	LogNormalSimulation::~LogNormalSimulation()
	{
	}

	void LogNormalSimulation::sort_to_plot(std::string input_file)
	{
		std::ifstream input(input_file);
		std::ofstream output;
		output.open("log_norm_1.csv");
		std::string line;
		std::unordered_map<uint32_t, uint32_t> frequency;
		if (!input.is_open())
		{
			std::cout << "could not open stream file " << input_file << "\n";
			exit(1);
		}
		while (getline(input, line))
		{
			uint32_t element = std::stoi(line);
			auto it = frequency.find(element);
			if (it == frequency.end())
			{
				frequency.insert(std::make_pair(element, 1));
			}
			else
			{
				it->second += 1;
			}
		}
		input.close();
		std::cout << "scanned file and created frequency map\n";
		std::map<uint32_t, std::vector<uint32_t>> sort_output;
		for (auto it = frequency.cbegin(); it != frequency.cend(); it++)
		{
			/*auto it_1 = sort_output.find(it->second);
			if (it_1 != sort_output.end())
			{
				it_1->second.push_back(it->first);
			}
			else
			{
				std::vector<uint32_t> tmp;
				tmp.push_back(it->first);
				sort_output.insert(std::make_pair(it->second, tmp));
			}*/
			output << it->first << "," << it->second << "\n";
		}
		frequency.clear();
		/*std::cout << "clustered counts together.\n";
		for (auto it = sort_output.begin(); it != sort_output.end(); it++)
		{
			if (it->second.size() > 0)
			{
				for (auto it_1 = it->second.begin(); it_1 != it->second.end(); it_1++)
				{
					output << *it_1 << "," << it->first << "\n";
				}
			}
		}*/
		output.close();
	}

	void LogNormalSimulation::simulate(std::vector<uint16_t> tasks, std::string input_file)
	{
		std::ifstream input(input_file);
		std::string line;
		std::vector<uint32_t> stream;
		if (!input.is_open())
		{
			std::cout << "could not open stream file " << input_file << "\n";
			exit(1);
		}
		while (getline(input, line))
		{
			uint32_t element = std::stoi(line);
			stream.push_back(element);
		}
		input.close();
		stream.shrink_to_fit();
		CardinalityAwarePolicy cag;
		LoadAwarePolicy lag;
		// FLD
		HashFieldPartitioner fld_partitioner(tasks);
		measure_imbalance(stream, tasks, fld_partitioner, "FLD");
		// PKG
		PkgPartitioner pkg_partitioner(tasks);
		measure_imbalance(stream, tasks, pkg_partitioner, "PKG");
		// CAG-naive
		CagPartitionLib::CagNaivePartitioner cag_naive(tasks, cag);
		measure_imbalance(stream, tasks, cag_naive, "CAG-naive");
		// LAG-naive
		CagPartitionLib::CagNaivePartitioner lag_naive(tasks, lag);
		measure_imbalance(stream, tasks, lag_naive, "LAG-naive");
		// CAG-pc
		CagPartitionLib::CagPcPartitioner cag_pc(tasks, cag);
		measure_imbalance(stream, tasks, cag_pc, "CAG-pc");
		// LAG-pc
		CagPartitionLib::CagPcPartitioner lag_pc(tasks, lag);
		measure_imbalance(stream, tasks, lag_pc, "LAG-pc");
		// CAG-hll
		CagPartitionLib::CagHllPartitioner cag_hll(tasks, cag, 10);
		measure_imbalance(stream, tasks, cag_hll, "CAG-hll");
		// CAG-hll-est
		CagPartitionLib::CagHllEstPartitioner cag_est_hll(tasks, 10);
		measure_imbalance(stream, tasks, cag_est_hll, "CAG-est-hll");
		// LAG-hll
		CagPartitionLib::CagHllPartitioner lag_hll(tasks, lag, 10);
		measure_imbalance(stream, tasks, lag_hll, "LAG-hll");
	}

	void LogNormalSimulation::measure_imbalance(const std::vector<uint32_t>& stream, const std::vector<uint16_t>& tasks, Partitioner & partitioner, const std::string partioner_name)
	{
		std::vector<std::unordered_set<std::string>> key_per_task;
		uint64_t* tuple_count = (uint64_t*)calloc(tasks.size(), sizeof(uint64_t));
		for (size_t i = 0; i < tasks.size(); ++i)
		{
			key_per_task.push_back(std::unordered_set<std::string>());
			tuple_count[i] = 0;
		}
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (std::vector<uint32_t>::const_iterator it = stream.cbegin(); it != stream.cend(); ++it)
		{
			std::string key = std::to_string(*it);
			short task = partitioner.partition_next(key, key.length());
			key_per_task[task].insert(key);
			tuple_count[task]++;
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> partition_time = part_end - part_start;
		size_t min_cardinality = key_per_task[0].size();
		size_t max_cardinality = key_per_task[0].size();
		size_t min_count = tuple_count[0];
		size_t max_count = tuple_count[0];
		double average_cardinality = 0;
		double average_count = 0;
		// std::cout << "Cardinalities: ";
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
			if (min_count > tuple_count[i])
			{
				min_count = tuple_count[i];
			}
			if (max_count < tuple_count[i])
			{
				max_count = tuple_count[i];
			}
			average_cardinality += key_per_task[i].size();
			average_count += tuple_count[i];
			// std::cout << key_per_task[i].size() << " ";
			key_per_task[i].clear();
		}
		std::cout << "\n";
		average_cardinality = average_cardinality / tasks.size();
		average_count = average_count / tasks.size();
		key_per_task.clear();
		std::cout << "Time partition using " << partioner_name << ": " << partition_time.count() << " (msec).\n";
		std::cout << "\tCardinality - min: " << min_cardinality << ", max: " << max_cardinality << ", avg: " << average_cardinality << "\n";
		std::cout << "\tCount - min: " << min_count << ", max: " << max_count << ", avg: " << average_count << "\n";
		std::cout << "\tImbalance: " << (max_count - average_count) << "\n";
		std::cout << "\tKey Imbalance: " << (max_cardinality - average_cardinality) << "\n";
		std::cout << partioner_name << "-end\n";
	}
	
}
#endif // !EXPERIMENT_LOGNORMAL_SIMULATION_H_
