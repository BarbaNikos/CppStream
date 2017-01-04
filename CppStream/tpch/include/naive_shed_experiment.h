#pragma once
#ifndef TPCH_QUERY_LIB_H_
#include "tpch_query_lib.h"
#endif // !TPCH_QUERY_LIB_H_

#ifndef TPCH_NAIVE_SHED_EXPERIMENT_H_
#define TPCH_NAIVE_SHED_EXPERIMENT_H_
namespace Experiment
{
	namespace Tpch
	{
		class ShedRouteLab
		{
		public:
			static std::map<std::string, Experiment::Tpch::query_one_result> correct_result(const std::vector<Experiment::Tpch::lineitem>& lines,
				const size_t task_num);
			static void gather_statistics(const std::vector<Experiment::Tpch::lineitem>& lines, const std::vector<uint16_t>& tasks, std::vector<unsigned long long>& tuple_count, std::vector<std::unordered_set<std::string>>& cardinality_count);
			static std::map<std::string, Experiment::Tpch::query_one_result> tpch_query_one_gather_statistics(const std::vector<Experiment::Tpch::lineitem>& lines, 
				const std::vector<uint16_t> tasks, const std::vector<unsigned long long>& tuple_count, const std::vector<std::unordered_set<std::string>>& cardinality_count);
		};
	}
}
#endif // !TPCH_NAIVE_SHED_EXPERIMENT_H_
std::map<std::string, Experiment::Tpch::query_one_result> Experiment::Tpch::ShedRouteLab::correct_result(const std::vector<Experiment::Tpch::lineitem>& lines,
	const size_t task_num)
{
	const std::string fld_out_file_name = "fld_out.csv";
	std::vector<Tpch::query_one_result> fld_intermediate_buffer;
	std::map<std::string, Experiment::Tpch::query_one_result> fld_result_buffer;
	std::vector<std::vector<Experiment::Tpch::lineitem>> fld_buffer(task_num, std::vector<Experiment::Tpch::lineitem>());
	std::vector<uint16_t> tasks;
	for (size_t i = 0; i < task_num; i++)
	{
		tasks.push_back(i);
	}
	HashFieldPartitioner fld(tasks);
	for (auto it = lines.cbegin(); it != lines.cend(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "," + std::to_string(it->l_linestatus);
		char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
		strcpy(c_key, key.c_str());
		uint16_t task = fld.partition_next(c_key, strlen(c_key));
		free(c_key);
		if (task < task_num)
		{
			fld_buffer[task].push_back(*it);
		}
	}
	for (size_t task = 0; task < task_num; ++task)
	{
		double duration;
		Experiment::Tpch::QueryOnePartition::thread_worker_operate(true, &fld_buffer[task], &fld_intermediate_buffer, &duration);
	}
	double aggregate_duration, io_duration;
	Experiment::Tpch::QueryOnePartition::thread_aggregate(true, "fld", &fld_intermediate_buffer, &fld_result_buffer,
		fld_out_file_name, &aggregate_duration, &io_duration);
	std::remove(fld_out_file_name.c_str());
	return fld_result_buffer;
}

void gather_statistics(const std::vector<Experiment::Tpch::lineitem>& lines, const std::vector<uint16_t>& tasks, std::vector<unsigned long long>& tuple_count, std::vector<std::unordered_set<std::string>>& cardinality_count)
{
	HashFieldPartitioner fld(tasks);
	// perform a first-pass to accumulate statistics
	for (auto it = lines.cbegin(); it != lines.cend(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "," + std::to_string(it->l_linestatus);
		char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
		strcpy(c_key, key.c_str());
		uint16_t task = fld.partition_next(c_key, strlen(c_key));
		free(c_key);
		if (task < tasks.size())
		{
			tuple_count[task]++;
			cardinality_count[task].insert(key);
		}
	}
}

std::map<std::string, Experiment::Tpch::query_one_result> Experiment::Tpch::ShedRouteLab::tpch_query_one_gather_statistics(
	const std::vector<Experiment::Tpch::lineitem>& lines, const std::vector<uint16_t> tasks, const std::vector<unsigned long long>& tuple_count, 
	const std::vector<std::unordered_set<std::string>>& cardinality_count)
{
	const std::string shed_fld_out_file_name = "shed_fld_out.csv";
	std::vector<Tpch::query_one_result> intermediate_buffer;
	std::map<std::string, Experiment::Tpch::query_one_result> result_buffer;
	std::vector<std::vector<Experiment::Tpch::lineitem>> buffer(tasks.size(), std::vector<Experiment::Tpch::lineitem>());
	HashFieldPartitioner fld(tasks);
	auto max_element = std::max_element(tuple_count.begin(), tuple_count.end());
	auto max_element_pos = std::distance(tuple_count.begin(), max_element);
	Partitioner* p_copy = PartitionerFactory::generate_copy("fld", &fld);
	p_copy->init();
	srand(time(nullptr));
	for (auto it = lines.cbegin(); it != lines.cend(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "," + std::to_string(it->l_linestatus);
		char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
		strcpy(c_key, key.c_str());
		uint16_t task = p_copy->partition_next(c_key, strlen(c_key));
		free(c_key);
		if (task < tasks.size())
		{
			// randomly decide to drop
			if (task == max_element_pos)
			{
				if (rand() % 2 == 0)
				{
					buffer[task].push_back(*it);
				}
			}
			else
			{
				buffer[task].push_back(*it);
			}
		}
	}
	delete p_copy;
	// now that the input buffers have the input for each opi -- calculate the result
	for (size_t task = 0; task < tasks.size(); ++task)
	{
		double duration;
		Experiment::Tpch::QueryOnePartition::thread_worker_operate(true, &buffer[task], &intermediate_buffer, &duration);
	}
	double aggregate_duration, io_duration;
	Experiment::Tpch::QueryOnePartition::thread_aggregate(true, "fld", &intermediate_buffer, &result_buffer, 
		shed_fld_out_file_name, &aggregate_duration, &io_duration);
	std::remove(shed_fld_out_file_name.c_str());
	return result_buffer;
}