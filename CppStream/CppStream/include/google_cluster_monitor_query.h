#pragma once
#include <fstream>
#include <vector>
#include <array>
#include <map>
#include <sstream>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <ctime>
#include <queue>
#include <thread>
#include <future>
#include <algorithm>
#include <unordered_set>
#include <numeric>

#ifndef _PARTITIONER_H_
#include "partitioner.h"
#endif // !_PARTITIONER_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#include "round_robin_partitioner.h"
#endif // !ROUND_ROBIN_PARTITIONER_H_

#ifndef HASH_FLD_PARTITIONER_H_
#include "hash_fld_partitioner.h"
#endif // !HASH_FLD_PARTITIONER_H_

#ifndef PKG_PARTITIONER_H_
#include "pkg_partitioner.h"
#endif // !PKG_PARTITIONER_H_

#ifndef CA_PARTITION_LIB_H_
#include "ca_partition_lib.h"
#endif // !CA_PARTITION_LIB_H_

#ifndef DEBS_STRUCTURE_LIB_H_
#include "debs_structure_lib.h"
#endif // !DEBS_STRUCTURE_LIB_H_

#ifndef DEBS_CELL_COORDINATE_UTIL_H_
#include "debs_cell_coordinate_util.h"
#endif // !DEBS_CELL_COORDINATE_UTIL_H_

#ifndef GOOGLE_CLUSTER_MONITOR_UTIL_H_
#include "google_cluster_monitor_util.h"
#endif // !GOOGLE_CLUSTER_MONITOR_UTIL_H_

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#define GOOGLE_CLUSTER_MONITOR_QUERY_H_
namespace Experiment
{
	namespace GoogleClusterMonitor
	{
		typedef struct cm_one_result_str
		{
			cm_one_result_str() {}
			cm_one_result_str(long long timestamp, int category, float total_cpu) :
				timestamp(timestamp), category(category), total_cpu(total_cpu) {}
			cm_one_result_str(const cm_one_result_str& o)
			{
				timestamp = o.timestamp;
				category = o.category;
				total_cpu = o.total_cpu;
			}
			~cm_one_result_str() {}
			cm_one_result_str& operator= (const cm_one_result_str& o)
			{
				if (this != &o)
				{
					timestamp = o.timestamp;
					category = o.category;
					total_cpu = o.total_cpu;
				}
				return *this;
			}
			long long timestamp;
			int category;
			float total_cpu;
		}cm_one_result;

		typedef struct cm_two_result_str
		{
			cm_two_result_str() {}
			cm_two_result_str(long long timestamp, long job_id, float sum_cpu, long count) : timestamp(timestamp), job_id(job_id), sum_cpu(sum_cpu), count(count) {}
			cm_two_result_str(const cm_two_result_str& o)
			{
				timestamp = o.timestamp;
				job_id = o.job_id;
				sum_cpu = o.sum_cpu;
				count = o.count;
			}
			~cm_two_result_str() {}
			cm_two_result_str& operator= (const cm_two_result_str& o)
			{
				if (this != &o)
				{
					timestamp = o.timestamp;
					job_id = o.job_id;
					sum_cpu = o.sum_cpu;
					count = o.count;
				}
				return *this;
			}
			long long timestamp;
			long job_id;
			float sum_cpu;
			long count;
		}cm_two_result;

		class TotalCpuPerCategoryWorker
		{
		public:
			TotalCpuPerCategoryWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~TotalCpuPerCategoryWorker();
			void operate();
			void update(Experiment::GoogleClusterMonitor::task_event& task_event);
			//void finalize();
			void partial_finalize(std::vector<Experiment::GoogleClusterMonitor::cm_one_result>&);
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<int, cm_one_result> result;
			std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue;
		};

		class TotalCpuPerCategoryOfflineAggregator
		{
		public:
			TotalCpuPerCategoryOfflineAggregator();
			~TotalCpuPerCategoryOfflineAggregator();
			void sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>&full_aggregates, std::vector<cm_one_result>& final_result);
			void calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_aggregates, std::vector<cm_one_result>& final_result);
			void write_output_to_file(const std::vector<GoogleClusterMonitor::cm_one_result>& final_result, const std::string& outfile_name);
		};

		class TotalCpuPerCategoryPartition
		{
		public:
			static void parse_task_events(const std::string input_file_name, std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer);
			static void parse_task_events_from_directory(const std::string input_dir_name, std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer);
			static void query_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const size_t task_number);
			static void query_partitioner_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const std::vector<uint16_t> tasks,
				Partitioner& partitioner, const std::string partitioner_name, const std::string worker_output_file_name);
		private:
		};

		class MeanCpuPerJobIdWorker
		{
		public:
			MeanCpuPerJobIdWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~MeanCpuPerJobIdWorker();
			void operate();
			void update(Experiment::GoogleClusterMonitor::task_event& task_event);
			void partial_finalize(std::vector<Experiment::GoogleClusterMonitor::cm_two_result>&);
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<int, cm_two_result> result;
			std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue;
		};

		class MeanCpuPerJobIdOfflineAggregator
		{
		public:
			MeanCpuPerJobIdOfflineAggregator();
			~MeanCpuPerJobIdOfflineAggregator();
			void sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_two_result>&full_aggregates, std::vector<cm_two_result>& final_result);
			void calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_two_result>& partial_aggregates, std::vector<cm_two_result>& final_result);
			void write_output_to_file(const std::vector<cm_two_result>& final_result, const std::string& outfile_name);
		private:
			std::map<int, cm_two_result> result;
		};

		class MeanCpuPerJobIdPartition
		{
		public:
			static void query_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const size_t task_number);
			static void query_partitioner_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const std::vector<uint16_t> tasks,
				Partitioner& partitioner, const std::string partitioner_name, const std::string worker_output_file_name);
		};
	}
}
#endif // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

