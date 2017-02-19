#pragma once
#include <vector>
#include <map>
#include <unordered_map>
#include <queue>
#include <future>

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

#ifndef GOOGLE_CLUSTER_MONITOR_UTIL_H_
#include "google_cluster_monitor_util.h"
#endif // !GOOGLE_CLUSTER_MONITOR_UTIL_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif // !IMBALANCE_SCORE_AGGR_H_

#ifndef GCM_KEY_EXTRACTOR_H_
#include "gcm_key_extractor.h"
#endif

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#define GOOGLE_CLUSTER_MONITOR_QUERY_H_
namespace Experiment
{
	namespace GoogleClusterMonitor
	{
		typedef struct cm_one_result_str
		{
			cm_one_result_str() : timestamp(0), category(0), total_cpu(0) {}
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
			cm_two_result_str() : timestamp(0), job_id(0), sum_cpu(0), count(0) {}
			cm_two_result_str(long long timestamp, long job_id, float sum_cpu, long count) : timestamp(timestamp), job_id(job_id), 
				sum_cpu(sum_cpu), count(count) {}
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
			double mean_cpu;
		}cm_two_result;

		class TotalCpuPerCategoryWorker
		{
		public:
			void update(const task_event& task_event);
			void submit(std::vector<cm_one_result>&) const;
		private:
			std::unordered_map<int, cm_one_result> result;
		};

		class TotalCpuPerCategoryAggregator
		{
		public:
			static void sort_final_aggregation(const std::vector<cm_one_result>&full_aggregates, std::vector<cm_one_result>& final_result);
			static void sort_final_aggregation(const std::unordered_map<int, cm_one_result>& buffer, std::vector<cm_one_result>& result);
			static void calculate_final_aggregation(const std::vector<cm_one_result>& partial_aggregates, std::unordered_map<int, cm_one_result>& final_result_buffer);
			static void write_output_to_file(const std::vector<cm_one_result>& final_result, const std::string& outfile_name);
		};

		class TotalCpuPerCategoryPartition
		{
		public:
			static void parse_task_events(const std::string& input_file_name, std::vector<task_event>& buffer);
			static void parse_task_events_from_directory(const std::string& input_dir_name, std::vector<task_event>& buffer);
			static void query_simulation(const std::vector<task_event>& buffer, const size_t task_number);
			static std::vector<double> query_partitioner_simulation(const bool write, const std::vector<task_event>& buffer, const std::vector<uint16_t>& tasks,
				std::unique_ptr<Partitioner>& partitioner, const std::string& partitioner_name, const std::string& worker_output_file_name);
			static void query_window_simulation(const std::vector<task_event>& buffer, const size_t task_number);
			static void query_window_partitioner_simulation(const std::vector<task_event>& buffer, const std::vector<uint16_t>& tasks,
				std::unique_ptr<Partitioner>& partitioner, const std::string& partitioner_name);
		};

		class MeanCpuPerJobIdWorker
		{
		public:
			void update(const task_event& task_event);
			void submit(std::vector<cm_two_result>&) const;
		private:
			std::unordered_map<int, cm_two_result> result;
		};

		class MeanCpuPerJobIdAggregator
		{
		public:
			static void order_final_result(const std::vector<cm_two_result>&buffer, std::vector<cm_two_result>& final_result);
			static void order_final_result(const std::unordered_map<long, cm_two_result>& buffer, std::vector<cm_two_result>& final_result);
			static void calculate_final_aggregation(const std::vector<cm_two_result>& partial_aggregates, std::unordered_map<long, cm_two_result>& final_result);
			static void write_output_to_file(const std::vector<cm_two_result>& final_result, const std::string& outfile_name);
		};

		class MeanCpuPerJobIdPartition
		{
		public:
			static void query_simulation(const std::vector<task_event>& buffer, const size_t task_number);
			static std::vector<double> query_partitioner_simulation(bool write, const std::vector<task_event>& buffer, const std::vector<uint16_t>& tasks,
				std::unique_ptr<Partitioner>& partitioner, const std::string& partitioner_name, const std::string& worker_output_file_name);
			static void query_window_simulation(const std::vector<task_event>& buffer, const size_t task_number);
			static void query_window_partitioner_simulation(const std::vector<task_event>& buffer, const std::vector<uint16_t>& tasks,
				std::unique_ptr<Partitioner>& partitioner, const std::string& partitioner_name);
		};
	}
}
#endif // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

