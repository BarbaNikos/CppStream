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

		class TotalCpuPerCategoryWorkerThread
		{
		public:
			TotalCpuPerCategoryWorkerThread(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~TotalCpuPerCategoryWorkerThread();
			void operate();
			void update(Experiment::GoogleClusterMonitor::task_event& task_event);
			void finalize();
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
			void sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>&full_aggregates, const std::string& outfile_name);
			void calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_aggregates, const std::string& outfile_name);
		private:
			std::map<int, cm_one_result> result;
		};

		class TotalCpuPerCategoryPartition
		{
		public:
			TotalCpuPerCategoryPartition();
			~TotalCpuPerCategoryPartition();
			std::vector<Experiment::GoogleClusterMonitor::task_event> parse_task_events(const std::string input_file_name);
		private:
		};
	}
}
#endif // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

