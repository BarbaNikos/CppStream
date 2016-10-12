#pragma once
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

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef DEBS_STRUCTURE_LIB_H_
#include "debs_structure_lib.h"
#endif // !DEBS_STRUCTURE_LIB_H_

#ifndef DEBS_CELL_COORDINATE_UTIL_H_
#include "debs_cell_coordinate_util.h"
#endif // !DEBS_CELL_COORDINATE_UTIL_H_

#ifndef DEBS_QUERY_LIB_H_
#define DEBS_QUERY_LIB_H_
namespace Experiment
{
	namespace DebsChallenge
	{
		class FrequentRouteWorkerThread
		{
		public:
			FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, std::mutex* aggr_mu, std::condition_variable* aggr_cond,
				std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond);
			FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, std::mutex* aggr_mu, std::condition_variable* aggr_cond,
				std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond, const std::string result_output_file_name);
			~FrequentRouteWorkerThread();
			void operate();
			void update(DebsChallenge::CompactRide& ride);
			void finalize();
			void partial_finalize(std::vector<Experiment::DebsChallenge::frequent_route>&);
		private:
			std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue;
			std::mutex* aggr_mu; 
			std::condition_variable* aggr_cond;
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, uint64_t> result;
			std::queue<DebsChallenge::CompactRide>* input_queue;
			std::string result_output_file_name;
		};

		class FrequentRouteOfflineAggregator
		{
		public:
			FrequentRouteOfflineAggregator();
			~FrequentRouteOfflineAggregator();
			void sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& full_aggregates, const std::string& outfile_name);
			void calculate_and_sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& partial_aggregates, const std::string& outfile_name);
		private:
			std::map<std::string, uint64_t> result;
		};

		class FrequentRoutePartition
		{
		public:
			FrequentRoutePartition();
			~FrequentRoutePartition();
			void produce_compact_ride_file(const std::string input_file_name, const std::string output_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
			std::vector<Experiment::DebsChallenge::CompactRide> parse_debs_rides(const std::string input_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
			void parse_debs_rides_with_to_string(const std::string input_file_name, std::vector<Experiment::DebsChallenge::CompactRide>* buffer);
			void debs_partition_performance(const std::vector<uint16_t>& tasks, Partitioner& partitioner, const std::string partioner_name, std::vector<Experiment::DebsChallenge::CompactRide>& rides);
			double debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table, Partitioner& partitioner, 
				const std::string partitioner_name, const size_t max_queue_size);
		private:
			static void debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRouteWorkerThread* frequent_route);
			std::queue<Experiment::DebsChallenge::CompactRide>** queues;
			std::mutex* mu_xes;
			std::condition_variable* cond_vars;
			std::thread** threads;
			Experiment::DebsChallenge::FrequentRouteWorkerThread** query_workers;
			size_t max_queue_size;
		};

		class ProfitableArea
		{
		public:
			ProfitableArea(std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond);
			ProfitableArea(std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond, const std::string result_output_file_name);
			~ProfitableArea();
			void operate();
			void update(DebsChallenge::CompactRide& ride);
			void finalize();
			void partial_finalize(std::vector<Experiment::DebsChallenge::most_profitable_cell>&);
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, std::vector<float>> fare_map;
			std::unordered_map<std::string, std::string> dropoff_cell;
			std::queue<DebsChallenge::CompactRide>* input_queue;
			std::string result_output_file_name;
		};

		class ProfitableAreaOfflineAggregator
		{
		public:
			ProfitableAreaOfflineAggregator();
			~ProfitableAreaOfflineAggregator();
			void sort_final_aggregation(const std::vector<Experiment::DebsChallenge::most_profitable_cell>& full_aggregates, const std::string& outfile_name);
			void calculate_and_sort_final_aggregation(const std::vector<Experiment::DebsChallenge::most_profitable_cell>& partial_aggregates, const std::string& outfile_name);
		private:
			std::unordered_map<std::string, uint64_t> result;
		};

		class ProfitableAreaPartition
		{
		public:
			ProfitableAreaPartition();
			~ProfitableAreaPartition();
			double debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table, Partitioner& partitioner,
				const std::string partitioner_name, const size_t max_queue_size);
		private:
			static void debs_profitable_area_worker(Experiment::DebsChallenge::ProfitableArea* profitable_area);
			std::queue<Experiment::DebsChallenge::CompactRide>** queues;
			std::mutex* mu_xes;
			std::condition_variable* cond_vars;
			std::thread** threads;
			Experiment::DebsChallenge::ProfitableArea** query_workers;
			size_t max_queue_size;
		};
	}
}
#endif // !DEBS_QUERY_LIB_H_