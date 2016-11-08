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
#include <numeric>
#include <limits>
#include <cstddef>

#ifndef _PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

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

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif // !IMBALANCE_SCORE_AGGR_H_

#ifndef PARTITIONER_FACTORY_H_
#include "partitioner_factory.h"
#endif // !PARTITIONER_FACTORY_H_

#ifndef DEBS_QUERY_LIB_H_
#define DEBS_QUERY_LIB_H_
namespace Experiment
{
	namespace DebsChallenge
	{
		class FrequentRouteWorkerThread
		{
		public:
			FrequentRouteWorkerThread() {}
			FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, std::mutex* aggr_mu, std::condition_variable* aggr_cond,
				std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~FrequentRouteWorkerThread();
			void operate();
			void update(const DebsChallenge::CompactRide& ride);
			void finalize(bool write, std::vector<Experiment::DebsChallenge::frequent_route>* partial_result);
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
			void sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& full_aggregates, std::vector<std::pair<unsigned long, std::string>>& result);
			void calculate_and_sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& partial_aggregates, std::vector<std::pair<unsigned long, std::string>>& result);
			void write_output_to_file(const std::vector<std::pair<unsigned long, std::string>>& result, const std::string& outfile_name);
		};

		class FrequentRoutePartition
		{
		public:
			FrequentRoutePartition();
			~FrequentRoutePartition();
			void produce_compact_ride_file(const std::string& input_file_name, const std::string& output_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
			std::vector<Experiment::DebsChallenge::CompactRide> parse_debs_rides(const std::string input_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
			void parse_debs_rides_with_to_string(const std::string input_file_name, std::vector<Experiment::DebsChallenge::CompactRide>* buffer);
			static void partition_thread_operate(bool write, std::string partitioner_name, Partitioner* partitioner,
				std::vector<Experiment::DebsChallenge::CompactRide>* rides, std::vector<std::vector<Experiment::DebsChallenge::CompactRide>>* worker_input_buffer,
				size_t task_number, float* imbalance, float* key_imbalance, double* duration);
			static void worker_thread_operate(bool write, std::vector<Experiment::DebsChallenge::CompactRide>* input, std::vector<Experiment::DebsChallenge::frequent_route>* result_buffer,
				double* total_duration);
			static void aggregation_thread_operate(bool write, std::string partitioner_name, std::vector<Experiment::DebsChallenge::frequent_route>* input_buffer,
				std::vector<std::pair<unsigned long, std::string>>* result, double* total_duration, std::string worker_output_file_name, double* io_duration);
			void frequent_route_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* lines, const size_t task_number);
			void frequent_route_partitioner_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* rides, const std::vector<uint16_t> tasks,
				Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name);
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
			ProfitableArea();
			ProfitableArea(std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~ProfitableArea();
			void operate();
			void update(const DebsChallenge::CompactRide& ride);
			void first_round_gather(std::vector<std::pair<std::string, std::vector<float>>>& fare_table,
				std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>& dropoff_table);
			void second_round_init();
			void second_round_update(const std::string& pickup_cell, const std::vector<float>& fare_list);
			void second_round_update(const std::string& medallion, const std::string& dropoff_cell, const time_t& timestamp);
			void partial_finalize(std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer); // place results in intermediate buffer - 2 choice partitioner
			void partial_second_step_finalize(std::vector<std::pair<std::string, std::pair<float, int>>>& partial_result_table);
			void finalize(std::unordered_map<std::string, float>& cell_profit_buffer); // place results in intermediate buffer - 1 choice partitioner
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, std::vector<float>> fare_map; // pickup-cell -> fare-amount list
			std::unordered_map<std::string, std::pair<std::string, std::time_t>> dropoff_table; // medallion -> (drop-off cell, timestamp)
			std::queue<DebsChallenge::CompactRide>* input_queue;
			std::unordered_map<std::string, float> pickup_cell_median_fare;
			std::unordered_map<std::string, unsigned long> dropoff_cell_empty_taxi_count;
		};

		class ProfitableAreaOfflineAggregator
		{
		public:
			ProfitableAreaOfflineAggregator();
			~ProfitableAreaOfflineAggregator();
			void step_one_materialize_aggregation(const std::vector<std::pair<std::string, std::vector<float>>>& fare_table, 
				std::vector<std::pair<std::string, std::vector<float>>>& final_fare_table, 
				const std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>& dropoff_table, 
				std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>& final_dropoff_table);
			void sort_final_aggregation(const std::unordered_map<std::string, float>& cell_profit_buffer, std::vector<std::pair<float, std::string>>& final_result);
			void calculate_and_sort_final_aggregation(const std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer, std::vector<std::pair<float, std::string>>& final_result);
			void output_result_to_file(const std::vector<std::pair<float, std::string>>& final_result, const std::string& out_file);
		private:
			std::unordered_map<std::string, uint64_t> result;
		};

		class ProfitableAreaPartition
		{
		public:
			ProfitableAreaPartition();
			~ProfitableAreaPartition();
			void most_profitable_cell_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* lines, const size_t task_number);
			static void thread_partition_medallion(bool writer, std::string partitioner_name, Partitioner* partitioner, std::vector<Experiment::DebsChallenge::CompactRide>* buffer,
				size_t task_number, float* imbalance, float* key_imbalance, double* duration, std::vector<std::vector<CompactRide>>* worker_buffer);
			static void thread_execution_aggregation_step_one(bool write, const std::string partitioner_name, const std::vector<std::pair<std::string, std::vector<float>>>* fare_table,
				const std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>* dropoff_table, std::vector<std::pair<std::string, std::vector<float>>>* fare_table_out,
				std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>* dropoff_table_out, double* total_duration);
			static void thread_partition_step_two(bool writer, std::string partitioner_name, Partitioner* partitioner, std::vector<std::pair<std::string, std::vector<float>>>* fares, 
				std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>* dropoffs, size_t task_number, float* fare_imbalance, float* fare_key_imbalance, 
				float* dropoff_imbalance, float* dropoff_key_imbalance, double* duration, std::vector<std::vector<std::pair<std::string, std::vector<float>>>>* fare_sub_table,
				std::vector<std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>>* dropoffs_sub_table);
			static void thread_execution_step_one(bool write, const std::vector<Experiment::DebsChallenge::CompactRide>* input_buffer, 
				std::vector<std::pair<std::string, std::vector<float>>>* fare_table, std::vector<std::pair<std::string, std::pair<std::string, time_t>>>* dropoff_table, double* total_duration);
			static void thread_final_aggregation(bool write, const std::string partitioner_name, std::unordered_map<std::string, std::pair<float, int>>* partial_input_buffer,
				const std::unordered_map<std::string, float>* full_input_buffer, std::vector<std::pair<float, std::string>>* final_result, const std::string worker_output_file_name,
				double* total_duration, double* io_duration);
			void most_profitable_partitioner_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* rides, const std::vector<uint16_t> tasks,
				Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name);
		};
	}
}
#endif // !DEBS_QUERY_LIB_H_