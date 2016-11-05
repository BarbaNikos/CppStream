#pragma once
#include <iostream>
#include <thread>
#include <future>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <cstdio>
#include <numeric>

#ifndef PARTITIONER_H_
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

#ifndef TPCH_STRUCT_LIB_H_
#include "tpch_struct_lib.h"
#endif // !TPCH_STRUCT_LIB_H_

#ifndef TPCH_UTIL_H_
#include "tpch_util.h"
#endif // !TPCH_UTIL_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif // !IMBALANCE_SCORE_AGGR_H_

#ifndef PARTITIONER_FACTORY_H_
#include "partitioner_factory.h"
#endif // !PARTITIONER_FACTORY_H_

#ifndef TPCH_QUERY_LIB_H_
#define TPCH_QUERY_LIB_H_
namespace Experiment
{
	namespace Tpch
	{
		typedef struct query_one_result_str
		{
			std::string to_string() const
			{
				return std::to_string(return_flag) + "," + std::to_string(line_status) + "," + 
					std::to_string(sum_qty) + "," + std::to_string(sum_base_price) + "," + std::to_string(sum_disc_price) +
					"," + std::to_string(sum_charge) + "," + std::to_string(avg_qty / count_order) + "," + std::to_string(avg_price / count_order) + "," +
					std::to_string(avg_disc / count_order);
			}
			char return_flag;
			char line_status;
			float sum_qty;
			float sum_base_price;
			float sum_disc_price;
			float sum_charge;
			float avg_qty;
			float avg_price;
			float avg_disc;
			unsigned int count_order;
		}query_one_result;

		typedef struct query_three_result_str
		{
			query_three_result_str() {}
			query_three_result_str(uint32_t o_orderkey, uint32_t o_shippriority, Tpch::date o_orderdate, float revenue)
			{
				this->o_orderkey = o_orderkey;
				this->o_shippriority = o_shippriority;
				this->o_orderdate = o_orderdate;
				this->revenue = revenue;
			}
			float revenue;
			Tpch::date o_orderdate;
			uint32_t o_orderkey;
			uint32_t o_shippriority;
		}query_three_result;

		typedef struct query_three_step_one_str
		{
			query_three_step_one_str() {}
			query_three_step_one_str(Tpch::date order_date, uint32_t o_shippriority) : 
				o_orderdate(order_date), o_shippriority(o_shippriority) {}
			Tpch::date o_orderdate;
			uint32_t o_shippriority;
		}query_three_step_one;

		typedef struct query_three_predicate_str
		{
			query_three_predicate_str() {}
			query_three_predicate_str(char c_mktsegment[10], Tpch::date order_date)
			{
				memcpy(this->c_mktsegment, c_mktsegment, 10 * sizeof(char));
				this->order_date = order_date;
			}
			char c_mktsegment[10];
			Tpch::date order_date;
		}query_three_predicate;

		class QueryOneWorker
		{
		public:
			QueryOneWorker() {}
			QueryOneWorker(std::queue<Experiment::Tpch::lineitem>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~QueryOneWorker();
			void operate();
			void update(Tpch::lineitem& line_item);
			void finalize(std::vector<query_one_result>& buffer);
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, Tpch::query_one_result> result;
			std::queue<Tpch::lineitem>* input_queue;
		};

		class QueryOneOfflineAggregator
		{
		public:
			QueryOneOfflineAggregator();
			~QueryOneOfflineAggregator();
			void sort_final_result(const std::vector<query_one_result>& buffer, std::map<std::string, query_one_result>& result);
			void calculate_and_produce_final_result(const std::vector<query_one_result>& buffer, std::map<std::string, query_one_result>& result);
			void write_output_result(const std::map<std::string, query_one_result>& result, const std::string& output_file);
		};

		class QueryOnePartition
		{
		public:
			static void query_one_simulation(const std::vector<Experiment::Tpch::lineitem>& lines, const size_t task_num);
			static void thread_lineitem_partition(bool write, size_t task_num, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::lineitem>* input_buffer,
				std::vector<std::vector<Experiment::Tpch::lineitem>>* worker_input_buffer, float *imbalance, float* key_imbalance, double *total_duration);
			static void query_one_partitioner_simulation(const std::vector<Experiment::Tpch::lineitem>& lineitem_table, const std::vector<uint16_t> tasks,
				Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix);
		};

		/*class LineitemOrderWorker
		{
		public:
			LineitemOrderWorker(std::queue<Experiment::Tpch::lineitem>* li_queue, std::mutex* li_mu, std::condition_variable* li_cond, 
					std::queue<Experiment::Tpch::order>* o_queue, std::mutex* o_mu, std::condition_variable* o_cond);
			~LineitemOrderWorker();
			void operate();
			void update(Tpch::lineitem& line_item, bool partial_flag);
			void update(Tpch::order& o);
			void finalize(std::unordered_map<std::string, lineitem_order>& buffer);
			void partial_finalize(std::unordered_map<std::string, Tpch::lineitem>& li_buffer, std::unordered_map<uint32_t, Tpch::order>& o_buffer, 
				std::unordered_map<std::string, lineitem_order>& result_buffer);
		private:
			std::mutex* li_mu;
			std::mutex* o_mu;
			std::condition_variable* li_cond;
			std::condition_variable* o_cond;
			std::unordered_map<uint32_t, Tpch::order> order_index;
			std::unordered_map<std::string, lineitem> li_index;
			std::unordered_map<std::string, Tpch::lineitem_order> result;
			std::queue<Experiment::Tpch::lineitem>* li_queue;
			std::queue<Experiment::Tpch::order>* o_queue;
		};

		class LineitemOrderOfflineAggregator
		{
		public:
			LineitemOrderOfflineAggregator();
			~LineitemOrderOfflineAggregator();;
			void calculate_and_produce_final_result(std::unordered_map<std::string, Tpch::lineitem>& li_buffer, std::unordered_map<uint32_t, Tpch::order>& o_buffer,
				std::unordered_map<std::string, Tpch::lineitem_order>& result);
			void write_output_result(const std::unordered_map<std::string, Tpch::lineitem_order>& result, const std::string& output_file);
		};

		class LineitemOrderPartition
		{
		public:
			static void lineitem_order_join_simulation(const std::vector<Experiment::Tpch::lineitem>& li_table, const std::vector<Experiment::Tpch::order>& o_table,
				const size_t task_num);
			static void lineitem_order_join_partitioner_simulation(const std::vector<Experiment::Tpch::lineitem>& li_table, const std::vector<Experiment::Tpch::order>& o_table,
				const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name);
		};*/

		class QueryThreeJoinWorker
		{
		public:
			QueryThreeJoinWorker(const Experiment::Tpch::query_three_predicate& predicate);
			~QueryThreeJoinWorker();
			void step_one_update(const Experiment::Tpch::q3_customer& customer);
			void step_one_update(const Experiment::Tpch::order& order);
			void step_one_finalize(std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result_buffer);
			void step_one_partial_finalize(std::unordered_set<uint32_t>& c_index, std::unordered_map<uint32_t, Tpch::order>& o_index,
				std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result_buffer);
			void step_two_init(const std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result);
			void step_two_update(const Experiment::Tpch::lineitem& line_item);
			void finalize(std::unordered_map<std::string, Experiment::Tpch::query_three_result>& result_buffer);
			void partial_finalize(std::unordered_map<std::string, Experiment::Tpch::query_three_result>& result_buffer);
		private:
			Experiment::Tpch::query_three_predicate predicate;
			std::unordered_set<uint32_t> cu_index;
			std::unordered_map<uint32_t, Tpch::order> o_index;
			std::unordered_map<uint32_t, Tpch::query_three_step_one> step_one_result;
			std::unordered_map<std::string, Tpch::query_three_result> final_result;
		};

		class QueryThreeOfflineAggregator
		{
		public:
			void write_output_to_file(const std::unordered_map<std::string, query_three_result>& result, const std::string& output_file);
		};

		class QueryThreePartition
		{
		public:
			static void query_three_simulation(const std::vector<Experiment::Tpch::q3_customer>& c_table, const std::vector<Experiment::Tpch::lineitem>& li_table, 
				const std::vector<Experiment::Tpch::order>& o_table, const size_t task_num);
			static void thread_customer_partition(bool write, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::q3_customer>* c_table,
				std::vector<std::vector<Experiment::Tpch::q3_customer>>* c_worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration);
			static void thread_order_partition(bool write, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::order>* o_table,
				std::vector<std::vector<Experiment::Tpch::order>>* o_worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration);
			static void thread_li_partition(bool write, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::lineitem>* li_table,
				std::vector<std::vector<Experiment::Tpch::lineitem>>* li_worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration);
			static void query_three_partitioner_simulation(const std::vector<Experiment::Tpch::q3_customer>& c_table, const std::vector<Experiment::Tpch::lineitem>& li_table,
				const std::vector<Experiment::Tpch::order>& o_table, const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, 
				const std::string worker_output_file_name);
		};

	}
}
#endif // !TPCH_QUERY_LIB_H_
