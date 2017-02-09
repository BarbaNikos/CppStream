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
#include <iomanip>

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

#ifndef TPCH_KEY_EXTRACTOR_H_
#include "../include/tpch_key_extractor.h"
#endif //!TPCH_KEY_EXTRACTOR_H_

#ifndef NAIVE_SHED_FLD_PARTITIONER_H_
#include "naive_shed_fld.h"
#endif // !NAIVE_SHED_FLD_PARTITIONER_H_

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
			void update(const lineitem& line_item);
			void finalize(std::vector<query_one_result>& buffer) const;
		private:
			std::unordered_map<std::string, query_one_result> result;
		};

		class QueryOneOfflineAggregator
		{
		public:
			static void order_result(const std::vector<query_one_result>& buffer, std::map<std::string, query_one_result>& ordered_result);
			static void order_result(const std::unordered_map<std::string, query_one_result>& buffer, std::map<std::string, query_one_result>& ordered_result);
			static void aggregate_final_result(const std::vector<query_one_result>& buffer, std::unordered_map<std::string, query_one_result>& result);
			static void write_output_result(const std::map<std::string, query_one_result>& result, const std::string& output_file);
		};

		class QueryOnePartition
		{
		public:
			static void query_one_simulation(const std::vector<lineitem>& lines, const size_t task_num);
			static void lineitem_partition(size_t task_num, Partitioner& partitioner, const std::vector<lineitem>& input_buffer,
				std::vector<std::vector<lineitem>>& worker_input_buffer, float *imbalance, float* key_imbalance);
			static void thread_worker_operate(const bool write, const std::vector<lineitem>* input_buffer, 
				std::vector<query_one_result>* result_buffer, double* operate_duration);
			static void thread_aggregate(const bool write, const std::string partitioner_name, const std::vector<query_one_result>* input_buffer,
				std::map<std::string, query_one_result>* result, const std::string worker_output_file_name, double* aggregate_duration, double* order_duration, double* io_duration);
			static void query_one_partitioner_simulation(const std::vector<lineitem>& lineitem_table, const std::vector<uint16_t> tasks,
				Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix);
		};

		class QueryThreeWorker
		{
		public:
			QueryThreeWorker(const query_three_predicate& predicate);
			void step_one_update(const q3_customer& customer, std::string partitioner_name);
			void step_one_update(const order& order, std::string partitioner_name);
			void step_one_finalize(std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer);
			void step_one_partial_finalize(std::unordered_set<uint32_t>& c_index, std::unordered_map<uint32_t, order>& o_index,
				std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer) const;
			void step_two_init(const std::unordered_map<uint32_t, query_three_step_one>& step_one_result);
			void step_two_update(const lineitem& line_item);
			void finalize(std::vector<std::pair<std::string, query_three_result>>& result_buffer) const;
		private:
			query_three_predicate predicate;
			std::unordered_set<uint32_t> cu_index;
			std::unordered_map<uint32_t, order> o_index;
			std::unordered_map<uint32_t, query_three_step_one> step_one_result;
			std::unordered_map<std::string, query_three_result> final_result;
		};

		class QueryThreeOfflineAggregator
		{
		public:
			static void step_one_transfer(const std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer, 
				std::unordered_map<uint32_t, query_three_step_one>& result_buffer);
			static void step_one_materialize(const std::unordered_set<uint32_t>& c_index, const std::unordered_map<uint32_t, order>& o_index,
				std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer, std::unordered_map<uint32_t, query_three_step_one>& result_buffer);
			static void step_two_order(const std::vector<std::pair<std::string, query_three_result>>& buffer, 
				std::vector<std::pair<std::string, query_three_result>>& final_result);
			static void step_two_order(const std::unordered_map<std::string, query_three_result>& buffer, std::vector<std::pair<std::string, query_three_result>>& result);
			static void step_two_materialize(const std::vector<std::pair<std::string, query_three_result>>& buffer,
				std::unordered_map<std::string, query_three_result>& result);
			static void write_output_to_file(const std::vector<std::pair<std::string, query_three_result>>& final_result, const std::string& output_file);
		};

		class QueryThreePartition
		{
		public:
			static void query_three_simulation(const std::vector<Experiment::Tpch::q3_customer>& c_table, const std::vector<lineitem>& li_table, 
				const std::vector<order>& o_table, const size_t task_num);
			static void customer_partition(Partitioner& partitioner, const std::vector<q3_customer>& c_table,
				std::vector<std::vector<q3_customer>>& c_worker_input_buffer, float* imbalance, float* key_imbalance);
			static void order_partition(Partitioner& partitioner, const std::vector<order>& o_table,
				std::vector<std::vector<order>>& o_worker_input_buffer, float* imbalance, float* key_imbalance);
			static void lineitem_partition(Partitioner& partitioner, const std::vector<lineitem>& li_table,
				std::vector<std::vector<lineitem>>& li_worker_input_buffer, float* imbalance, float* key_imbalance);
			static void query_three_partitioner_simulation(const std::vector<q3_customer>& c_table, const std::vector<lineitem>& li_table,
				const std::vector<order>& o_table, const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, 
				const std::string worker_output_file_name);
		};

	}
}
#endif // !TPCH_QUERY_LIB_H_
