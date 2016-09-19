#pragma once
#include <fstream>
#include <vector>
#include <cstring>

#include "partition_policy.h"
#include "pkg_partitioner.h"
#include "cag_partitioner.h"

#ifndef EXPERIMENT_TPCH_H_
#define EXPERIMENT_TPCH_H_

namespace Experiment
{
	namespace Tpch
	{
		typedef struct
		{
			uint16_t year;
			uint8_t month;
			uint8_t day;
		}date;

		typedef struct
		{
			uint32_t p_partkey;
			char p_name[55];
			char p_mfgr[25];
			char p_brand[10];
			char p_type[25];
			uint32_t p_size;
			char p_container[10];
			float p_retailprice;
			char p_comment[23];
		}part;

		typedef struct
		{
			uint32_t s_suppkey;
			char s_name[25];
			char s_address[40];
			uint32_t s_nationkey;
			char s_phone[15];
			float s_acctbal;
			char s_comment[101];
		}supplier;

		typedef struct
		{
			uint32_t o_orderkey;
			uint32_t o_custkey;
			char o_orderstatus;
			float o_totalprice;
			Tpch::date o_orderdate;
			char o_orderpriority[16];
			char o_clerk[16];
			uint32_t o_shippriority;
			char o_comment[79];
		}order;

		typedef struct
		{
			uint32_t l_order_key;
			uint32_t l_part_key;
			uint32_t l_supp_key;
			int32_t l_linenumber;
			float l_quantity;
			float l_extendedprice;
			float l_discount;
			float l_tax;
			char l_returnflag;
			char l_linestatus;
			Tpch::date l_shipdate;
			Tpch::date l_commitdate;
			Tpch::date l_receiptdate;
			char l_shipinstruct[25];
			char l_shipmode[10];
			char l_comment[44];
		}lineitem;

		class DataParser
		{
		public:
			static void parse_part(std::string part_info, Tpch::part& part);
			static void parse_supplier(std::string supplier_info, Tpch::supplier& supplier);
			static void parse_order(std::string order_info, Tpch::order& order);
			static void parse_lineitem(std::string lineitem_info, Tpch::lineitem& line_item);
		};

		typedef struct
		{
			float sum_qty;
			float sum_base_price;
			float sum_disc_price;
			float sum_charge;
			float avg_qty;
			float avg_price;
			float avg_disc;
			uint32_t count_order;
		}query_one_result;

		class QueryOne
		{
		public:
			QueryOne(std::queue<Experiment::Tpch::lineitem>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~QueryOne();
			void operate();
			void update(Tpch::lineitem& line_item);
			void finalize();
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, Tpch::query_one_result> result;
			std::queue<Tpch::lineitem>* input_queue;
		};

		class QueryOnePartition
		{
		public:
			static std::vector<Experiment::Tpch::lineitem> parse_tpch_lineitem(std::string input_file_name);
			static std::vector<Experiment::Tpch::order> parse_tpch_order(std::string input_file_name);
			static void tpch_q1_performance(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_pkg_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_cag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_lag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_cag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_lag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_cag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
			static void tpch_q1_lag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table);
		private:
			static void tpch_q1_worker(Tpch::QueryOne* query_one)
			{
				query_one->operate();
			}
		};
	}
}

void Experiment::Tpch::DataParser::parse_part(std::string part_info, Tpch::part & part)
{
	std::stringstream str_stream(part_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	part.p_partkey = std::stoi(tokens[0]);
	//strcpy_s(part.p_name, sizeof part.p_name, tokens[1].c_str());
	strcpy(part.p_name, tokens[1].c_str());
	//strcpy_s(part.p_mfgr, sizeof part.p_mfgr, tokens[2].c_str());
	strcpy(part.p_mfgr, tokens[2].c_str());
	//strcpy_s(part.p_brand, sizeof part.p_brand, tokens[3].c_str());
	strcpy(part.p_brand, tokens[3].c_str());
	//strcpy_s(part.p_type, sizeof part.p_type, tokens[4].c_str());
	strcpy(part.p_type, tokens[4].c_str());
	part.p_size = std::stoi(tokens[5]);
	//strcpy_s(part.p_container, sizeof part.p_container, tokens[6].c_str());
	strcpy(part.p_container, tokens[6].c_str());
	part.p_retailprice = std::stof(tokens[7]);
	//strcpy_s(part.p_comment, sizeof part.p_comment, tokens[8].c_str());
	strcpy(part.p_comment, tokens[8].c_str());
}

void Experiment::Tpch::DataParser::parse_supplier(std::string supplier_info, Experiment::Tpch::supplier & supplier)
{
	std::stringstream str_stream(supplier_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	supplier.s_suppkey = std::stoi(tokens[0]);
	//strcpy_s(supplier.s_name, sizeof supplier.s_name, tokens[1].c_str());
	strcpy(supplier.s_name, tokens[1].c_str());
	//strcpy_s(supplier.s_address, sizeof supplier.s_address, tokens[2].c_str());
	strcpy(supplier.s_address, tokens[2].c_str());
	supplier.s_nationkey = std::stoi(tokens[3]);
	//strcpy_s(supplier.s_phone, sizeof supplier.s_phone, tokens[4].c_str());
	strcpy(supplier.s_phone, tokens[4].c_str());
	supplier.s_acctbal = std::stof(tokens[5]);
	//strcpy_s(supplier.s_comment, sizeof supplier.s_comment, tokens[6].c_str());
	strcpy(supplier.s_comment, tokens[6].c_str());
}

void Experiment::Tpch::DataParser::parse_order(std::string order_info, Experiment::Tpch::order & order)
{
	std::stringstream str_stream(order_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	order.o_orderkey = std::stoi(tokens[0]);
	order.o_custkey = std::stoi(tokens[1]);
	order.o_orderstatus = tokens[2][0];
	order.o_totalprice = std::stof(tokens[3]);
	// order-date
	std::vector<std::string> order_date_tokens;
	std::stringstream str_stream_1(tokens[4]);
	while (getline(str_stream_1, token, '-'))
	{
		order_date_tokens.push_back(token);
	}
	order.o_orderdate.year = std::stoi(order_date_tokens[0]);
	order.o_orderdate.month = std::stoi(order_date_tokens[1]);
	order.o_orderdate.day = std::stoi(order_date_tokens[2]);
	//strcpy_s(order.o_orderpriority, sizeof order.o_orderpriority, tokens[5].c_str());
	strcpy(order.o_orderpriority, tokens[5].c_str());
	// clerk
	//strcpy_s(order.o_clerk, sizeof order.o_clerk, tokens[6].c_str());
	strcpy(order.o_clerk, tokens[6].c_str());
	order.o_shippriority = std::stoi(tokens[7]);
	//strcpy_s(order.o_comment, sizeof order.o_comment, tokens[8].c_str());
	strcpy(order.o_comment, tokens[8].c_str());
}

void Experiment::Tpch::DataParser::parse_lineitem(std::string lineitem_info, Experiment::Tpch::lineitem& line_item)
{
	std::stringstream str_stream(lineitem_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	line_item.l_order_key = std::stoi(tokens[0]);
	line_item.l_part_key = std::stoi(tokens[1]);
	line_item.l_supp_key = std::stoi(tokens[2]);
	line_item.l_linenumber = std::stoi(tokens[3]);
	line_item.l_quantity = std::stof(tokens[4]);
	line_item.l_extendedprice = std::stof(tokens[5]);
	line_item.l_discount = std::stof(tokens[6]);
	line_item.l_tax = std::stof(tokens[7]);
	line_item.l_returnflag = tokens[8][0];
	line_item.l_linestatus = tokens[9][0];
	// ship-date
	std::vector<std::string> ship_date_tokens;
	std::stringstream str_stream_1(tokens[10]);
	while (getline(str_stream_1, token, '-'))
	{
		ship_date_tokens.push_back(token);
	}
	line_item.l_shipdate.year = std::stoi(ship_date_tokens[0]);
	line_item.l_shipdate.month = std::stoi(ship_date_tokens[1]);
	line_item.l_shipdate.day = std::stoi(ship_date_tokens[2]);
	// commit-date
	std::vector<std::string> commit_date_tokens;
	std::stringstream str_stream_2(tokens[11]);
	while (getline(str_stream_2, token, '-'))
	{
		commit_date_tokens.push_back(token);
	}
	line_item.l_commitdate.year = std::stoi(commit_date_tokens[0]);
	line_item.l_commitdate.month = std::stoi(commit_date_tokens[1]);
	line_item.l_commitdate.day = std::stoi(commit_date_tokens[2]);
	// receipt-date
	std::vector<std::string> receipt_date_tokens;
	std::stringstream str_stream_3(tokens[12]);
	while (getline(str_stream_3, token, '-'))
	{
		receipt_date_tokens.push_back(token);
	}
	line_item.l_receiptdate.year = std::stoi(receipt_date_tokens[0]);
	line_item.l_receiptdate.month = std::stoi(receipt_date_tokens[1]);
	line_item.l_receiptdate.day = std::stoi(receipt_date_tokens[2]);
	//strcpy_s(line_item.l_shipinstruct, sizeof line_item.l_shipinstruct, tokens[13].c_str());
	strcpy(line_item.l_shipinstruct, tokens[13].c_str());
	//strcpy_s(line_item.l_shipmode, sizeof line_item.l_shipmode, tokens[14].c_str());
	strcpy(line_item.l_shipmode, tokens[14].c_str());
	//strcpy_s(line_item.l_comment, sizeof line_item.l_comment, tokens[15].c_str());
	strcpy(line_item.l_comment, tokens[15].c_str());
}

Experiment::Tpch::QueryOne::QueryOne(std::queue<Experiment::Tpch::lineitem>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

inline Experiment::Tpch::QueryOne::~QueryOne()
{
	result.clear();
}

inline void Experiment::Tpch::QueryOne::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Tpch::lineitem line_item = input_queue->back();
		input_queue->pop();
		// process
		if (line_item.l_linenumber >= 0)
		{
			update(line_item);
		}
		else
		{
			finalize();
			locker.unlock();
			cond->notify_all();
			break;
		}
		locker.unlock();
		cond->notify_all();
	}
}

void Experiment::Tpch::QueryOne::update(Experiment::Tpch::lineitem& line_item)
{
	std::string key = std::to_string(line_item.l_returnflag) + std::to_string(line_item.l_linestatus);
	std::unordered_map<std::string, Tpch::query_one_result>::iterator it = result.find(key);
	if (it != result.end())
	{
		it->second.sum_qty += line_item.l_quantity;
		it->second.sum_base_price += line_item.l_extendedprice;
		it->second.sum_disc_price += (line_item.l_extendedprice * (1 - line_item.l_discount));
		it->second.sum_charge += (line_item.l_extendedprice * (1 - line_item.l_discount) * (1 + line_item.l_tax));
		it->second.avg_qty += line_item.l_quantity;
		it->second.avg_price += line_item.l_extendedprice;
		it->second.avg_disc += line_item.l_discount;
		it->second.count_order += 1;
	}
	else
	{
		Tpch::query_one_result tmp;
		tmp.sum_qty = line_item.l_quantity;
		tmp.sum_base_price = line_item.l_extendedprice;
		tmp.sum_disc_price = line_item.l_extendedprice * (1 - line_item.l_discount);
		tmp.sum_charge = line_item.l_extendedprice * (1 - line_item.l_discount) * (1 + line_item.l_tax);
		tmp.avg_qty = line_item.l_quantity;
		tmp.avg_price = line_item.l_extendedprice;
		tmp.avg_disc = line_item.l_discount;
		tmp.count_order = 1;
		result.insert(std::make_pair(key, tmp));
	}
}

inline void Experiment::Tpch::QueryOne::finalize()
{
	std::cout << "number of groups: " << result.size() << ".\n";
	for (std::unordered_map<std::string, Tpch::query_one_result>::iterator it = result.begin(); it != result.end(); ++it)
	{
		it->second.avg_qty = it->second.avg_qty / it->second.count_order;
		it->second.avg_price = it->second.avg_price / it->second.count_order;
		it->second.avg_disc = it->second.avg_disc / it->second.count_order;
	}
}

std::vector<Experiment::Tpch::lineitem> Experiment::Tpch::QueryOnePartition::parse_tpch_lineitem(std::string input_file_name)
{
	std::vector<Tpch::lineitem> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	std::chrono::high_resolution_clock::time_point scan_start = std::chrono::high_resolution_clock::now();
	while (getline(file, line))
	{
		Tpch::lineitem line_item;
		Tpch::DataParser::parse_lineitem(line, line_item);
		lines.push_back(line_item);
	}
	std::chrono::high_resolution_clock::time_point scan_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	file.close();
	lines.shrink_to_fit();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";
	return lines;
}

std::vector<Experiment::Tpch::order> Experiment::Tpch::QueryOnePartition::parse_tpch_order(std::string input_file_name)
{
	std::vector<Tpch::order> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	std::chrono::high_resolution_clock::time_point scan_start = std::chrono::high_resolution_clock::now();
	while (getline(file, line))
	{
		Tpch::order order;
		Tpch::DataParser::parse_order(line, order);
		lines.push_back(order);
	}
	file.close();
	std::chrono::high_resolution_clock::time_point scan_end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	file.close();
	line.shrink_to_fit();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";
	return lines;
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_performance(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	std::cout << "------- TPCH Q-1 partition performance --------\n";
	// FLD
	HashFieldPartitioner fld(tasks);
	std::chrono::system_clock::time_point fld_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		fld.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point fld_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> fld_partition_time = fld_end - fld_start;
	std::cout << "Time partition using FLD: " << fld_partition_time.count() << " (msec).\n";
	// PKG
	PkgPartitioner pkg(tasks);
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		pkg.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Time partition using PKG: " << pkg_partition_time.count() << " (msec).\n";
	// CAG - naive
	CardinalityAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, policy);
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		cag_naive.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	std::cout << "Time partition using CAG(naive): " << cag_naive_partition_time.count() << " (msec).\n";
	// LAG - naive
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner lag_naive(tasks, lag_policy);
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		lag_naive.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	std::cout << "Time partition using LAG(naive): " << lag_naive_partition_time.count() << " (msec).\n";
	// CAG - pc
	CagPartionLib::CagPcPartitioner cag_pc(tasks, policy);
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		cag_pc.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	std::cout << "Time partition using CAG(pc): " << cag_pc_partition_time.count() << " (msec).\n";
	// LAG - pc
	CagPartionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		lag_pc.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	std::cout << "Time partition using LAG(pc): " << lag_pc_partition_time.count() << " (msec).\n";
	// CAG - hll
	CagPartionLib::CagHllPartitioner cag_hll(tasks, policy, 5);
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		cag_hll.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	std::cout << "Time partition using CAG(hll): " << cag_hll_partition_time.count() << " (msec).\n";
	// LAG - hll
	CagPartionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		lag_hll.partition_next(key, key.length());
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	std::cout << "Time partition using LAG(hll): " << lag_hll_partition_time.count() << " (msec).\n";
	std::cout << "------ END -----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_pkg_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::Tpch::lineitem>();
		query_workers[i] = new Experiment::Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	PkgPartitioner pkg(tasks);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = pkg.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Partioner thread (PKG) total partition time: " <<
		pkg_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_cag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, cag_policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_naive.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	std::cout << "Partioner thread (CAG-naive) total partition time: " <<
		cag_naive_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_lag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, lag_policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_naive.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	std::cout << "Partioner thread (LAG-naive) total partition time: " <<
		lag_naive_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_cag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagPcPartitioner cag_pc(tasks, cag_policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_pc.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	std::cout << "Partioner thread (CAG-PC) total partition time: " <<
		cag_pc_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_lag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_pc.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	std::cout << "Partioner thread (LAG-PC) total partition time: " <<
		lag_pc_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_cag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	CardinalityAwarePolicy cag_policy;
	CagPartionLib::CagHllPartitioner cag_hll(tasks, cag_policy, 5);
	std::cout << "Partitioner thread INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = cag_hll.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	std::cout << "Partioner thread (CAG-HLL) total partition time: " <<
		cag_hll_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::Tpch::QueryOnePartition::tpch_q1_lag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::Tpch::lineitem>& lineitem_table)
{
	// initialize shared memory
	std::queue<Tpch::lineitem>** queues = new std::queue<Tpch::lineitem>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Tpch::QueryOne** query_workers = new Tpch::QueryOne*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Tpch::lineitem>();
		query_workers[i] = new Tpch::QueryOne(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(tpch_q1_worker, query_workers[i]);
	}
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
	std::cout << "Partitioner INITIATES partitioning.\n";

	// start partitioning
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Tpch::lineitem>::const_iterator it = lineitem_table.begin(); it != lineitem_table.end(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "." + std::to_string(it->l_linestatus);
		short task = lag_hll.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}

	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Tpch::lineitem final_lineitem;
		final_lineitem.l_linenumber = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_lineitem);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	std::cout << "Partioner (LAG-HLL) total partition time: " <<
		lag_hll_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}
#endif // !EXPERIMENT_TPCH_H_
