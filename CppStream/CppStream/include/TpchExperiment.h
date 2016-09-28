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
		typedef struct date_str
		{
			date_str() {}
			date_str(const date_str& obj)
			{
				year = obj.year;
				month = obj.month;
				day = obj.day;
			}
			~date_str() {}
			date_str& operator= (const date_str& o)
			{
				if (this != &o)
				{
					year = o.year;
					month = o.month;
					day = o.day;
				}
				return *this;
			}
			uint16_t year;
			uint8_t month;
			uint8_t day;
		}date;

		typedef struct part_str
		{
			part_str() : p_name(), p_type(), p_comment() {}
			part_str(const part_str& o)
			{
				p_partkey = o.p_partkey;
				p_name = o.p_name;
				memcpy(p_mfgr, o.p_mfgr, 25 * sizeof(char));
				memcpy(p_brand, o.p_brand, 10 * sizeof(char));
				p_type = p_type;
				p_size = o.p_size;
				memcpy(p_container, o.p_container, 10 * sizeof(char));
				p_comment = o.p_comment;
			}
			~part_str() {}
			part_str& operator= (const part_str& o)
			{
				if (this != &o)
				{
					p_partkey = o.p_partkey;
					p_name = o.p_name;
					memcpy(p_mfgr, o.p_mfgr, 25 * sizeof(char));
					memcpy(p_brand, o.p_brand, 10 * sizeof(char));
					p_type = p_type;
					p_size = o.p_size;
					memcpy(p_container, o.p_container, 10 * sizeof(char));
					p_comment = o.p_comment;
				}
				return *this;
			}
			uint32_t p_partkey;
			std::string p_name;
			char p_mfgr[25]; // fixed text, size 25
			char p_brand[10]; // fixed text, size 10
			std::string p_type;
			uint32_t p_size;
			char p_container[10]; // fixed text, size 10 
			float p_retailprice; 
			std::string p_comment;
		}part;

		typedef struct supplier_str
		{
			supplier_str() : s_address(), s_comment() {}
			supplier_str(const supplier_str& o)
			{
				s_suppkey = o.s_suppkey;
				memcpy(s_name, o.s_name, 25 * sizeof(char));
				s_address = o.s_address;
				s_nationkey = o.s_nationkey;
				memcpy(s_phone, o.s_phone, 15 * sizeof(char));
				s_acctbal = o.s_acctbal;
				s_comment = o.s_comment;
			}
			~supplier_str() {}
			supplier_str& operator= (const supplier_str& o)
			{
				if (this != &o)
				{
					s_suppkey = o.s_suppkey;
					memcpy(s_name, o.s_name, 25 * sizeof(char));
					s_address = o.s_address;
					s_nationkey = o.s_nationkey;
					memcpy(s_phone, o.s_phone, 15 * sizeof(char));
					s_acctbal = o.s_acctbal;
					s_comment = o.s_comment;
				}
				return *this;
			}
			uint32_t s_suppkey;
			char s_name[25]; // fixed text, size 25
			std::string s_address;
			uint32_t s_nationkey;
			char s_phone[15]; // fixed text, size 15
			float s_acctbal;
			std::string s_comment;
		}supplier;

		typedef struct partsupp_str
		{
			partsupp_str() : ps_comment() {}
			partsupp_str(const partsupp_str& o)
			{
				ps_partkey = o.ps_partkey;
				ps_suppkey = o.ps_suppkey;
				ps_availqty = o.ps_availqty;
				ps_supplycost = o.ps_supplycost;
				ps_comment = o.ps_comment;
			}
			~partsupp_str() {}
			partsupp_str& operator= (const partsupp_str& o)
			{
				if (this != &o)
				{
					ps_partkey = o.ps_partkey;
					ps_suppkey = o.ps_suppkey;
					ps_availqty = o.ps_availqty;
					ps_supplycost = o.ps_supplycost;
					ps_comment = o.ps_comment;
				}
				return *this;
			}
			uint32_t ps_partkey;
			uint32_t ps_suppkey;
			int32_t ps_availqty;
			float ps_supplycost;
			std::string ps_comment;
		}partsupp;

		typedef struct customer_str
		{
			customer_str() : c_name(), c_address(), c_comment() {}
			customer_str(const customer_str& o)
			{
				c_custkey = o.c_custkey;
				c_name = o.c_name;
				c_address = o.c_address;
				c_nationkey = o.c_nationkey;
				memcpy(c_phone, o.c_phone, 15 * sizeof(char));
				c_acctbal = o.c_acctbal;
				memcpy(c_mktsegment, o.c_mktsegment, 10 * sizeof(char));
				c_comment = o.c_comment;
			}
			~customer_str() {}
			customer_str& operator= (const customer_str& o)
			{
				if (this != &o)
				{
					c_custkey = o.c_custkey;
					c_name = o.c_name;
					c_address = o.c_address;
					c_nationkey = o.c_nationkey;
					memcpy(c_phone, o.c_phone, 15 * sizeof(char));
					c_acctbal = o.c_acctbal;
					memcpy(c_mktsegment, o.c_mktsegment, 10 * sizeof(char));
					c_comment = o.c_comment;
				}
				return *this;
			}
			uint32_t c_custkey;
			std::string c_name;
			std::string c_address;
			uint32_t c_nationkey;
			char c_phone[15]; // fixed text, size 15
			float c_acctbal;
			char c_mktsegment[10]; // fixed text, size 10
			std::string c_comment;
		}customer;

		typedef struct order_str
		{
			order_str() : o_comment() {}
			order_str(const order_str& o)
			{
				o_orderkey = o.o_orderkey;
				o_custkey = o.o_custkey;
				o_orderstatus = o.o_orderstatus;
				o_totalprice = o.o_totalprice;
				o_orderdate = o.o_orderdate;
				memcpy(o_orderpriority, o.o_orderpriority, 15 * sizeof(char));
				memcpy(o_clerk, o.o_clerk, 15 * sizeof(char));
				o_shippriority = o.o_shippriority;
				o_comment = o.o_comment;
			}
			~order_str() {}
			order_str& operator= (const order_str& o)
			{
				if (this != &o)
				{
					o_orderkey = o.o_orderkey;
					o_custkey = o.o_custkey;
					o_orderstatus = o.o_orderstatus;
					o_totalprice = o.o_totalprice;
					o_orderdate = o.o_orderdate;
					memcpy(o_orderpriority, o.o_orderpriority, 15 * sizeof(char));
					memcpy(o_clerk, o.o_clerk, 15 * sizeof(char));
					o_shippriority = o.o_shippriority;
					o_comment = o.o_comment;
				}
				return *this;
			}
			uint32_t o_orderkey;
			uint32_t o_custkey;
			char o_orderstatus;
			float o_totalprice;
			Tpch::date o_orderdate;
			char o_orderpriority[15]; // fixed text, size 15
			char o_clerk[15]; // fixed text, size 15
			uint32_t o_shippriority;
			std::string o_comment;
		}order;

		typedef struct lineitem_str
		{
			lineitem_str() : l_comment() {}
			lineitem_str(const lineitem_str& o)
			{
				l_order_key = o.l_order_key;
				l_part_key = o.l_part_key;
				l_supp_key = o.l_supp_key;
				l_linenumber = o.l_linenumber;
				l_quantity = o.l_quantity;
				l_extendedprice = o.l_extendedprice;
				l_discount = o.l_discount;
				l_tax = o.l_tax;
				l_returnflag = o.l_returnflag;
				l_linestatus = o.l_linestatus;
				l_shipdate = o.l_shipdate;
				l_commitdate = o.l_commitdate;
				l_receiptdate = o.l_receiptdate;
				memcpy(l_shipinstruct, o.l_shipinstruct, 25 * sizeof(char));
				memcpy(l_shipmode, o.l_shipmode, 10 * sizeof(char));
				l_comment = o.l_comment;
			}
			~lineitem_str() {}
			lineitem_str& operator= (const lineitem_str& o)
			{
				if (this != &o)
				{
					l_order_key = o.l_order_key;
					l_part_key = o.l_part_key;
					l_supp_key = o.l_supp_key;
					l_linenumber = o.l_linenumber;
					l_quantity = o.l_quantity;
					l_extendedprice = o.l_extendedprice;
					l_discount = o.l_discount;
					l_tax = o.l_tax;
					l_returnflag = o.l_returnflag;
					l_linestatus = o.l_linestatus;
					l_shipdate = o.l_shipdate;
					l_commitdate = o.l_commitdate;
					l_receiptdate = o.l_receiptdate;
					memcpy(l_shipinstruct, o.l_shipinstruct, 25 * sizeof(char));
					memcpy(l_shipmode, o.l_shipmode, 10 * sizeof(char));
					l_comment = o.l_comment;
				}
				return *this;
			}
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
			char l_shipinstruct[25]; // fixed text size 25
			char l_shipmode[10]; // fixed text size 10
			std::string l_comment;
		}lineitem;

		typedef struct nation_str
		{
			nation_str() : n_comment() {}
			nation_str(const nation_str& o)
			{
				n_nationkey = o.n_nationkey;
				memcpy(n_name, o.n_name, 25 * sizeof(char));
				n_regionkey = o.n_regionkey;
				n_comment = o.n_comment;
			}
			~nation_str() {}
			nation_str& operator= (const nation_str& o)
			{
				if (this != &o)
				{
					n_nationkey = o.n_nationkey;
					memcpy(n_name, o.n_name, 25 * sizeof(char));
					n_regionkey = o.n_regionkey;
					n_comment = o.n_comment;
				}
				return *this;
			}
			uint32_t n_nationkey;
			char n_name[25]; // fixed size
			uint32_t n_regionkey;
			std::string n_comment;
		}nation;

		typedef struct region_str
		{
			region_str() : r_comment() {}
			region_str(const region_str& o)
			{
				r_regionkey = o.r_regionkey;
				memcpy(r_name, o.r_name, 25 * sizeof(char));
				r_comment = o.r_comment;
			}
			~region_str() {}
			region_str& operator= (const region_str& o)
			{
				if (this != &o)
				{
					r_regionkey = o.r_regionkey;
					memcpy(r_name, o.r_name, 25 * sizeof(char));
					r_comment = o.r_comment;
				}
				return *this;
			}
			uint32_t r_regionkey;
			char r_name[25]; // fixed size
			std::string r_comment;
		}region;

		class DataParser
		{
		public:
			static void parse_tpch_part(const std::string input_file_name, std::vector<Experiment::Tpch::part>& buffer);
			static void parse_tpch_supplier(const std::string input_file_name, std::vector<Experiment::Tpch::supplier>& buffer);
			static void parse_tpch_partsupp(const std::string input_file_name, std::vector<Experiment::Tpch::partsupp>& buffer);
			static void parse_tpch_customer(const std::string input_file_name, std::vector<Experiment::Tpch::customer>& buffer);
			static void parse_tpch_lineitem(const std::string input_file_name, std::vector<Experiment::Tpch::lineitem>& buffer);
			static void parse_tpch_order(const std::string input_file_name, std::vector<Experiment::Tpch::order>& buffer);
			static void parse_tpch_region(const std::string input_file_name, std::vector<Experiment::Tpch::region>& buffer);
			static void parse_tpch_nation(const std::string input_file_name, std::vector<Experiment::Tpch::nation>& buffer);
			static void parse_all_files_test(const std::string input_file_dir);
		private:
			static void parse_part(std::string part_info, Experiment::Tpch::part* part);
			static void parse_supplier(std::string supplier_info, Experiment::Tpch::supplier* supplier);
			static void parse_partsupp(std::string partsupp_info, Experiment::Tpch::partsupp* partsupp);
			static void parse_customer(std::string customer_info, Experiment::Tpch::customer* customer);
			static void parse_order(std::string order_info, Experiment::Tpch::order* order);
			static void parse_lineitem(std::string lineitem_info, Experiment::Tpch::lineitem* line_item);
			static void parse_region(std::string region_info, Experiment::Tpch::region* region);
			static void parse_nation(std::string nation_info, Experiment::Tpch::nation* nation);
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

void Experiment::Tpch::DataParser::parse_part(std::string part_info, Experiment::Tpch::part* part)
{
	std::stringstream str_stream(part_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	part->p_partkey = std::stoi(tokens[0]);
	part->p_name = tokens[1];
	memcpy(part->p_mfgr, tokens[2].c_str(), 25 * sizeof(char));
	memcpy(part->p_brand, tokens[3].c_str(), 10 * sizeof(char));
	part->p_type = tokens[4];
	part->p_size = std::stoi(tokens[5]);
	memcpy(part->p_container, tokens[6].c_str(), 10 * sizeof(char));
	part->p_retailprice = std::stof(tokens[7]);
	part->p_comment = tokens[8];
}

void Experiment::Tpch::DataParser::parse_supplier(std::string supplier_info, Experiment::Tpch::supplier* supplier)
{
	std::stringstream str_stream(supplier_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	supplier->s_suppkey = std::stoi(tokens[0]);
	memcpy(supplier->s_name, tokens[1].c_str(), 25 * sizeof(char));
	supplier->s_address = tokens[2];
	supplier->s_nationkey = std::stoi(tokens[3]);
	memcpy(supplier->s_phone, tokens[4].c_str(), 15 * sizeof(char));
	supplier->s_acctbal = std::stof(tokens[5]);
	supplier->s_comment = tokens[6];
}

void Experiment::Tpch::DataParser::parse_partsupp(std::string partsupp_info, Experiment::Tpch::partsupp* partsupp)
{
	std::stringstream str_stream(partsupp_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	partsupp->ps_partkey = std::stoi(tokens[0]);
	partsupp->ps_suppkey = std::stoi(tokens[1]);
	partsupp->ps_availqty = std::stoi(tokens[2]);
	partsupp->ps_supplycost = std::stof(tokens[3]);
	partsupp->ps_comment = tokens[4];
}

void Experiment::Tpch::DataParser::parse_customer(std::string customer_info, Experiment::Tpch::customer* customer)
{
	std::stringstream str_stream(customer_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	customer->c_custkey = std::stoi(tokens[0]);
	customer->c_name = tokens[1];
	customer->c_address = tokens[2];
	customer->c_nationkey = std::stoi(tokens[3]);
	memcpy(customer->c_phone, tokens[4].c_str(), 15 * sizeof(char));
	customer->c_acctbal = std::stof(tokens[5]);
	memcpy(customer->c_mktsegment, tokens[6].c_str(), 10 * sizeof(char));
	customer->c_comment = tokens[7];
}

void Experiment::Tpch::DataParser::parse_order(std::string order_info, Experiment::Tpch::order* order)
{
	std::stringstream str_stream(order_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	order->o_orderkey = std::stoi(tokens[0]);
	order->o_custkey = std::stoi(tokens[1]);
	order->o_orderstatus = tokens[2][0];
	order->o_totalprice = std::stof(tokens[3]);
	// order-date
	std::vector<std::string> order_date_tokens;
	std::stringstream str_stream_1(tokens[4]);
	while (getline(str_stream_1, token, '-'))
	{
		order_date_tokens.push_back(token);
	}
	order->o_orderdate.year = std::stoi(order_date_tokens[0]);
	order->o_orderdate.month = std::stoi(order_date_tokens[1]);
	order->o_orderdate.day = std::stoi(order_date_tokens[2]);
	memcpy(order->o_orderpriority, tokens[5].c_str(), 15 * sizeof(char));
	memcpy(order->o_clerk, tokens[6].c_str(), 15 * sizeof(char));
	order->o_shippriority = std::stoi(tokens[7]);
	order->o_comment = tokens[8];
}

void Experiment::Tpch::DataParser::parse_lineitem(std::string lineitem_info, Experiment::Tpch::lineitem* line_item)
{
	std::stringstream str_stream(lineitem_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	line_item->l_order_key = std::stoi(tokens[0]);
	line_item->l_part_key = std::stoi(tokens[1]);
	line_item->l_supp_key = std::stoi(tokens[2]);
	line_item->l_linenumber = std::stoi(tokens[3]);
	line_item->l_quantity = std::stof(tokens[4]);
	line_item->l_extendedprice = std::stof(tokens[5]);
	line_item->l_discount = std::stof(tokens[6]);
	line_item->l_tax = std::stof(tokens[7]);
	line_item->l_returnflag = tokens[8][0];
	line_item->l_linestatus = tokens[9][0];
	// ship-date
	std::vector<std::string> ship_date_tokens;
	std::stringstream str_stream_1(tokens[10]);
	while (getline(str_stream_1, token, '-'))
	{
		ship_date_tokens.push_back(token);
	}
	line_item->l_shipdate.year = std::stoi(ship_date_tokens[0]);
	line_item->l_shipdate.month = std::stoi(ship_date_tokens[1]);
	line_item->l_shipdate.day = std::stoi(ship_date_tokens[2]);
	// commit-date
	std::vector<std::string> commit_date_tokens;
	std::stringstream str_stream_2(tokens[11]);
	while (getline(str_stream_2, token, '-'))
	{
		commit_date_tokens.push_back(token);
	}
	line_item->l_commitdate.year = std::stoi(commit_date_tokens[0]);
	line_item->l_commitdate.month = std::stoi(commit_date_tokens[1]);
	line_item->l_commitdate.day = std::stoi(commit_date_tokens[2]);
	// receipt-date
	std::vector<std::string> receipt_date_tokens;
	std::stringstream str_stream_3(tokens[12]);
	while (getline(str_stream_3, token, '-'))
	{
		receipt_date_tokens.push_back(token);
	}
	line_item->l_receiptdate.year = std::stoi(receipt_date_tokens[0]);
	line_item->l_receiptdate.month = std::stoi(receipt_date_tokens[1]);
	line_item->l_receiptdate.day = std::stoi(receipt_date_tokens[2]);
	memcpy(line_item->l_shipinstruct, tokens[13].c_str(), 25 * sizeof(char));
	memcpy(line_item->l_shipmode, tokens[14].c_str(), 10 * sizeof(char));
	line_item->l_comment = tokens[15];
}

void Experiment::Tpch::DataParser::parse_region(std::string region_info, Experiment::Tpch::region * region)
{
	std::stringstream str_stream(region_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	region->r_regionkey = std::stoi(tokens[0]);
	memcpy(region->r_name, tokens[1].c_str(), 25 * sizeof(char));
	region->r_comment = tokens[2];
}

void Experiment::Tpch::DataParser::parse_nation(std::string nation_info, Experiment::Tpch::nation * nation)
{
	std::stringstream str_stream(nation_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, '|'))
	{
		tokens.push_back(token);
	}
	nation->n_nationkey = std::stoi(tokens[0]);
	memcpy(nation->n_name, tokens[1].c_str(), 25 * sizeof(char));
	nation->n_regionkey = std::stoi(tokens[2]);
	nation->n_comment = tokens[3];
}

inline void Experiment::Tpch::DataParser::parse_tpch_part(const std::string input_file_name, std::vector<Experiment::Tpch::part>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::part part;
		Tpch::DataParser::parse_part(line, &part);
		buffer.push_back(part);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_supplier(const std::string input_file_name, std::vector<Experiment::Tpch::supplier>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::supplier supplier;
		Tpch::DataParser::parse_supplier(line, &supplier);
		buffer.push_back(supplier);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_partsupp(const std::string input_file_name, std::vector<Experiment::Tpch::partsupp>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::partsupp partsupp;
		Tpch::DataParser::parse_partsupp(line, &partsupp);
		buffer.push_back(partsupp);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_customer(const std::string input_file_name, std::vector<Experiment::Tpch::customer>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::customer customer;
		Tpch::DataParser::parse_customer(line, &customer);
		buffer.push_back(customer);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_lineitem(const std::string input_file_name, std::vector<Experiment::Tpch::lineitem>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::lineitem line_item;
		Tpch::DataParser::parse_lineitem(line, &line_item);
		buffer.push_back(line_item);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_order(const std::string input_file_name, std::vector<Experiment::Tpch::order>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::order order;
		Tpch::DataParser::parse_order(line, &order);
		buffer.push_back(order);
	}
	file.close();
	line.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_region(const std::string input_file_name, std::vector<Experiment::Tpch::region>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::region region;
		Tpch::DataParser::parse_region(line, &region);
		buffer.push_back(region);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_tpch_nation(const std::string input_file_name, std::vector<Experiment::Tpch::nation>& buffer)
{
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		Tpch::nation nation;
		Tpch::DataParser::parse_nation(line, &nation);
		buffer.push_back(nation);
	}
	file.close();
	buffer.shrink_to_fit();
}

inline void Experiment::Tpch::DataParser::parse_all_files_test(const std::string input_file_dir)
{
	std::string nation_file = input_file_dir + "\\nation.tbl";
	std::string region_file = input_file_dir + "\\region.tbl";
	std::string part_file = input_file_dir + "\\part.tbl";
	std::string supplier_file = input_file_dir + "\\supplier.tbl";
	std::string partsupp_file = input_file_dir + "\\partsupp.tbl";
	std::string customer_file = input_file_dir + "\\customer.tbl";
	std::string orders_file = input_file_dir + "\\orders.tbl";
	std::vector<Experiment::Tpch::nation> nation_table;
	std::vector<Experiment::Tpch::region> region_table;
	std::vector<Experiment::Tpch::part> part_table;
	std::vector<Experiment::Tpch::supplier> supplier_table;
	std::vector<Experiment::Tpch::partsupp> partsupp_table;
	std::vector<Experiment::Tpch::customer> customer_table;
	std::vector<Experiment::Tpch::order> orders_table;
	parse_tpch_nation(nation_file, nation_table);
	parse_tpch_region(region_file, region_table);
	parse_tpch_part(part_file, part_table);
	parse_tpch_supplier(supplier_file, supplier_table);
	parse_tpch_partsupp(partsupp_file, partsupp_table);
	parse_tpch_customer(customer_file, customer_table);
	parse_tpch_order(orders_file, orders_table);
	std::cout << "|NATION| = " << nation_table.size() << ", |REGION| = " << region_table.size() <<
		", |PART| = " << part_table.size() << ", |SUPPLIER| = " << supplier_table.size() <<
		", |PARTSUPP| = " << partsupp_table.size() << ", |CUSTOMER| = " << customer_table.size() <<
		", |ORDERS| = " << orders_table.size() << "\n";
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
	CagPartitionLib::CagNaivePartitioner cag_naive(tasks, policy);
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
	CagPartitionLib::CagNaivePartitioner lag_naive(tasks, lag_policy);
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
	CagPartitionLib::CagPcPartitioner cag_pc(tasks, policy);
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
	CagPartitionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
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
	CagPartitionLib::CagHllPartitioner cag_hll(tasks, policy, 5);
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
	CagPartitionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
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
	CagPartitionLib::CagNaivePartitioner cag_naive(tasks, cag_policy);
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
	CagPartitionLib::CagNaivePartitioner cag_naive(tasks, lag_policy);
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
	CagPartitionLib::CagPcPartitioner cag_pc(tasks, cag_policy);
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
	CagPartitionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
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
	CagPartitionLib::CagHllPartitioner cag_hll(tasks, cag_policy, 5);
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
	CagPartitionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
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
