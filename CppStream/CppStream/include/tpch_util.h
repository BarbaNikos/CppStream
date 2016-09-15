#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <ctime>
#include <cmath>
#include <sstream>
#include <locale>
#include <iomanip>
#include <chrono>

#ifndef PARTITION_TPCH_UTIL_H_
#define PARTITION_TPCH_UTIL_H_
/*
	Query 1: (lineitem)
			lineitem.l_returnflag, lineitem.l_linestatus
	Query 2: (part, supplier, partsupp, nation, region)
			part.p_partkey, partsupp.ps_partkey, supplier.s_suppkey, 
			nation.n_nationkey, region.r_regionkey
	Query 3: (customer, orders, lineitem)
			l_orderkey, o_orderdate, o_shippriority, c_custkey, o_orderkey
	Query 4: (orders, lineitem)
			o_orderpriority, o_orderkey, l_orderkey
	Query 5: (customer, orders, lineitem, supplier, nation, region)
			c_custkey, o_custkey, l_orderkey, o_orderkey, l_suppkey, s_suppkey, 
			c_nationkey, s_nationkey, n_nationkey, n_regionkey, r_regionkey
	Query 6: (lineitem)
			
	Query 7: (supplier, lineitem, orders, customer, nation)
			supp_nation, cust_nation, l_year
*/
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
}

void Tpch::DataParser::parse_part(std::string part_info, Tpch::part & part)
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

void Tpch::DataParser::parse_supplier(std::string supplier_info, Tpch::supplier & supplier)
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

void Tpch::DataParser::parse_order(std::string order_info, Tpch::order & order)
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

void Tpch::DataParser::parse_lineitem(std::string lineitem_info, lineitem& line_item)
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
#endif // !PARTITION_TPCH_UTIL_H_
