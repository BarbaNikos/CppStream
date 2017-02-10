#include <iostream>
#include <ctime>
#include <fstream>
#include <string>
#include <chrono>
#include <climits>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <thread>
#include <future>
#include <fstream>
#include <functional>
#include <numeric>
#include <algorithm>
#include <set>

#ifndef TPCH_QUERY_LIB_H_
#include "../include/tpch_query_lib.h"
#endif // !TPCH_QUERY_LIB_H_

//#ifndef TPCH_NAIVE_SHED_EXPERIMENT_H_
//#include "../include/naive_shed_experiment.h"
//#endif // !TPCH_NAIVE_SHED_EXPERIMENT_H_


int main(int argc, char** argv)
{
	if (argc < 4)
	{
		std::cout << "usage: <customer_file.tbl> <lineitem_file.tbl> <orders_file.tbl>\n";
		exit(1);
	}
	std::string customer_file = argv[1];
	std::string lineitem_file = argv[2];
	std::string orders_file = argv[3];
	/*
	* TPC-H query
	*/
	std::vector<Experiment::Tpch::q3_customer> customer_table;
	std::vector<Experiment::Tpch::lineitem> lineitem_table;
	std::vector<Experiment::Tpch::order> order_table;

	Experiment::Tpch::DataParser::parse_tpch_lineitem(lineitem_file, lineitem_table);
	//Experiment::Tpch::ShedRouteLab::correct_result(lineitem_table, 8);
	/*Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 8);
	Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 16);
	Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 32);*/

	Experiment::Tpch::DataParser::parse_tpch_q3_customer(customer_file, customer_table);
	Experiment::Tpch::DataParser::parse_tpch_order(orders_file, order_table);
	Experiment::Tpch::QueryThreePartition::query_three_simulation(customer_table, lineitem_table, order_table, 8);
	Experiment::Tpch::QueryThreePartition::query_three_simulation(customer_table, lineitem_table, order_table, 16);
	Experiment::Tpch::QueryThreePartition::query_three_simulation(customer_table, lineitem_table, order_table, 32);
	customer_table.clear();
	lineitem_table.clear();
	order_table.clear();
#ifdef _WIN32
	std::cout << "Press ENTER to Continue";
	std::cin.ignore();
	return 0;
#else // _WIN32
	return 0;
#endif
}