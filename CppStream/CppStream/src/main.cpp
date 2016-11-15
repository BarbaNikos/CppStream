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

#ifndef C_HLL_H_
#include "../include/c_hll.h"
#endif // !C_HLL_H_

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // !DEBS_QUERY_LIB_H_

#ifndef EXPERIMENT_LOGNORMAL_SIMULATION_H_
#include "../include/lognormal_experiment.h"
#endif // !EXPERIMENT_LOGNORMAL_SIMULATION_H_

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

#ifndef TPCH_QUERY_LIB_H_
#include "../include/tpch_query_lib.h"
#endif // !TPCH_QUERY_LIB_H_


void log_normal_simulation(std::string input_file);

int main(int argc, char** argv)
{
	if (argc < 7)
	{
		std::cout << "usage: <customer_file.tbl> <lineitem_file.tbl> <orders_file.tbl> <debs_ride_q1.csv> <debs_ride_q2.csv> <google_task_event_dir>\n";
		exit(1);
	}
	std::string customer_file = argv[1];
	std::string lineitem_file = argv[2];
	std::string orders_file = argv[3];
	std::string ride_q1_file = argv[4];
	std::string ride_q2_file = argv[5];
	std::string google_task_event_dir = argv[6];
	/*
	 * TPC-H query
	 */
	 std::vector<Experiment::Tpch::q3_customer> customer_table;
	 std::vector<Experiment::Tpch::lineitem> lineitem_table;
	 std::vector<Experiment::Tpch::order> order_table;

	 Experiment::Tpch::DataParser::parse_tpch_lineitem(lineitem_file, lineitem_table);
	 Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 8);
	 Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 16);
	 Experiment::Tpch::QueryOnePartition::query_one_simulation(lineitem_table, 32);

	 Experiment::Tpch::DataParser::parse_tpch_q3_customer(customer_file, customer_table);
	 Experiment::Tpch::DataParser::parse_tpch_order(orders_file, order_table);
	 Experiment::Tpch::QueryThreePartition::query_three_simulation(customer_table, lineitem_table, order_table, 8);
	 Experiment::Tpch::QueryThreePartition::query_three_simulation(customer_table, lineitem_table, order_table, 16);
	 Experiment::Tpch::QueryThreePartition::query_three_simulation(customer_table, lineitem_table, order_table, 32);
	 customer_table.clear();
	 lineitem_table.clear();
	 order_table.clear();
	/*
	 * DEBS queries
	 */
	 std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table;
	 std::vector<Experiment::DebsChallenge::CompactRide> profitable_cell_table;
	 Experiment::DebsChallenge::FrequentRoutePartition debs_experiment_frequent_route;
	 debs_experiment_frequent_route.parse_debs_rides_with_to_string(ride_q1_file, &frequent_ride_table);
	 debs_experiment_frequent_route.frequent_route_simulation(&frequent_ride_table, 8);
	 debs_experiment_frequent_route.frequent_route_simulation(&frequent_ride_table, 16);
	 debs_experiment_frequent_route.frequent_route_simulation(&frequent_ride_table, 32);
	 frequent_ride_table.clear();

	 debs_experiment_frequent_route.parse_debs_rides_with_to_string(ride_q2_file, &profitable_cell_table);
	 Experiment::DebsChallenge::ProfitableAreaPartition debs_experiment_profit_cell;
	 debs_experiment_profit_cell.most_profitable_cell_simulation(&profitable_cell_table, 8);
	 debs_experiment_profit_cell.most_profitable_cell_simulation(&profitable_cell_table, 16);
	 debs_experiment_profit_cell.most_profitable_cell_simulation(&profitable_cell_table, 32);
	 profitable_cell_table.clear();

	/*
	 * GOOGLE-MONITOR-CLUSTER queries
	 */
	std::vector<Experiment::GoogleClusterMonitor::task_event> task_event_table;
	Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events_from_directory(google_task_event_dir, task_event_table);
	Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_simulation(&task_event_table, 8);
	Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_simulation(&task_event_table, 16);
	Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_simulation(&task_event_table, 32);

	Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_simulation(&task_event_table, 8);
	Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_simulation(&task_event_table, 16);
	Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_simulation(&task_event_table, 32);
	task_event_table.clear();

	return 0;
}

void log_normal_simulation(std::string input_file)
{
	Experiment::LogNormalSimulation simulation;
	//simulation.sort_to_plot(input_file);
	uint16_t task_num[] = { 10, 50, 100 };
	for (size_t i = 0; i < 3; i++)
	{
	std::vector<uint16_t> tasks;
	for (size_t j = 0; j < task_num[i]; j++)
	{
	tasks.push_back(uint16_t(j));
	}
	tasks.shrink_to_fit();
	std::cout << "## Tasks: " << task_num[i] << ".\n";
	simulation.simulate(tasks, input_file);
	}
	std::cout << "********* END ***********\n";
}

double get_cardinality_average(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	auto sum = 0.0;
	for (auto it = task_cardinality.cbegin(); it != task_cardinality.cend(); ++it)
	{
		sum += it->size();
	}
	return sum / task_cardinality.size();
}

double get_cardinality_max(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	auto max = 0.0;
	for (auto it = task_cardinality.cbegin(); it != task_cardinality.cend(); ++it)
	{
		max = max < it->size() ? it->size() : max;
	}
	return max;
}

double get_imbalance(const std::vector<std::unordered_set<uint32_t>>& task_cardinality)
{
	return get_cardinality_max(task_cardinality) - get_cardinality_average(task_cardinality);
}

double get_imbalance(const std::vector<uint32_t>& task_cardinality)
{
	auto max_card = *std::max_element(task_cardinality.begin(), task_cardinality.end());
	double sum_card = std::accumulate(task_cardinality.begin(), task_cardinality.end(), 0);
	return max_card - (sum_card / task_cardinality.size());
}