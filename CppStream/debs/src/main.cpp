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

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // !DEBS_QUERY_LIB_H_

#ifndef PARTITION_LATENCY_EXP_
#include "../include/partition_latency.h"
#endif // !PARTITION_LATENCY_EXP_

int main(int argc, char** argv)
{
	if (argc < 4)
	{
		std::cout << "usage: <debs_ride_q1.csv> <debs_ride_q2.csv> <experiment: 1 - queries, 2 - part. latency, 3 - windowed queries>\n";
		exit(1);
	}
	std::string ride_q1_file = argv[1];
	std::string ride_q2_file = argv[2];
	std::string experiment = argv[3];

	std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table;
	std::vector<Experiment::DebsChallenge::CompactRide> profitable_cell_table;
	if (experiment.compare("1") == 0)
	{
		/*
		* DEBS queries
		*/
		Experiment::DebsChallenge::Parser::parse_debs_rides_with_to_string(ride_q1_file, &frequent_ride_table);
		Experiment::DebsChallenge::FrequentRoutePartition debs_experiment_frequent_route;
		debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table, 8);
		debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table, 16);
		debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table, 32);
		frequent_ride_table.clear();
		Experiment::DebsChallenge::Parser::parse_debs_rides_with_to_string(ride_q2_file, &profitable_cell_table);
		Experiment::DebsChallenge::ProfitableAreaPartition debs_experiment_profit_cell;
		debs_experiment_profit_cell.most_profitable_cell_simulation(profitable_cell_table, 8);
		debs_experiment_profit_cell.most_profitable_cell_simulation(profitable_cell_table, 16);
		debs_experiment_profit_cell.most_profitable_cell_simulation(profitable_cell_table, 32);
		profitable_cell_table.clear();
	}
	else if (experiment.compare("2") == 0)
	{
		/*
		 * Partition latency on DEBS Query 1
		 */
		PartitionLatency latency_experiment;
		Experiment::DebsChallenge::Parser::parse_debs_rides_with_to_string(ride_q1_file, &frequent_ride_table);
		latency_experiment.measure_latency(8, frequent_ride_table);
		latency_experiment.measure_latency(16, frequent_ride_table);
		latency_experiment.measure_latency(32, frequent_ride_table);
		frequent_ride_table.clear();
	}
	else if (experiment.compare("3") == 0)
	{
		Experiment::DebsChallenge::FrequentRoutePartition debs_frequent_route;
		debs_frequent_route.frequent_route_window_simulation(ride_q1_file, 8);
		debs_frequent_route.frequent_route_window_simulation(ride_q1_file, 16);
		debs_frequent_route.frequent_route_window_simulation(ride_q1_file, 32);
		
		Experiment::DebsChallenge::ProfitableAreaPartition debs_profitable_route;
		debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 8);
		debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 16);
		debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 32);
	}
	else
	{
		std::cout << "un-recognized experiment-id. valid ids: 1 - debs queries, 2 - debs q1 part. latency\n";
	}
#ifdef _WIN32
	std::cout << "Press ENTER to Continue";
	std::cin.ignore();
	return 0;
#else // _WIN32
	return 0;
#endif
}