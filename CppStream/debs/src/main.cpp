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
#include "c_hll.h"
#endif // !C_HLL_H_

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // !DEBS_QUERY_LIB_H_

int main(int argc, char** argv)
{
	if (argc < 3)
	{
		std::cout << "usage: <debs_ride_q1.csv> <debs_ride_q2.csv>\n";
		exit(1);
	}
	std::string ride_q1_file = argv[1];
	std::string ride_q2_file = argv[2];
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
#ifdef _WIN32
	std::cout << "Press ENTER to Continue";
	std::cin.ignore();
	return 0;
#else // _WIN32
	return 0;
#endif
}