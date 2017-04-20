#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <numeric>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif  // !DEBS_QUERY_LIB_H_

#ifndef PARTITION_LATENCY_EXP_
#include "../include/partition_latency_exp.h"
#endif  // !PARTITION_LATENCY_EXP_

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cout << "usage: <debs_ride_q1.csv> <debs_ride_q2.csv> <parse: 0, experiment: 1 "
                 "- queries, 2 - part. latency, 3 - windowed queries>\n";
    exit(1);
  }
  std::string ride_q1_file = argv[1];
  std::string ride_q2_file = argv[2];
  std::string experiment = argv[3];

  std::cout << "file-1: " << argv[1] << ", file-2: " << argv[2] << ", experiment: " << 
    experiment << "\n";
  std::vector<Experiment::DebsChallenge::CompactRide> frequent_ride_table;
  std::vector<Experiment::DebsChallenge::CompactRide> profitable_cell_table;
  if (experiment.compare("0") == 0) {
    Experiment::DebsChallenge::Parser::produce_compact_ride_file(argv[1], 
        "debs_frequent_route_compact.csv", 500, 300);
    Experiment::DebsChallenge::Parser::produce_compact_ride_file(argv[2], 
        "debs_profitable_cell_compact.csv", 250, 600);
  } else if (experiment.compare("1") == 0) {
    /*
    * DEBS queries
    */
    // Experiment::DebsChallenge::Parser::parse_debs_rides_with_to_string(ride_q1_file,
    // &frequent_ride_table);
    // Experiment::DebsChallenge::FrequentRoutePartition
    // debs_experiment_frequent_route;
    // debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table,
    // 8);
    // debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table,
    // 16);
    // debs_experiment_frequent_route.frequent_route_simulation(frequent_ride_table,
    // 32);
    // frequent_ride_table.clear();
    Experiment::DebsChallenge::Parser::parse_debs_rides_with_to_string(
        ride_q2_file, &profitable_cell_table);
    Experiment::DebsChallenge::ProfitableAreaPartition
        debs_experiment_profit_cell;
    // debs_experiment_profit_cell.most_profitable_cell_simulation(profitable_cell_table,
    // 8);
    // debs_experiment_profit_cell.most_profitable_cell_simulation(profitable_cell_table,
    // 16);
    // debs_experiment_profit_cell.most_profitable_cell_simulation(profitable_cell_table,
    // 32);
    debs_experiment_profit_cell.most_profitable_cell_simulation(
        profitable_cell_table, 64);
    debs_experiment_profit_cell.most_profitable_cell_simulation(
        profitable_cell_table, 128);
    profitable_cell_table.clear();
  } else if (experiment.compare("2") == 0) {
    /*
     * Partition latency on DEBS Query 1
     */
    Experiment::DebsChallenge::PartitionLatency latency_experiment;
    Experiment::DebsChallenge::Parser::parse_debs_rides_with_to_string(
        ride_q1_file, &frequent_ride_table);
    latency_experiment.measure_latency(8, frequent_ride_table);
    latency_experiment.measure_latency(16, frequent_ride_table);
    latency_experiment.measure_latency(32, frequent_ride_table);
    frequent_ride_table.clear();
  } else if (experiment.compare("3") == 0) {
    Experiment::DebsChallenge::FrequentRoutePartition debs_frequent_route;
    // debs_frequent_route.frequent_route_window_simulation(ride_q1_file, 8);
    // debs_frequent_route.frequent_route_window_simulation(ride_q1_file, 16);
    // debs_frequent_route.frequent_route_window_simulation(ride_q1_file, 32);

    Experiment::DebsChallenge::ProfitableAreaPartition debs_profitable_route;
    debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 8);
    debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 16);
    debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 32);
    debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 64);
    debs_profitable_route.profitable_route_window_simulation(ride_q2_file, 128);
  } else {
    std::cout << "un-recognized experiment-id. valid ids: 1 - debs queries, 2 "
                 "- debs q1 part. latency\n";
  }
#ifdef _WIN32
  std::cout << "Press ENTER to Continue";
  std::cin.ignore();
  return 0;
#else  // _WIN32
  return 0;
#endif
}
