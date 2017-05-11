#pragma once
#include <fstream>
#include <functional>
#include <future>
#include <iostream>

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif  // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cout << "usage: <google_task_event_dir> <1 - throughput experiment, 2 "
                 "- windowed latency>\n";
    exit(1);
  }
  std::string google_task_event_dir = argv[1];
  std::string experiment = argv[2];
  /*
  * GOOGLE-MONITOR-CLUSTER queries
  */
  if (experiment.compare("1") == 0) {
    std::vector<Experiment::GoogleClusterMonitor::task_event> task_event_table;
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        parse_task_events_from_directory(google_task_event_dir,
                                         task_event_table);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_simulation(task_event_table, 8);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_simulation(task_event_table, 16);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_simulation(task_event_table, 32);

    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_simulation(task_event_table, 8);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_simulation(task_event_table, 16);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_simulation(task_event_table, 32);
    task_event_table.clear();
  } else if (experiment.compare("2") == 0) {
    // need to figure out how to provide a single file
    std::vector<Experiment::GoogleClusterMonitor::task_event> task_event_table;
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        parse_task_events_from_directory(google_task_event_dir,
                                         task_event_table);

    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_window_simulation(task_event_table, 8);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_window_simulation(task_event_table, 16);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_window_simulation(task_event_table, 32);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_window_simulation(task_event_table, 64);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_window_simulation(task_event_table, 128);
    Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
        query_window_simulation(task_event_table, 256);

    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_window_simulation(task_event_table, 8);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_window_simulation(task_event_table, 16);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_window_simulation(task_event_table, 32);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_window_simulation(task_event_table, 64);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_window_simulation(task_event_table, 128);
    Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
        query_window_simulation(task_event_table, 256);
  } else {
    std::cout << "usage: <google_task_event_dir> <1 - throughput experiment, 2 "
                 "- windowed latency>\n";
    exit(1);
  }
#ifdef _WIN32
  std::cout << "Press ENTER to Continue";
  std::cin.ignore();
  return 0;
#else  // _WIN32
  return 0;
#endif
}
