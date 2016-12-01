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

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif // !GOOGLE_CLUSTER_MONITOR_QUERY_H_

int main(int argc, char** argv)
{
	if (argc < 2)
	{
		std::cout << "usage: <google_task_event_dir>\n";
		exit(1);
	}
	std::string google_task_event_dir = argv[1];
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
	
#ifdef _WIN32
	std::cout << "Press ENTER to Continue";
	std::cin.ignore();
	return 0;
#else // _WIN32
	return 0;
#endif
}