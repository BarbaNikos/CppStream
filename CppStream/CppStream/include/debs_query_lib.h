#pragma once
#include <fstream>
#include <vector>
#include <array>
#include <map>
#include <sstream>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <ctime>
#include <queue>
#include <thread>
#include <future>
#include <algorithm>
#include <unordered_set>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef DEBS_STRUCTURE_LIB_H_
#include "debs_structure_lib.h"
#endif // !DEBS_STRUCTURE_LIB_H_

#ifndef DEBS_CELL_COORDINATE_UTIL_H_
#include "debs_cell_coordinate_util.h"
#endif // !DEBS_CELL_COORDINATE_UTIL_H_

#ifndef DEBS_QUERY_LIB_H_
#define DEBS_QUERY_LIB_H_
namespace Experiment
{
	namespace DebsChallenge
	{
		class FrequentRouteWorkerThread
		{
		public:
			FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, std::mutex* aggr_mu, std::condition_variable* aggr_cond,
				std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~FrequentRouteWorkerThread();
			void operate();
			void update(DebsChallenge::CompactRide& ride);
			void finalize();
		private:
			std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue;
			std::mutex* aggr_mu; 
			std::condition_variable* aggr_cond;
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, uint64_t> result;
			std::queue<DebsChallenge::CompactRide>* input_queue;
		};

		class FrequentRouteAggregator
		{
		public:
			FrequentRouteAggregator(std::queue<Experiment::DebsChallenge::frequent_route>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~FrequentRouteAggregator();
			void operate();
			void update(Experiment::DebsChallenge::frequent_route& frequent_route);
			void finalize();
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, uint64_t> result;
			std::queue<DebsChallenge::frequent_route>* input_queue;
		};

		class FrequentRoutePartition
		{
		public:
			FrequentRoutePartition();
			~FrequentRoutePartition();
			void produce_compact_ride_file(const std::string input_file_name, const std::string output_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
			std::vector<Experiment::DebsChallenge::CompactRide> parse_debs_rides(const std::string input_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
			void parse_debs_rides_with_to_string(const std::string input_file_name, std::vector<Experiment::DebsChallenge::CompactRide>* buffer);
			void debs_partition_performance(const std::vector<uint16_t>& tasks, Partitioner& partitioner, const std::string partioner_name, std::vector<Experiment::DebsChallenge::CompactRide>& rides);
			double debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table, Partitioner& partitioner, 
				const std::string partitioner_name, const size_t max_queue_size);
		private:
			static void debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRouteWorkerThread* frequent_route);
			std::queue<Experiment::DebsChallenge::CompactRide>** queues;
			std::mutex* mu_xes;
			std::condition_variable* cond_vars;
			std::thread** threads;
			Experiment::DebsChallenge::FrequentRouteWorkerThread** query_workers;
			size_t max_queue_size;
		};

		class ProfitableArea
		{
		public:
			ProfitableArea(std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~ProfitableArea();
			void operate();
			void update(DebsChallenge::CompactRide& ride);
			void finalize();
		private:
			std::time_t* oldest_time;
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, std::vector<float>> fare_map;
			std::unordered_map<std::string, std::string> dropoff_cell;
			std::queue<DebsChallenge::CompactRide>* input_queue;
		};

		class ProfitableAreaAggregator
		{
		public:
			ProfitableAreaAggregator(std::queue<Experiment::DebsChallenge::most_profitable_cell>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~ProfitableAreaAggregator();
			void operate();
			void update(Experiment::DebsChallenge::most_profitable_cell& most_profitable_cell);
			void finalize();
		private:
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, uint64_t> result;
			std::queue<DebsChallenge::frequent_route>* input_queue;
		};

		class ProfitableAreaPartition
		{
		public:
			ProfitableAreaPartition();
			~ProfitableAreaPartition();
			double debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table, Partitioner& partitioner,
				const std::string partitioner_name, const size_t max_queue_size);
		private:
			static void debs_profitable_area_worker(Experiment::DebsChallenge::ProfitableArea* profitable_area);
			std::queue<Experiment::DebsChallenge::CompactRide>** queues;
			std::mutex* mu_xes;
			std::condition_variable* cond_vars;
			std::thread** threads;
			Experiment::DebsChallenge::ProfitableArea** query_workers;
			size_t max_queue_size;
		};
	}
}
#endif // !DEBS_QUERY_LIB_H_

Experiment::DebsChallenge::FrequentRouteWorkerThread::FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, std::mutex* aggr_mu, 
	std::condition_variable* aggr_cond, std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	if (aggregator_queue != nullptr)
	{
		this->aggregator_queue = aggregator_queue;
		this->aggr_mu = aggr_mu;
		this->aggr_cond = aggr_cond;
	}
	else
	{
		this->aggregator_queue = nullptr;
		this->aggr_mu = nullptr;
		this->aggr_cond = nullptr;
	}
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

inline Experiment::DebsChallenge::FrequentRouteWorkerThread::~FrequentRouteWorkerThread()
{
	result.clear();
}

inline void Experiment::DebsChallenge::FrequentRouteWorkerThread::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Experiment::DebsChallenge::CompactRide ride = input_queue->back();
		input_queue->pop();
		// process
		if (ride.trip_distance >= 0)
		{
			update(ride);
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

inline void Experiment::DebsChallenge::FrequentRouteWorkerThread::update(Experiment::DebsChallenge::CompactRide& ride)
{
	std::string key = std::to_string(ride.pickup_cell.first) +
		std::to_string(ride.pickup_cell.second) +
		std::to_string(ride.dropoff_cell.first) +
		std::to_string(ride.dropoff_cell.second);
	// update counts
	std::unordered_map<std::string, uint64_t>::iterator it = result.find(key);
	if (it != result.end())
	{
		it->second += 1;
	}
	else
	{
		result.insert(std::make_pair(key, uint64_t(1)));
	}
}

inline void Experiment::DebsChallenge::FrequentRouteWorkerThread::finalize()
{
	
	std::map<uint64_t, std::vector<std::string>> count_to_keys_map;
	std::vector<uint64_t> top_counts;
	std::vector<std::string> top_routes;
	if (aggregator_queue == nullptr)
	{
		for (std::unordered_map<std::string, uint64_t>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
		{
			auto top_iter = count_to_keys_map.find(it->second);
			if (top_iter == count_to_keys_map.end())
			{
				std::vector<std::string> buffer;
				buffer.push_back(it->first);
				count_to_keys_map.insert(std::make_pair(it->second, buffer));
			}
			else
			{
				count_to_keys_map[it->second].push_back(it->first);
			}
		}
		if (count_to_keys_map.size() > 0)
		{
			std::map<uint64_t, std::vector<std::string>>::const_iterator reverse_iter = count_to_keys_map.cend();
			do
			{
				reverse_iter--;
				for (size_t i = 0; i < reverse_iter->second.size(); ++i)
				{
					top_routes.push_back(reverse_iter->second[i]);
					top_counts.push_back(reverse_iter->first);
					if (top_routes.size() >= 10)
					{
						return;
					}
				}
			} while (reverse_iter != count_to_keys_map.cbegin());
		}
	}
	else
	{
		// start feeding partial aggregations to the aggregator
		// TO-DO: Complete this part
	}
}

Experiment::DebsChallenge::FrequentRouteAggregator::FrequentRouteAggregator(std::queue<Experiment::DebsChallenge::frequent_route>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

inline Experiment::DebsChallenge::FrequentRouteAggregator::~FrequentRouteAggregator()
{
	result.clear();
}

inline void Experiment::DebsChallenge::FrequentRouteAggregator::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Experiment::DebsChallenge::frequent_route freq_route = input_queue->back();
		input_queue->pop();
		// process
		if (freq_route.count >= 0)
		{
			update(freq_route);
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

inline void Experiment::DebsChallenge::FrequentRouteAggregator::update(Experiment::DebsChallenge::frequent_route & frequent_route)
{
	// update counts
	std::unordered_map<std::string, uint64_t>::iterator it = result.find(frequent_route.route);
	if (it != result.end())
	{
		it->second += 1;
	}
	else
	{
		result.insert(std::make_pair(frequent_route.route, frequent_route.count));
	}
}

inline void Experiment::DebsChallenge::FrequentRouteAggregator::finalize()
{
	std::map<uint64_t, std::vector<std::string>> count_to_keys_map;
	std::vector<uint64_t> top_counts;
	std::vector<std::string> top_routes;
	for (std::unordered_map<std::string, uint64_t>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		auto top_iter = count_to_keys_map.find(it->second);
		if (top_iter == count_to_keys_map.end())
		{
			std::vector<std::string> buffer;
			buffer.push_back(it->first);
			count_to_keys_map.insert(std::make_pair(it->second, buffer));
		}
		else
		{
			count_to_keys_map[it->second].push_back(it->first);
		}
	}
	if (count_to_keys_map.size() > 0)
	{
		std::map<uint64_t, std::vector<std::string>>::const_iterator reverse_iter = count_to_keys_map.cend();
		do
		{
			reverse_iter--;
			for (size_t i = 0; i < reverse_iter->second.size(); ++i)
			{
				top_routes.push_back(reverse_iter->second[i]);
				top_counts.push_back(reverse_iter->first);
				if (top_routes.size() >= 10)
				{
					return;
				}
			}
		} while (reverse_iter != count_to_keys_map.cbegin());
	}
}

inline Experiment::DebsChallenge::FrequentRoutePartition::FrequentRoutePartition()
{
}

inline Experiment::DebsChallenge::ProfitableAreaPartition::ProfitableAreaPartition()
{
}

inline Experiment::DebsChallenge::FrequentRoutePartition::~FrequentRoutePartition()
{
}

void Experiment::DebsChallenge::FrequentRoutePartition::produce_compact_ride_file(const std::string input_file_name, const std::string output_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells)
{
	std::string line;
	std::ifstream file(input_file_name);
	std::ofstream output_file(output_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	Experiment::DebsChallenge::DebsCellAssignment cell_assign(cell_side_size, grid_side_size_in_cells);
	while (getline(file, line))
	{
		Experiment::DebsChallenge::CompactRide ride;
		int val = cell_assign.parse_compact_ride(line, ride);
		if (val == 0)
		{
			output_file << ride.to_string() << "\n";
		}
	}
	file.close();
	output_file.flush();
	output_file.close();
}

inline Experiment::DebsChallenge::ProfitableAreaPartition::~ProfitableAreaPartition()
{
}

void Experiment::DebsChallenge::FrequentRoutePartition::parse_debs_rides_with_to_string(const std::string input_file_name, std::vector<Experiment::DebsChallenge::CompactRide>* buffer)
{
	std::string line;
	//std::map<std::string, uint32_t> ride_frequency;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	while (getline(file, line))
	{
		buffer->push_back(Experiment::DebsChallenge::CompactRide(line));
	}
	buffer->shrink_to_fit();
	file.close();
}

std::vector<Experiment::DebsChallenge::CompactRide> Experiment::DebsChallenge::FrequentRoutePartition::parse_debs_rides(const std::string input_file_name, uint32_t cell_side_size, uint32_t grid_side_size_in_cells)
{
	std::vector<Experiment::DebsChallenge::CompactRide> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	Experiment::DebsChallenge::DebsCellAssignment cell_assign(cell_side_size, grid_side_size_in_cells);
	std::chrono::system_clock::time_point scan_start = std::chrono::system_clock::now();
	while (getline(file, line))
	{
		Experiment::DebsChallenge::CompactRide ride;
		int val = cell_assign.parse_compact_ride(line, ride);
		if (val == 0)
		{
			lines.push_back(ride);
		}
	}
	std::chrono::system_clock::time_point scan_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	lines.shrink_to_fit();
	file.close();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";
	return lines;
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_partition_performance(const std::vector<uint16_t>& tasks, Partitioner& partitioner, 
	const std::string partioner_name, std::vector<Experiment::DebsChallenge::CompactRide>& rides)
{
	std::vector<std::unordered_set<std::string>> key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		short task = partitioner.partition_next(key.c_str(), key.length());
		key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> partition_time = part_end - part_start;
	size_t min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t max_cardinality = std::numeric_limits<uint64_t>::min();
	double average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (min_cardinality > key_per_task[i].size())
		{
			min_cardinality = key_per_task[i].size();
		}
		if (max_cardinality < key_per_task[i].size())
		{
			max_cardinality = key_per_task[i].size();
		}
		average_cardinality += key_per_task[i].size();
		std::cout << key_per_task[i].size() << " ";
		key_per_task[i].clear();
	}
	std::cout << "\n";
	average_cardinality = average_cardinality / tasks.size();
	key_per_task.clear();
	std::cout << "Time partition using " << partioner_name << ": " << partition_time.count() << " (msec). Min: " << min_cardinality <<
		", Max: " << max_cardinality << ", AVG: " << average_cardinality << "\n";
}

double Experiment::DebsChallenge::FrequentRoutePartition::debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table, 
	Partitioner& partitioner, const std::string partitioner_name, const size_t max_queue_size)
{
	// initialize shared memory
	queues = new std::queue<Experiment::DebsChallenge::CompactRide>*[tasks.size()];
	mu_xes = new std::mutex[tasks.size()];
	cond_vars = new std::condition_variable[tasks.size()];
	threads = new std::thread*[tasks.size()];
	query_workers = new Experiment::DebsChallenge::FrequentRouteWorkerThread*[tasks.size()];
	this->max_queue_size = max_queue_size;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::CompactRide>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRouteWorkerThread(nullptr, nullptr, nullptr, queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	//std::cout << partitioner_name << " thread INITIATES partitioning (frequent-route).\n";
	// start partitioning
	std::chrono::system_clock::time_point partition_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = route_table.cbegin(); it != route_table.cend(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "-" +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = partitioner.partition_next(key.c_str(), key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		cond_vars[task].wait(locker, [this, task, max_queue_size]() { return queues[task]->size() < max_queue_size; });
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	//std::cout << partitioner_name << " thread SENT all rides (frequent-route).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::CompactRide final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		cond_vars[i].wait(locker, [this, i, max_queue_size]() { return queues[i]->size() < max_queue_size; });
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point partition_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> partition_time = partition_end - partition_start;
	//std::cout << partitioner_name << " total partition time: " << partition_time.count() << " (msec) (frequent-route).\n";
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
	//std::cout << "------END-----\n";
	return partition_time.count();
}

double Experiment::DebsChallenge::ProfitableAreaPartition::debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table,
	Partitioner& partitioner, const std::string partitioner_name, const size_t max_queue_size)
{
	// initialize shared memory
	queues = new std::queue<Experiment::DebsChallenge::CompactRide>*[tasks.size()];
	mu_xes = new std::mutex[tasks.size()];
	cond_vars = new std::condition_variable[tasks.size()];
	threads = new std::thread*[tasks.size()];
	query_workers = new Experiment::DebsChallenge::ProfitableArea*[tasks.size()];
	this->max_queue_size = max_queue_size;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::CompactRide>();
		query_workers[i] = new Experiment::DebsChallenge::ProfitableArea(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_profitable_area_worker, query_workers[i]);
	}
	//std::cout << partitioner_name << " thread INITIATES partitioning (profit-areas).\n";
	// start partitioning
	std::chrono::system_clock::time_point partition_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = route_table.cbegin(); it != route_table.cend(); ++it)
	{
		std::string key = it->medallion;
		short task = partitioner.partition_next(key.c_str(), key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		cond_vars[task].wait(locker, [this, task, max_queue_size]() { return queues[task]->size() < max_queue_size; });
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	//std::cout << partitioner_name << " thread SENT all rides (profit-areas).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::CompactRide final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		cond_vars[i].wait(locker, [this, i, max_queue_size]() { return queues[i]->size() < max_queue_size; });
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point partition_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> partition_time = partition_end - partition_start;
	//std::cout << partitioner_name << " total partition time: " << partition_time.count() << " (msec) (profit-areas).\n";
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
	//std::cout << "------END-----\n";
	return partition_time.count();
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRouteWorkerThread* frequent_route)
{
	frequent_route->operate();
}

void Experiment::DebsChallenge::ProfitableAreaPartition::debs_profitable_area_worker(Experiment::DebsChallenge::ProfitableArea* profitable_area)
{
	profitable_area->operate();
}

Experiment::DebsChallenge::ProfitableArea::ProfitableArea(std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

Experiment::DebsChallenge::ProfitableArea::~ProfitableArea()
{
	fare_map.clear();
	dropoff_cell.clear();
}

void Experiment::DebsChallenge::ProfitableArea::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Experiment::DebsChallenge::CompactRide ride = input_queue->back();
		input_queue->pop();
		// process
		if (ride.trip_distance >= 0)
		{
			update(ride);
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

void Experiment::DebsChallenge::ProfitableArea::update(DebsChallenge::CompactRide & ride)
{
	// update fare table
	float total = ride.fare_amount + ride.tip_amount;
	std::string pickup_cell = std::to_string(ride.pickup_cell.first) + "." +
		std::to_string(ride.pickup_cell.second);
	auto it = this->fare_map.find(pickup_cell);
	if (it != this->fare_map.end())
	{
		it->second.push_back(total);
	}
	else
	{
		std::vector<float> fares;
		fares.push_back(total);
		this->fare_map[pickup_cell] = fares;
	}
	// update area cell
	auto medallion_it = this->dropoff_cell.find(ride.medallion);
	if (medallion_it != this->dropoff_cell.end())
	{
		medallion_it->second = std::to_string(ride.dropoff_cell.first) + "." +
			std::to_string(ride.dropoff_cell.second);
	}
	else
	{
		this->dropoff_cell[ride.medallion] = std::to_string(ride.dropoff_cell.first) + "." +
			std::to_string(ride.dropoff_cell.second);
	}
}

void Experiment::DebsChallenge::ProfitableArea::finalize()
{
	// calculate # of empty taxis per area
	std::unordered_map<std::string, uint32_t> empty_taxi_count;
	std::map<double, std::vector<std::string>> most_profitable_areas;
	for (auto it = this->dropoff_cell.cbegin(); it != this->dropoff_cell.cend(); ++it)
	{
		auto cell_it = empty_taxi_count.find(it->second);
		if (cell_it != empty_taxi_count.end())
		{
			cell_it->second++;
		}
		else
		{
			empty_taxi_count[it->second] = uint32_t(1);
		}
	}
	// for each cell that has # of empty taxis > 0
	for (auto cell_it = empty_taxi_count.cbegin(); cell_it != empty_taxi_count.cend(); ++cell_it)
	{
		auto fare_it = this->fare_map.find(cell_it->first);
		if (fare_it != this->fare_map.end())
		{
			// calculate median
			double median = 0;
			if (fare_it->second.size() % 2 != 0)
			{
				std::nth_element(fare_it->second.begin(), fare_it->second.begin() + fare_it->second.size() / 2,
					fare_it->second.end());
				median = fare_it->second[fare_it->second.size() / 2];
			}
			else
			{
				std::nth_element(fare_it->second.begin(), fare_it->second.begin() + fare_it->second.size() / 2,
					fare_it->second.end());
				std::nth_element(fare_it->second.begin(), fare_it->second.begin() + fare_it->second.size() / 2 + 1,
					fare_it->second.end());
				median = (fare_it->second[fare_it->second.size() / 2] + fare_it->second[fare_it->second.size() / 2 + 1]) / 2;
			}
			auto mpa_it = most_profitable_areas.find(median);
			if (mpa_it != most_profitable_areas.end())
			{
				mpa_it->second.push_back(cell_it->first);
			}
			else
			{
				most_profitable_areas[median] = std::vector<std::string>();
				most_profitable_areas[median].push_back(cell_it->first);
			}
		}
	}
	// get the top-10
	if (most_profitable_areas.size() > 0)
	{
		std::vector<std::string> top_10_areas;
		std::vector<double> top_10_profit;
		auto it = most_profitable_areas.cend();
		do
		{
			it--;
			for (size_t i = 0; i < it->second.size(); ++i)
			{
				top_10_areas.push_back(it->second[i]);
				top_10_profit.push_back(it->first);
				if (top_10_areas.size() >= 10)
				{
					return;
				}
			}
		} while (it != most_profitable_areas.cbegin());
	}
}
