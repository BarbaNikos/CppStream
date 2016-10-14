#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // DEBS_QUERY_LIB_H_

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
	this->result_output_file_name = "";
}

Experiment::DebsChallenge::FrequentRouteWorkerThread::FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, std::mutex * aggr_mu, std::condition_variable * aggr_cond, std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex * mu, std::condition_variable * cond, const std::string result_output_file_name)
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
	this->result_output_file_name = result_output_file_name;
}

Experiment::DebsChallenge::FrequentRouteWorkerThread::~FrequentRouteWorkerThread()
{
	result.clear();
}

void Experiment::DebsChallenge::FrequentRouteWorkerThread::operate()
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

void Experiment::DebsChallenge::FrequentRouteWorkerThread::update(Experiment::DebsChallenge::CompactRide& ride)
{
	std::string key = std::to_string(ride.pickup_cell.first) + "." + std::to_string(ride.pickup_cell.second) + "-" +
		std::to_string(ride.dropoff_cell.first) + "." + std::to_string(ride.dropoff_cell.second);
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

void Experiment::DebsChallenge::FrequentRouteWorkerThread::finalize()
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
		if (result_output_file_name.compare("") != 0)
		{
			std::ofstream output_file(result_output_file_name);
			if (output_file.is_open() == false)
			{
				std::cerr << "FrequentRouteWorkerThread::finalize() failed to open file with name: " << result_output_file_name << ".\n";
				exit(1);
			}
			for (std::unordered_map<std::string, uint64_t>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
			{
				output_file << it->first << "," << it->second << "\n";
			}
			output_file.flush();
			output_file.close();
		}
	}
}

void Experiment::DebsChallenge::FrequentRouteWorkerThread::partial_finalize(std::vector<Experiment::DebsChallenge::frequent_route>& partial_result)
{
	for (std::unordered_map<std::string, uint64_t>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		Experiment::DebsChallenge::frequent_route fr;
		fr.route = it->first;
		fr.count = it->second;
		partial_result.push_back(fr);
	}
}

Experiment::DebsChallenge::FrequentRouteOfflineAggregator::FrequentRouteOfflineAggregator()
{
}

Experiment::DebsChallenge::FrequentRouteOfflineAggregator::~FrequentRouteOfflineAggregator()
{
}

void Experiment::DebsChallenge::FrequentRouteOfflineAggregator::sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& full_aggregates,
	const std::string& out_filename)
{
	FILE* fd;
	std::map<unsigned long, std::vector<std::string>> final_result;
	for (std::vector<frequent_route>::const_iterator cit = full_aggregates.cbegin(); cit != full_aggregates.cend(); ++cit)
	{
		auto it = final_result.find(cit->count);
		if (it != final_result.end())
		{
			it->second.push_back(cit->route);
		}
		else
		{
			std::vector<std::string> tmp;
			tmp.push_back(cit->route);
			final_result.insert(std::make_pair(cit->count, tmp));
		}
	}
	fd = fopen(out_filename.c_str(), "w");
	for (std::map<unsigned long, std::vector<std::string>>::const_iterator c = final_result.cbegin(); c != final_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			std::string buffer = *i + "," + std::to_string(c->first) + "\n";
			fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		}
	}
	fclose(fd);
	final_result.clear();
}

void Experiment::DebsChallenge::FrequentRouteOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& partial_aggregates,
	const std::string& out_filename)
{
	FILE* fd;
	for (std::vector<frequent_route>::const_iterator cit = partial_aggregates.cbegin(); cit != partial_aggregates.cend(); ++cit)
	{
		auto it = result.find(cit->route);
		if (it != result.end())
		{
			it->second += cit->count;
		}
		else
		{
			result.insert(std::make_pair(cit->route, cit->count));
		}
	}
	std::map<unsigned long, std::vector<std::string>> final_result;
	for (auto cit = result.begin(); cit != result.end(); ++cit)
	{
		auto it = final_result.find(cit->second);
		if (it != final_result.end())
		{
			it->second.push_back(cit->first);
		}
		else
		{
			std::vector<std::string> tmp;
			tmp.push_back(cit->first);
			final_result.insert(std::make_pair(cit->second, tmp));
		}
	}
	// at this point the result is materialized
	fd = fopen(out_filename.c_str(), "w");
	for (std::map<unsigned long, std::vector<std::string>>::const_iterator c = final_result.cbegin(); c != final_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			std::string buffer = *i + "," + std::to_string(c->first) + "\n";
			fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		}
	}
	fclose(fd);
	final_result.clear();
}

Experiment::DebsChallenge::FrequentRoutePartition::FrequentRoutePartition()
{
}

Experiment::DebsChallenge::FrequentRoutePartition::~FrequentRoutePartition()
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

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines)
{
	RoundRobinPartitioner* rrg;
	PkgPartitioner* pkg;
	HashFieldPartitioner* fld;
	CardinalityAwarePolicy ca_policy;
	CaPartitionLib::CA_Exact_Partitioner* ca_naive;
	LoadAwarePolicy la_policy;
	CaPartitionLib::CA_Exact_Partitioner* la_naive;
	std::vector<uint16_t> tasks;

	// tasks: 10
	for (uint16_t i = 0; i < 10; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	frequent_route_partitioner_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	frequent_route_partitioner_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	frequent_route_partitioner_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	frequent_route_partitioner_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	frequent_route_partitioner_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
	// tasks: 100
	for (uint16_t i = 0; i < 100; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	frequent_route_partitioner_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	frequent_route_partitioner_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	frequent_route_partitioner_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	frequent_route_partitioner_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	frequent_route_partitioner_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_partitioner_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks, 
	Partitioner & partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix)
{
	std::queue<Experiment::DebsChallenge::CompactRide> queue;
	std::mutex mu;
	std::condition_variable cond;
	// get maximum and minimum running times
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::vector<Experiment::DebsChallenge::frequent_route> partial_result;
	// first read the input file and generate sub-files with 
	// the tuples that will be handled by each worker
	std::ofstream** out_file;
	out_file = new std::ofstream*[tasks.size()];
	// create files
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i] = new std::ofstream(partitioner_name + "_" + std::to_string(i) + ".csv");
	}
	// distribute tuples
	for (auto it = rides.cbegin(); it != rides.cend(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		*out_file[task] << it->to_string() << "\n";
	}
	// write out files and clean up memory
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i]->flush();
		out_file[i]->close();
		delete out_file[i];
	}
	delete[] out_file;

	// for every task - calculate (partial) workload
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<Experiment::DebsChallenge::frequent_route> p;
		std::vector<Experiment::DebsChallenge::CompactRide>* task_lines = new std::vector<Experiment::DebsChallenge::CompactRide>();
		std::string workload_file_name = partitioner_name + "_" + std::to_string(i) + ".csv";
		parse_debs_rides_with_to_string(workload_file_name, task_lines);
		// feed the worker
		Experiment::DebsChallenge::FrequentRouteWorkerThread worker(nullptr, nullptr, nullptr, &queue, &mu, &cond,
			worker_output_file_name_prefix + "_" + std::to_string(i));

		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - START
		for (auto it = task_lines->begin(); it != task_lines->end(); ++it)
		{
			worker.update(*it);
		}

		worker.partial_finalize(p);
		partial_result.reserve(partial_result.size() + p.size());
		std::move(p.begin(), p.end(), std::inserter(partial_result, partial_result.end()));
		// TIME CRITICAL CODE - END
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();

		std::chrono::duration<double, std::milli> execution_time = end - start;

		sum_of_durations += execution_time.count();
		if (max_duration < execution_time.count())
		{
			max_duration = execution_time.count();
		}
		min_duration = i == 0 ? execution_time.count() : (min_duration > execution_time.count() ? execution_time.count() : min_duration);

		p.clear();
		task_lines->clear();
		delete task_lines;
		std::remove(workload_file_name.c_str());
	}

	Experiment::DebsChallenge::FrequentRouteOfflineAggregator aggregator;
	// TIME CRITICAL CODE - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_sort_final_aggregation(partial_result, partitioner_name + "_full_result.csv");
	}
	else
	{
		aggregator.sort_final_aggregation(partial_result, partitioner_name + "_full_result.csv");
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL CODE - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";

	partial_result.clear();
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRouteWorkerThread* frequent_route)
{
	frequent_route->operate();
}

Experiment::DebsChallenge::ProfitableArea::ProfitableArea(std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex * mu, std::condition_variable * cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

Experiment::DebsChallenge::ProfitableArea::~ProfitableArea()
{
	fare_map.clear();
	dropoff_table.clear();
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
	float total = ride.fare_amount + ride.tip_amount;
	std::string pickup_cell = std::to_string(ride.pickup_cell.first) + "." +
		std::to_string(ride.pickup_cell.second);
	std::string dropoff_cell = std::to_string(ride.dropoff_cell.first) + "." + 
		std::to_string(ride.dropoff_cell.second);
	// update fare table
	auto it = fare_map.find(pickup_cell);
	if (it != fare_map.end())
	{
		it->second.push_back(total);
	}
	else
	{
		std::vector<float> fares;
		fares.push_back(total);
		fare_map[pickup_cell] = fares;
	}
	// update area cell
	auto med_it = this->dropoff_table.find(ride.medallion);
	if (med_it != dropoff_table.end())
	{
		if (med_it->second.second <= ride.dropoff_datetime)
		{
			// update
			med_it->second.first = dropoff_cell;
			med_it->second.second = ride.dropoff_datetime;
		}
	}
	else
	{
		dropoff_table.insert(std::make_pair(ride.medallion, std::make_pair(dropoff_cell, ride.dropoff_datetime)));
	}
}

void Experiment::DebsChallenge::ProfitableArea::first_round_aggregation(std::unordered_map<std::string, std::vector<float>>& complete_fare_map, 
	std::unordered_map<std::string, std::pair<std::string, std::time_t>>& complete_dropoff_table, const bool two_choice_partition_used)
{
	for (std::unordered_map<std::string, std::vector<float>>::const_iterator it = fare_map.cbegin(); it != fare_map.cend(); ++it)
	{
		auto c_fm_it = complete_fare_map.find(it->first);
		if (c_fm_it != complete_fare_map.end())
		{
			// add all the fares recorded
			c_fm_it->second.reserve(c_fm_it->second.size() + it->second.size());
			std::move(it->second.begin(), it->second.end(), std::inserter(c_fm_it->second, c_fm_it->second.end()));
		}
		else 
		{
			// create new record
			complete_fare_map.insert(std::make_pair(it->first, it->second));
		}
	}
	if (two_choice_partition_used)
	{
		// if a two choice partitioner is used - an aggregation should take place for figuring out the last 
		// cell a taxi has dropped off a ride
		for (std::unordered_map<std::string, std::pair<std::string, std::time_t>>::const_iterator it = dropoff_table.cbegin();
			it != dropoff_table.cend(); ++it)
		{
			auto medallion_it = complete_dropoff_table.find(it->first);
			if (medallion_it != complete_dropoff_table.end())
			{
				if (medallion_it->second.second < it->second.second && medallion_it->second.first.compare(it->second.first) != 0)
				{
					medallion_it->second.first = it->second.first;
					medallion_it->second.second = it->second.second;
				}
			}
			else
			{
				complete_dropoff_table.insert(std::make_pair(it->first, std::make_pair(it->second.first, it->second.second)));
			}
		}
	}
	else
	{
		// if a single choice partitioner is used - the information about medallions have to be placed in the intermediate buffer
		for (std::unordered_map<std::string, std::pair<std::string, std::time_t>>::const_iterator it = dropoff_table.cbegin();
			it != dropoff_table.cend(); ++it)
		{
			complete_dropoff_table.insert(std::make_pair(it->first, it->second));
		}
	}
}

void Experiment::DebsChallenge::ProfitableArea::second_round_init()
{
	this->pickup_cell_median_fare.clear();
	this->dropoff_cell_empty_taxi_count.clear();
}

void Experiment::DebsChallenge::ProfitableArea::second_round_update(const std::string & pickup_cell, std::vector<float>& fare_list)
{
	// calculate median
	float median_fare;
	if (fare_list.size() % 2 != 0)
	{
		std::nth_element(fare_list.begin(), fare_list.begin() + fare_list.size() / 2, fare_list.end());
		median_fare = fare_list[fare_list.size() / 2];
	}
	else
	{
		std::nth_element(fare_list.begin(), fare_list.begin() + fare_list.size() / 2, fare_list.end());
		std::nth_element(fare_list.begin(), fare_list.begin() + fare_list.size() / 2 + 1, fare_list.end());
		median_fare = (fare_list[fare_list.size() / 2] + fare_list[fare_list.size() / 2 + 1]) / 2;
	}
	auto it = pickup_cell_median_fare.find(pickup_cell);
	if (it != pickup_cell_median_fare.end())
	{
		// this is probably a mistake since each pickup-cell has a single list of fare lists
		// it->second = median_fare;
		std::cerr << "second_round_update() identified duplicate pickup-cell record. the cell-id: " << pickup_cell << " was encountered twice. current median value found: " << it->second <<
			", new value: " << median_fare << ".\n";
	}
	else
	{
		pickup_cell_median_fare.insert(std::make_pair(pickup_cell, median_fare));
	}
}

void Experiment::DebsChallenge::ProfitableArea::second_round_update(const std::string & medallion, const std::string & dropoff_cell, const time_t & timestamp)
{
	auto it = this->dropoff_cell_empty_taxi_count.find(dropoff_cell);
	if (it != dropoff_cell_empty_taxi_count.end())
	{
		it->second += 1;
	}
	else
	{
		dropoff_cell_empty_taxi_count.insert(std::make_pair(dropoff_cell, 1));
	}
}

void Experiment::DebsChallenge::ProfitableArea::partial_finalize(std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer)
{
	for (auto dropoff_cell_it = dropoff_cell_empty_taxi_count.begin(); dropoff_cell_it != dropoff_cell_empty_taxi_count.end(); dropoff_cell_it++)
	{
		auto pickup_cell_it = pickup_cell_median_fare.find(dropoff_cell_it->first);
		if (pickup_cell_it != pickup_cell_median_fare.end())
		{
			// calculate profit
			auto cell_profit_buffer_it = cell_profit_buffer.find(dropoff_cell_it->first);
			if (cell_profit_buffer_it != cell_profit_buffer.end())
			{
				cell_profit_buffer_it->second.first += dropoff_cell_it->second;
				cell_profit_buffer_it->second.second += pickup_cell_it->second;
			}
			else
			{
				cell_profit_buffer_it->second.first = dropoff_cell_it->second;
				cell_profit_buffer_it->second.second = pickup_cell_it->second;
			}
		}
	}
}

void Experiment::DebsChallenge::ProfitableArea::finalize(std::unordered_map<std::string, float>& cell_profit_buffer)
{
	for (auto dropoff_cell_it = dropoff_cell_empty_taxi_count.begin(); dropoff_cell_it != dropoff_cell_empty_taxi_count.end(); dropoff_cell_it++)
	{
		auto pickup_cell_it = pickup_cell_median_fare.find(dropoff_cell_it->first);
		if (pickup_cell_it != pickup_cell_median_fare.end())
		{
			float profit = pickup_cell_it->second / dropoff_cell_it->second;
			cell_profit_buffer[dropoff_cell_it->first] = profit;
		}
	}
}

Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::ProfitableAreaOfflineAggregator()
{
}

Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::~ProfitableAreaOfflineAggregator()
{
}

void Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::sort_final_aggregation(const std::unordered_map<std::string, float>& cell_profit_buffer, const std::string& out_file)
{
	FILE* fd;
	std::map<float, std::vector<std::string>> final_result;
	for (std::unordered_map<std::string, float>::const_iterator cit = cell_profit_buffer.cbegin(); cit != cell_profit_buffer.cend(); ++cit)
	{
		auto it = final_result.find(cit->second);
		if (it != final_result.end())
		{
			it->second.push_back(cit->first);
		}
		else
		{
			std::vector<std::string> tmp;
			tmp.push_back(cit->first);
			final_result[cit->second] = tmp;
		}
	}
	fd = fopen(out_file.c_str(), "w");
	for (std::map<float, std::vector<std::string>>::const_iterator c = final_result.cbegin(); c != final_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			std::string buffer = *i + "," + std::to_string(c->first) + "\n";
			fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		}
	}
	fclose(fd);
	final_result.clear();
}

void Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::calculate_and_sort_final_aggregation(const std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer, const std::string& out_file)
{
	FILE* fd;
	std::unordered_map<std::string, std::pair<float, int>> cell_profit_final_buffer;
	for (std::unordered_map<std::string, std::pair<float, int>>::const_iterator cit = cell_profit_buffer.cbegin(); cit != cell_profit_buffer.cend(); ++cit)
	{
		auto it = cell_profit_final_buffer.find(cit->first);
		if (it != cell_profit_final_buffer.end())
		{
			it->second.first += cit->second.first;
			it->second.second += cit->second.second;
		}
		else
		{
			cell_profit_final_buffer[cit->first] = cit->second;
		}
	}
	std::map<float, std::vector<std::string>> final_result;
	for (auto cit = cell_profit_final_buffer.begin(); cit != cell_profit_final_buffer.end(); ++cit)
	{
		float final_profit = cit->second.first / cit->second.second;
		auto it = final_result.find(final_profit);
		if (it != final_result.end())
		{
			it->second.push_back(cit->first);
		}
		else
		{
			std::vector<std::string> tmp;
			tmp.push_back(cit->first);
			final_result[final_profit] = tmp;
		}
	}
	// at this point the result is materialized
	fd = fopen(out_file.c_str(), "w");
	for (std::map<float, std::vector<std::string>>::const_iterator c = final_result.cbegin(); c != final_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			std::string buffer = *i + "," + std::to_string(c->first) + "\n";
			fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		}
	}
	fclose(fd);
	final_result.clear();
}

Experiment::DebsChallenge::ProfitableAreaPartition::ProfitableAreaPartition()
{
}

Experiment::DebsChallenge::ProfitableAreaPartition::~ProfitableAreaPartition()
{
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

void Experiment::DebsChallenge::ProfitableAreaPartition::most_profitable_cell_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines)
{
	RoundRobinPartitioner* rrg;
	PkgPartitioner* pkg;
	HashFieldPartitioner* fld;
	CardinalityAwarePolicy ca_policy;
	CaPartitionLib::CA_Exact_Partitioner* ca_naive;
	LoadAwarePolicy la_policy;
	CaPartitionLib::CA_Exact_Partitioner* la_naive;
	Experiment::DebsChallenge::FrequentRoutePartition experiment;
	std::vector<uint16_t> tasks;

	// tasks: 10
	for (uint16_t i = 0; i < 10; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	most_profitable_partitioner_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	most_profitable_partitioner_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	most_profitable_partitioner_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	most_profitable_partitioner_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	most_profitable_partitioner_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
	// tasks: 100
	for (uint16_t i = 0; i < 100; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::cout << "# of Tasks: " << tasks.size() << ".\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, ca_policy);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, la_policy);
	most_profitable_partitioner_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	most_profitable_partitioner_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	most_profitable_partitioner_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	most_profitable_partitioner_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	most_profitable_partitioner_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void Experiment::DebsChallenge::ProfitableAreaPartition::most_profitable_partitioner_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::vector<uint16_t> tasks, Partitioner & partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix)
{
	// auxiliary variables
	Experiment::DebsChallenge::FrequentRoutePartition experiment;
	std::queue<Experiment::DebsChallenge::CompactRide> queue;
	std::mutex mu;
	std::condition_variable cond;
	// get maximum and minimum running times
	double min_first_round_duration = -1, max_first_round_duration = 0, sum_of_first_round_durations = 0;
	double min_second_round_duration = -1, max_second_round_duration = 0, sum_of_second_round_durations = 0;
	// intermediate buffers
	std::unordered_map<std::string, std::vector<float>> complete_fare_table;
	std::unordered_map<std::string, std::pair<std::string, std::time_t>> dropoff_table;
	std::vector<std::vector<std::pair<std::string, std::vector<float>>>> complete_fare_sub_table;
	std::vector<std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>> dropoff_cell_sub_table;
	std::unordered_map<std::string, std::pair<float, int>> cell_partial_result_buffer;
	std::unordered_map<std::string, float> cell_full_result_buffer;
	// Partition Data - Key: Medallion
	std::ofstream** out_file;
	out_file = new std::ofstream*[tasks.size()];
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i] = new std::ofstream(partitioner_name + "_" + std::to_string(i) + ".csv");
	}
	for (auto it = rides.cbegin(); it != rides.cend(); ++it)
	{
		std::string key = it->medallion;
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		*out_file[task] << it->to_string() << "\n";
	}
	// write out files and clean up memory
	for (size_t i = 0; i < tasks.size(); i++)
	{
		out_file[i]->flush();
		out_file[i]->close();
		delete out_file[i];
	}
	delete[] out_file;

	// first parallel round of processing (for each pickup cell gather full-fares, for each medallion keep track of the latest dropoff-cell)
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<Experiment::DebsChallenge::CompactRide>* task_lines = new std::vector<Experiment::DebsChallenge::CompactRide>();
		std::string workload_file_name = partitioner_name + "_" + std::to_string(i) + ".csv";
		experiment.parse_debs_rides_with_to_string(workload_file_name, task_lines);
		Experiment::DebsChallenge::ProfitableArea worker(&queue, &mu, &cond);

		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - START
		for (auto it = task_lines->begin(); it != task_lines->end(); ++it)
		{
			worker.update(*it);
		}
		if (partitioner_name.compare("fld") != 0)
		{
			worker.first_round_aggregation(complete_fare_table, dropoff_table, true);
		}
		else
		{
			worker.first_round_aggregation(complete_fare_table, dropoff_table, false);
		}
		// TIME CRITICAL CODE - END
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();

		std::chrono::duration<double, std::milli> execution_time = end - start;

		sum_of_first_round_durations += execution_time.count();
		if (max_first_round_duration < execution_time.count())
		{
			max_first_round_duration = execution_time.count();
		}
		min_first_round_duration = i == 0 ? execution_time.count() : (min_first_round_duration > execution_time.count() ? execution_time.count() : min_first_round_duration);
		task_lines->clear();
		delete task_lines;
		std::remove(workload_file_name.c_str());
	}
	// re-initialize the partitioners
	partitioner.init();
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		complete_fare_sub_table.push_back(std::vector<std::pair<std::string, std::vector<float>>>());
		dropoff_cell_sub_table.push_back(std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>());
	}
	// 2nd - round
	// partition fare_map
	for (auto it = complete_fare_table.begin(); it != complete_fare_table.end(); ++it)
	{
		std::string key = it->first;
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		complete_fare_sub_table[task].push_back(std::make_pair(it->first, it->second));
	}
	complete_fare_sub_table.shrink_to_fit();
	complete_fare_table.clear();
	// partition dropoff_cells
	for (auto it = dropoff_table.begin(); it != dropoff_table.end(); ++it)
	{
		std::string key = it->second.first;
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		dropoff_cell_sub_table[task].push_back(std::make_pair(it->first, it->second));
	}
	dropoff_cell_sub_table.shrink_to_fit();
	dropoff_table.clear();
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		// first process the updates
		Experiment::DebsChallenge::ProfitableArea worker(&queue, &mu, &cond);
		worker.second_round_init();
		// TIME CRITICAL CODE - START
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = complete_fare_sub_table[i].begin(); it != complete_fare_sub_table[i].end(); ++it)
		{
			worker.second_round_update(it->first, it->second);
		}
		for (auto it = dropoff_cell_sub_table[i].begin(); it != dropoff_cell_sub_table[i].end(); ++it)
		{
			worker.second_round_update(it->first, it->second.first, it->second.second);
		}
		if (partitioner_name.compare("fld") != 0)
		{
			worker.partial_finalize(cell_partial_result_buffer);
		}
		else
		{
			worker.finalize(cell_full_result_buffer);
		}
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - END
		complete_fare_sub_table[i].clear();
		dropoff_cell_sub_table[i].clear();
		std::chrono::duration<double, std::milli> execution_time = end - start;
		sum_of_second_round_durations += execution_time.count();
		if (max_second_round_duration < execution_time.count())
		{
			max_second_round_duration = execution_time.count();
		}
		min_second_round_duration = i == 0 ? execution_time.count() : (min_second_round_duration > execution_time.count() ? execution_time.count() : min_second_round_duration);
	}
	complete_fare_sub_table.clear();
	dropoff_cell_sub_table.clear();
	std::cout << "cell_full_result_buffer size(): " << cell_full_result_buffer.size() << ", cell_partial_result_buffer size(): " << cell_partial_result_buffer.size() << ".\n";
	// final aggregation
	Experiment::DebsChallenge::ProfitableAreaOfflineAggregator aggregator;
	// TIME CRITICAL CODE - START
	std::chrono::system_clock::time_point aggr_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_sort_final_aggregation(cell_partial_result_buffer, partitioner_name + "_debs_most_profit_result.csv");
	}
	else
	{
		aggregator.sort_final_aggregation(cell_full_result_buffer, partitioner_name + "_debs_most_profit_result.csv");
	}
	std::chrono::system_clock::time_point aggr_end = std::chrono::system_clock::now();
	// TIME CRITICAL CODE - END
	std::chrono::duration<double, std::milli> aggr_time = aggr_end - aggr_start;
	std::cout << partitioner_name << " :: Step 1 durations: (Min: " << min_first_round_duration << ", Max: " <<
		max_first_round_duration << ", MEAN: " << sum_of_first_round_durations / tasks.size() <<
		") (msec). Step 2 durations: (Min: " << min_second_round_duration << ", Max: " <<
		max_second_round_duration << ", MEAN: " << sum_of_second_round_durations / tasks.size() <<
		") (msec). Final Aggregation Time: " << aggr_time.count() << " (msec).\n";
}

void Experiment::DebsChallenge::ProfitableAreaPartition::debs_profitable_area_worker(Experiment::DebsChallenge::ProfitableArea* profitable_area)
{
	profitable_area->operate();
}