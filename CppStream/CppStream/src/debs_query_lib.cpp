#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // DEBS_QUERY_LIB_H_

Experiment::DebsChallenge::FrequentRouteWorkerThread::FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, 
	std::mutex* aggr_mu, std::condition_variable* aggr_cond, std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, 
	std::mutex* mu, std::condition_variable* cond)
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

Experiment::DebsChallenge::FrequentRouteWorkerThread::FrequentRouteWorkerThread(std::queue<Experiment::DebsChallenge::frequent_route>* aggregator_queue, 
	std::mutex * aggr_mu, std::condition_variable * aggr_cond, std::queue<Experiment::DebsChallenge::CompactRide>* input_queue, std::mutex * mu, 
	std::condition_variable * cond, const std::string result_output_file_name)
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
	std::vector<std::pair<unsigned long, std::string>>& result)
{
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
	for (std::map<unsigned long, std::vector<std::string>>::const_iterator c = final_result.cbegin(); c != final_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			result.push_back(std::make_pair(c->first, *i));
		}
	}
	final_result.clear();
}

void Experiment::DebsChallenge::FrequentRouteOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::DebsChallenge::frequent_route>& partial_aggregates,
	std::vector<std::pair<unsigned long, std::string>>& result)
{
	for (std::vector<frequent_route>::const_iterator cit = partial_aggregates.cbegin(); cit != partial_aggregates.cend(); ++cit)
	{
		auto it = this->result.find(cit->route);
		if (it != this->result.end())
		{
			it->second += cit->count;
		}
		else
		{
			this->result.insert(std::make_pair(cit->route, cit->count));
		}
	}
	std::map<unsigned long, std::vector<std::string>> final_result;
	for (auto cit = this->result.begin(); cit != this->result.end(); ++cit)
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
	for (std::map<unsigned long, std::vector<std::string>>::const_iterator c = final_result.cbegin(); c != final_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			result.push_back(std::make_pair(c->first, *i));
		}
	}
	final_result.clear();
}

void Experiment::DebsChallenge::FrequentRouteOfflineAggregator::write_output_to_file(const std::vector<std::pair<unsigned long, std::string>>& result, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	for (auto it = result.cbegin(); it != result.cend(); ++it)
	{
		std::string buffer = std::to_string(it->first) + "," + it->second + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

Experiment::DebsChallenge::FrequentRoutePartition::FrequentRoutePartition()
{
}

Experiment::DebsChallenge::FrequentRoutePartition::~FrequentRoutePartition()
{
}

void Experiment::DebsChallenge::FrequentRoutePartition::produce_compact_ride_file(const std::string& input_file_name, const std::string& output_file_name, 
	uint32_t cell_side_size, uint32_t grid_side_size_in_cells)
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

//void Experiment::DebsChallenge::FrequentRoutePartition::debs_partition_performance(const std::vector<uint16_t>& tasks, Partitioner& partitioner,
//	const std::string partioner_name, std::vector<Experiment::DebsChallenge::CompactRide>& rides)
//{
//	std::vector<std::unordered_set<std::string>> key_per_task;
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		key_per_task.push_back(std::unordered_set<std::string>());
//	}
//	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
//	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = rides.begin(); it != rides.end(); ++it)
//	{
//		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
//		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
//		std::string key = pickup_cell + "-" + dropoff_cell;
//		short task = partitioner.partition_next(key.c_str(), key.length());
//		key_per_task[task].insert(key);
//	}
//	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
//	std::chrono::duration<double, std::milli> partition_time = part_end - part_start;
//	size_t min_cardinality = std::numeric_limits<unsigned long>::max();
//	size_t max_cardinality = std::numeric_limits<unsigned long>::min();
//	double average_cardinality = 0;
//	std::cout << "Cardinalities: ";
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		if (min_cardinality > key_per_task[i].size())
//		{
//			min_cardinality = key_per_task[i].size();
//		}
//		if (max_cardinality < key_per_task[i].size())
//		{
//			max_cardinality = key_per_task[i].size();
//		}
//		average_cardinality += key_per_task[i].size();
//		std::cout << key_per_task[i].size() << " ";
//		key_per_task[i].clear();
//	}
//	std::cout << "\n";
//	average_cardinality = average_cardinality / tasks.size();
//	key_per_task.clear();
//	std::cout << "Time partition using " << partioner_name << ": " << partition_time.count() << " (msec). Min: " << min_cardinality <<
//		", Max: " << max_cardinality << ", AVG: " << average_cardinality << "\n";
//}

//double Experiment::DebsChallenge::FrequentRoutePartition::debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table,
//	Partitioner& partitioner, const std::string partitioner_name, const size_t max_queue_size)
//{
//	// initialize shared memory
//	queues = new std::queue<Experiment::DebsChallenge::CompactRide>*[tasks.size()];
//	mu_xes = new std::mutex[tasks.size()];
//	cond_vars = new std::condition_variable[tasks.size()];
//	threads = new std::thread*[tasks.size()];
//	query_workers = new Experiment::DebsChallenge::FrequentRouteWorkerThread*[tasks.size()];
//	this->max_queue_size = max_queue_size;
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		queues[i] = new std::queue<Experiment::DebsChallenge::CompactRide>();
//		query_workers[i] = new Experiment::DebsChallenge::FrequentRouteWorkerThread(nullptr, nullptr, nullptr, queues[i], &mu_xes[i], &cond_vars[i]);
//		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
//	}
//	//std::cout << partitioner_name << " thread INITIATES partitioning (frequent-route).\n";
//	// start partitioning
//	std::chrono::system_clock::time_point partition_start = std::chrono::system_clock::now();
//	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = route_table.cbegin(); it != route_table.cend(); ++it)
//	{
//		std::string key = std::to_string(it->pickup_cell.first) + "." +
//			std::to_string(it->pickup_cell.second) + "-" +
//			std::to_string(it->dropoff_cell.first) + "." +
//			std::to_string(it->dropoff_cell.second);
//		short task = partitioner.partition_next(key.c_str(), key.length());
//		std::unique_lock<std::mutex> locker(mu_xes[task]);
//		cond_vars[task].wait(locker, [this, task, max_queue_size]() { return queues[task]->size() < max_queue_size; });
//		queues[task]->push(*it);
//		locker.unlock();
//		cond_vars[task].notify_all();
//	}
//	//std::cout << partitioner_name << " thread SENT all rides (frequent-route).\n";
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		Experiment::DebsChallenge::CompactRide final_ride;
//		final_ride.trip_distance = -1;
//		std::unique_lock<std::mutex> locker(mu_xes[i]);
//		cond_vars[i].wait(locker, [this, i, max_queue_size]() { return queues[i]->size() < max_queue_size; });
//		queues[i]->push(final_ride);
//		locker.unlock();
//		cond_vars[i].notify_all();
//	}
//	// wait for workers to join
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		threads[i]->join();
//	}
//	std::chrono::system_clock::time_point partition_end = std::chrono::system_clock::now();
//	std::chrono::duration<double, std::milli> partition_time = partition_end - partition_start;
//	//std::cout << partitioner_name << " total partition time: " << partition_time.count() << " (msec) (frequent-route).\n";
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		delete threads[i];
//		delete query_workers[i];
//		delete queues[i];
//	}
//	delete[] threads;
//	delete[] query_workers;
//	delete[] queues;
//	delete[] mu_xes;
//	delete[] cond_vars;
//	//std::cout << "------END-----\n";
//	return partition_time.count();
//}

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* lines, const size_t task_number)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks(task_number, 0);

	Partitioner* rrg;
	Partitioner* fld;
	Partitioner* pkg;
	Partitioner* ca_naive;
	Partitioner* ca_aff_naive;
	Partitioner* ca_hll;
	Partitioner* ca_aff_hll;
	Partitioner* la_naive;
	Partitioner* la_hll;
	
	for (uint16_t i = 0; i < task_number; i++)
	{
		tasks[i] = i;
	}
	tasks.shrink_to_fit();

	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &ca_policy);
	ca_aff_naive = new CaPartitionLib::CA_Exact_Aff_Partitioner(tasks);
	ca_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &ca_policy, 12);
	ca_aff_hll = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 12);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &la_policy);
	la_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &la_policy, 12);

	std::string sh_file_name = "sh_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string fld_file_name = "fld_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string pkg_file_name = "pk_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_naive_file_name = "ca_naive_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_hll_file_name = "ca_hll_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_naive_file_name = "la_naive_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_hll_file_name = "la_hll_debs_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::cout << "DEBS Q1***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-number,min-exec-msec,max-exec-msec,avg-exec-msec,avg-aggr-msec,io-msec,avg-part-msec,imb,key-imb\n";
	std::cout << info_stream.str();
	frequent_route_partitioner_simulation(lines, tasks, rrg, "sh", sh_file_name);
	frequent_route_partitioner_simulation(lines, tasks, fld, "fld", fld_file_name);
	frequent_route_partitioner_simulation(lines, tasks, pkg, "pk", pkg_file_name);
	frequent_route_partitioner_simulation(lines, tasks, ca_naive, "ca_naive", ca_naive_file_name);
	frequent_route_partitioner_simulation(lines, tasks, ca_aff_naive, "ca_aff_naive", ca_aff_naive_file_name);
	frequent_route_partitioner_simulation(lines, tasks, ca_hll, "ca_hll", ca_hll_file_name);
	frequent_route_partitioner_simulation(lines, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
	frequent_route_partitioner_simulation(lines, tasks, la_naive, "la_naive", la_naive_file_name);
	frequent_route_partitioner_simulation(lines, tasks, la_hll, "la_hll", la_hll_file_name);

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete ca_aff_naive;
	delete ca_hll;
	delete ca_aff_hll;
	delete la_naive;
	delete la_hll;
	tasks.clear();

	std::remove(sh_file_name.c_str());
	std::remove(fld_file_name.c_str());
	std::remove(pkg_file_name.c_str());
	std::remove(ca_naive_file_name.c_str());
	std::remove(ca_aff_naive_file_name.c_str());
	std::remove(ca_hll_file_name.c_str());
	std::remove(ca_aff_hll_file_name.c_str());
	std::remove(la_naive_file_name.c_str());
	std::remove(la_hll_file_name.c_str());
}

void Experiment::DebsChallenge::FrequentRoutePartition::partition_thread_operate(std::string partitioner_name, Partitioner* partitioner, 
	std::vector<Experiment::DebsChallenge::CompactRide>* rides,
	size_t task_number, float* imbalance, float* key_imbalance, double* duration)
{
	std::chrono::duration<double, std::milli> total_duration;
	DebsFrequentRideKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::DebsChallenge::CompactRide, std::string> imbalance_aggregator(task_number, key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = rides->cbegin(); it != rides->cend(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		uint16_t task = p_copy->partition_next(&key, key.length());
		imbalance_aggregator.incremental_measure_score_tuple_count(task, *it);
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	total_duration = (part_end - part_start);
	*duration = total_duration.count();
	*imbalance = imbalance_aggregator.imbalance();
	*key_imbalance = imbalance_aggregator.cardinality_imbalance();
	delete p_copy;
}

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_partitioner_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* rides, const std::vector<uint16_t> tasks, 
	Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	float imbalance[7], key_imbalance[7];
	std::thread** threads;
	double part_durations[7];
	std::vector<double> partition_duration_vector;
	std::vector<double> exec_durations(tasks.size(), double(0));
	std::vector<double> aggr_durations;
	double aggr_duration, write_output_duration;
	std::vector<Experiment::DebsChallenge::frequent_route> partial_result;
	std::vector<std::vector<Experiment::DebsChallenge::CompactRide>> worker_input_buffer(tasks.size(), 
		std::vector<Experiment::DebsChallenge::CompactRide>());
	std::queue<Experiment::DebsChallenge::CompactRide> queue;
	std::mutex mu;
	std::condition_variable cond;
	// partition tuples - use 6 auxiliary threads and this main thread to scan buffer
	threads = new std::thread*[7];
	for (size_t part_run = 1; part_run < 7; ++part_run)
	{
		threads[part_run] = new std::thread(Experiment::DebsChallenge::FrequentRoutePartition::partition_thread_operate,
			std::string(partitioner_name), partitioner, rides, tasks.size(), &imbalance[part_run], &key_imbalance[part_run],
			&part_durations[part_run]);
	}
	std::chrono::duration<double, std::milli> duration;
	DebsFrequentRideKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::DebsChallenge::CompactRide, std::string> imbalance_aggregator(tasks.size(), key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = rides->cbegin(); it != rides->cend(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "-" + dropoff_cell;
		uint16_t task = p_copy->partition_next(&key, key.length());
		imbalance_aggregator.incremental_measure_score(task, *it);
		worker_input_buffer[task].push_back(*it);
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	duration = (part_end - part_start);
	delete p_copy;
	part_durations[0] = duration.count();
	imbalance[0] = imbalance_aggregator.imbalance();
	key_imbalance[0] = imbalance_aggregator.cardinality_imbalance();
	for (size_t part_run_thread = 1; part_run_thread < 7; ++part_run_thread)
	{
		threads[part_run_thread]->join();
	}
	for (size_t part_run = 1; part_run < 7; part_run++)
	{
		delete threads[part_run];
	}
	delete[] threads;
	for (size_t i = 0; i < 7; i++)
	{
		partition_duration_vector.push_back(part_durations[i]);
	}
	for (auto it = worker_input_buffer.begin(); it != worker_input_buffer.end(); ++it)
	{
		it->shrink_to_fit();
	}
	worker_input_buffer.shrink_to_fit();
	
	// for every task - calculate (partial) workload
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> task_durations;
		for (size_t run = 0; run < 7; ++run)
		{
			std::vector<Experiment::DebsChallenge::frequent_route> p;
			// feed the worker
			Experiment::DebsChallenge::FrequentRouteWorkerThread worker(nullptr, nullptr, nullptr, &queue, &mu, &cond,
				worker_output_file_name);
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			// TIME CRITICAL CODE - START
			for (auto it = worker_input_buffer[i].begin(); it != worker_input_buffer[i].end(); ++it)
			{
				worker.update(*it);
			}
			worker.partial_finalize(p);
			if (run >= 6)
			{
				partial_result.reserve(partial_result.size() + p.size());
				std::move(p.begin(), p.end(), std::inserter(partial_result, partial_result.end()));
			}
			// TIME CRITICAL CODE - END
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			std::chrono::duration<double, std::milli> execution_time = end - start;
			task_durations.push_back(execution_time.count());
			p.clear();
		}
		worker_input_buffer[i].clear();
		auto max_it = std::max_element(task_durations.begin(), task_durations.end());
		task_durations.erase(max_it);
		auto min_it = std::min_element(task_durations.begin(), task_durations.end());
		task_durations.erase(min_it);
		double average_exec_duration = std::accumulate(task_durations.begin(), task_durations.end(), 0.0) / task_durations.size();
		exec_durations[i] = average_exec_duration;
	}
	worker_input_buffer.clear();
	for (size_t aggr_run = 0; aggr_run < 7; ++aggr_run)
	{
		Experiment::DebsChallenge::FrequentRouteOfflineAggregator aggregator;
		std::vector<std::pair<unsigned long, std::string>> result;
		// TIME CRITICAL CODE - START
		std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") != 0)
		{
			aggregator.calculate_and_sort_final_aggregation(partial_result, result);
		}
		else
		{
			aggregator.sort_final_aggregation(partial_result, result);
		}
		std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - END
		std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;
		aggr_durations.push_back(aggregation_time.count());
		std::chrono::system_clock::time_point output_write_start = std::chrono::system_clock::now();
		if (aggr_run >= 6)
		{
			aggregator.write_output_to_file(result, worker_output_file_name);
		}
		std::chrono::system_clock::time_point output_write_end = std::chrono::system_clock::now();
		write_output_duration = (output_write_end - output_write_start).count();
	}
	auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(max_it);
	auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(min_it);
	aggr_duration = std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) / aggr_durations.size();

	auto part_max_it = std::max_element(partition_duration_vector.begin(), partition_duration_vector.end());
	partition_duration_vector.erase(part_max_it);
	auto part_min_it = std::min_element(partition_duration_vector.begin(), partition_duration_vector.end());
	partition_duration_vector.erase(part_min_it);
	float mean_part_time = std::accumulate(partition_duration_vector.begin(), partition_duration_vector.end(), 0.0) / partition_duration_vector.size();

	std::stringstream result_stream;
	result_stream << partitioner_name << "," << tasks.size() << "," << *std::min_element(exec_durations.begin(), exec_durations.end()) << "," <<
		*std::max_element(exec_durations.begin(), exec_durations.end()) << "," << (std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) / exec_durations.size()) << "," <<
		aggr_duration << "," << write_output_duration << "," << mean_part_time << "," << imbalance[0] << "," << key_imbalance[0] << "\n";
	std::cout << result_stream.str();
	partial_result.clear();
	exec_durations.clear();
	aggr_durations.clear();
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRouteWorkerThread* frequent_route)
{
	frequent_route->operate();
}

Experiment::DebsChallenge::ProfitableArea::ProfitableArea()
{
	mu = nullptr;
	cond = nullptr;
	input_queue = nullptr;
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

void Experiment::DebsChallenge::ProfitableArea::update(const DebsChallenge::CompactRide & ride)
{
	float total = ride.fare_amount + ride.tip_amount;
	std::string pickup_cell = std::to_string(ride.pickup_cell.first) + "." +
		std::to_string(ride.pickup_cell.second);
	std::string dropoff_cell = std::to_string(ride.dropoff_cell.first) + "." + 
		std::to_string(ride.dropoff_cell.second);
	std::string medal = std::string(ride.medallion, sizeof(ride.medallion) / sizeof(ride.medallion[0]));
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
	auto med_it = this->dropoff_table.find(medal);
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
		dropoff_table.insert(std::make_pair(medal, std::make_pair(dropoff_cell, ride.dropoff_datetime)));
	}
}

void Experiment::DebsChallenge::ProfitableArea::first_round_aggregation(std::unordered_map<std::string, std::vector<float>>& complete_fare_map, 
	std::unordered_map<std::string, std::vector<float>>& complete_fare_map_out, 
	std::unordered_map<std::string, std::pair<std::string, std::time_t>>& complete_dropoff_table, 
	std::unordered_map<std::string, std::pair<std::string, std::time_t>>& complete_dropoff_table_out, const bool two_choice_partition_used, const bool write)
{
	std::unordered_map<std::string, std::vector<float>> complete_fare_map_aux;
	std::unordered_map<std::string, std::pair<std::string, std::time_t>> complete_dropoff_table_aux;
	for (auto it = fare_map.cbegin(); it != fare_map.cend(); ++it)
	{
		auto c_fm_it = complete_fare_map.find(it->first);
		auto c_fm_it_aux = complete_fare_map_aux.find(it->first);
		if ((c_fm_it != complete_fare_map.end() && c_fm_it_aux != complete_fare_map_aux.end()) ||
			(c_fm_it == complete_fare_map.end() && c_fm_it_aux != complete_fare_map_aux.end()))
		{
			// just update the value in the auxiliary table
			c_fm_it_aux->second.reserve(c_fm_it_aux->second.size() + it->second.size());
			std::move(it->second.begin(), it->second.end(), std::inserter(c_fm_it_aux->second, c_fm_it_aux->second.end()));
		}
		else if (c_fm_it != complete_fare_map.end() && c_fm_it_aux == complete_fare_map_aux.end())
		{
			// create a record in the auxiliary table and update the value
			complete_fare_map_aux[it->first] = c_fm_it->second;
			c_fm_it_aux = complete_fare_map_aux.find(it->first);
			c_fm_it_aux->second.reserve(c_fm_it_aux->second.size() + it->second.size());
			std::move(it->second.begin(), it->second.end(), std::inserter(c_fm_it_aux->second, c_fm_it_aux->second.end()));
		}
		else
		{
			complete_fare_map_aux[it->first] = it->second;
		}
	}
	for (auto it = complete_fare_map.cbegin(); it != complete_fare_map.cend(); ++it)
	{
		auto aux_it = complete_fare_map_aux.find(it->first);
		if (aux_it == complete_fare_map_aux.end())
		{
			complete_fare_map_aux[it->first] = it->second;
		}
	}
	if (two_choice_partition_used)
	{
		for (auto it = dropoff_table.cbegin(); it != dropoff_table.cend(); ++it)
		{
			auto med_it = complete_dropoff_table.find(it->first);
			auto med_it_aux = complete_dropoff_table_aux.find(it->first);
			if (med_it != complete_dropoff_table.end() && med_it_aux == complete_dropoff_table_aux.end())
			{
				if (med_it->second.second < it->second.second && med_it->second.first.compare(it->second.first) != 0)
				{
					complete_dropoff_table_aux[it->first] = std::make_pair(it->second.first, it->second.second);
				}
				else
				{
					complete_dropoff_table_aux[it->first] = std::make_pair(med_it->second.first, med_it->second.second);
				}
			}
			else if (med_it == complete_dropoff_table.end() && med_it_aux == complete_dropoff_table_aux.end())
			{
				complete_dropoff_table_aux[it->first] = std::make_pair(it->second.first, it->second.second);
			}
			else if ((med_it == complete_dropoff_table.end() && med_it_aux != complete_dropoff_table_aux.end()) ||
				(med_it != complete_dropoff_table.end() && med_it_aux != complete_dropoff_table_aux.end()))
			{
				if (med_it_aux->second.second < it->second.second && med_it_aux->second.first.compare(it->second.first) != 0)
				{
					med_it_aux->second.first = it->second.first;
					med_it_aux->second.second = it->second.second;
				}
			}
		}
		for (auto it = complete_dropoff_table.cbegin(); it != complete_dropoff_table.cend(); ++it)
		{
			auto aux_it = complete_dropoff_table_aux.find(it->first);
			if (aux_it == complete_dropoff_table_aux.end())
			{
				complete_dropoff_table_aux[it->first] = it->second;
			}
		}
	}
	else
	{
		for (auto it = dropoff_table.cbegin(); it != dropoff_table.cend(); ++it)
		{
			complete_dropoff_table_aux.insert(std::make_pair(it->first, it->second));
		}
		// i need to add the values in the argument dropoff-table
		for (auto it = complete_dropoff_table.cbegin(); it != complete_dropoff_table.cend(); ++it)
		{
			complete_dropoff_table_aux.insert(std::make_pair(it->first, it->second));
		}
	}
	if (write == true)
	{
		complete_dropoff_table_out.clear();
		complete_dropoff_table_out.insert(complete_dropoff_table_aux.cbegin(), complete_dropoff_table_aux.cend());
		complete_fare_map_out.clear();
		complete_fare_map_out.insert(complete_fare_map_aux.cbegin(), complete_fare_map_aux.cend());
	}
}

void Experiment::DebsChallenge::ProfitableArea::second_round_init()
{
	this->pickup_cell_median_fare.clear();
	this->dropoff_cell_empty_taxi_count.clear();
}

void Experiment::DebsChallenge::ProfitableArea::second_round_update(const std::string & pickup_cell, const std::vector<float>& fare_list)
{
	// calculate median
	float median_fare = 0.0;
	std::vector<float> fare_list_copy(fare_list);
	if (fare_list.size() > 0)
	{
		if (fare_list.size() % 2 != 0)
		{
			std::nth_element(fare_list_copy.begin(), fare_list_copy.begin() + fare_list_copy.size() / 2, fare_list_copy.end());
			median_fare = fare_list_copy[fare_list_copy.size() / 2];
		}
		else
		{
			std::sort(fare_list_copy.begin(), fare_list_copy.end());
			size_t index_one = (fare_list_copy.size() / 2) - 1;
			size_t index_two = 1 + index_one;
			median_fare = (fare_list_copy[index_one] + fare_list_copy[index_two]) / 2;
		}
		auto it = pickup_cell_median_fare.find(pickup_cell);
		if (it != pickup_cell_median_fare.end())
		{
			// this is probably a mistake since each pickup-cell has a single list of fare lists
			// it->second = median_fare;
			std::cerr << "second_round_update() identified duplicate pickup-cell record. the cell-id: " << pickup_cell <<
				" was encountered twice. current median value found: " << it->second <<
				", new value: " << median_fare << ".\n";
		}
		else
		{
			pickup_cell_median_fare.insert(std::make_pair(pickup_cell, median_fare));
		}
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
				cell_profit_buffer[dropoff_cell_it->first] = std::make_pair(dropoff_cell_it->second, pickup_cell_it->second);
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

void Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::sort_final_aggregation(const std::unordered_map<std::string, float>& cell_profit_buffer, 
	std::vector<std::pair<float, std::string>>& final_result)
{
	std::map<float, std::vector<std::string>> ordered_result;
	for (std::unordered_map<std::string, float>::const_iterator cit = cell_profit_buffer.cbegin(); cit != cell_profit_buffer.cend(); ++cit)
	{
		auto it = ordered_result.find(cit->second);
		if (it != ordered_result.end())
		{
			it->second.push_back(cit->first);
		}
		else
		{
			std::vector<std::string> tmp;
			tmp.push_back(cit->first);
			ordered_result[cit->second] = tmp;
		}
	}
	for (std::map<float, std::vector<std::string>>::const_iterator c = ordered_result.cbegin(); c != ordered_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			final_result.push_back(std::make_pair(c->first, *i));
		}
	}
	ordered_result.clear();
}

void Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::calculate_and_sort_final_aggregation(const std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer, 
	std::vector<std::pair<float, std::string>>& final_result)
{
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
	std::map<float, std::vector<std::string>> ordered_result;
	for (auto cit = cell_profit_final_buffer.begin(); cit != cell_profit_final_buffer.end(); ++cit)
	{
		float final_profit = cit->second.first / cit->second.second;
		auto it = ordered_result.find(final_profit);
		if (it != ordered_result.end())
		{
			it->second.push_back(cit->first);
		}
		else
		{
			std::vector<std::string> tmp;
			tmp.push_back(cit->first);
			ordered_result[final_profit] = tmp;
		}
	}
	for (std::map<float, std::vector<std::string>>::const_iterator c = ordered_result.cbegin(); c != ordered_result.cend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			final_result.push_back(std::make_pair(c->first, *i));
		}
	}
	ordered_result.clear();
}

void Experiment::DebsChallenge::ProfitableAreaOfflineAggregator::output_result_to_file(const std::vector<std::pair<float, std::string>>& final_result, const std::string & out_file)
{
	FILE* fd;
	fd = fopen(out_file.c_str(), "w");
	for (auto it = final_result.cbegin(); it != final_result.cend(); ++it)
	{
		std::string buffer = std::to_string(it->first) + "," + it->second + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

Experiment::DebsChallenge::ProfitableAreaPartition::ProfitableAreaPartition()
{
}

Experiment::DebsChallenge::ProfitableAreaPartition::~ProfitableAreaPartition()
{
}

//double Experiment::DebsChallenge::ProfitableAreaPartition::debs_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::CompactRide>& route_table,
//	Partitioner& partitioner, const std::string partitioner_name, const size_t max_queue_size)
//{
//	// initialize shared memory
//	queues = new std::queue<Experiment::DebsChallenge::CompactRide>*[tasks.size()];
//	mu_xes = new std::mutex[tasks.size()];
//	cond_vars = new std::condition_variable[tasks.size()];
//	threads = new std::thread*[tasks.size()];
//	query_workers = new Experiment::DebsChallenge::ProfitableArea*[tasks.size()];
//	this->max_queue_size = max_queue_size;
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		queues[i] = new std::queue<Experiment::DebsChallenge::CompactRide>();
//		query_workers[i] = new Experiment::DebsChallenge::ProfitableArea(queues[i], &mu_xes[i], &cond_vars[i]);
//		threads[i] = new std::thread(debs_profitable_area_worker, query_workers[i]);
//	}
//	//std::cout << partitioner_name << " thread INITIATES partitioning (profit-areas).\n";
//	// start partitioning
//	std::chrono::system_clock::time_point partition_start = std::chrono::system_clock::now();
//	for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = route_table.cbegin(); it != route_table.cend(); ++it)
//	{
//		std::string key = it->medallion;
//		short task = partitioner.partition_next(key.c_str(), key.length());
//		std::unique_lock<std::mutex> locker(mu_xes[task]);
//		cond_vars[task].wait(locker, [this, task, max_queue_size]() { return queues[task]->size() < max_queue_size; });
//		queues[task]->push(*it);
//		locker.unlock();
//		cond_vars[task].notify_all();
//	}
//	//std::cout << partitioner_name << " thread SENT all rides (profit-areas).\n";
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		Experiment::DebsChallenge::CompactRide final_ride;
//		final_ride.trip_distance = -1;
//		std::unique_lock<std::mutex> locker(mu_xes[i]);
//		cond_vars[i].wait(locker, [this, i, max_queue_size]() { return queues[i]->size() < max_queue_size; });
//		queues[i]->push(final_ride);
//		locker.unlock();
//		cond_vars[i].notify_all();
//	}
//	// wait for workers to join
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		threads[i]->join();
//	}
//	std::chrono::system_clock::time_point partition_end = std::chrono::system_clock::now();
//	std::chrono::duration<double, std::milli> partition_time = partition_end - partition_start;
//	//std::cout << partitioner_name << " total partition time: " << partition_time.count() << " (msec) (profit-areas).\n";
//	for (size_t i = 0; i < tasks.size(); ++i)
//	{
//		delete threads[i];
//		delete query_workers[i];
//		delete queues[i];
//	}
//	delete[] threads;
//	delete[] query_workers;
//	delete[] queues;
//	delete[] mu_xes;
//	delete[] cond_vars;
//	//std::cout << "------END-----\n";
//	return partition_time.count();
//}

void Experiment::DebsChallenge::ProfitableAreaPartition::most_profitable_cell_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* lines, const size_t task_number)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks;

	Partitioner* rrg;
	Partitioner* pkg;
	Partitioner* fld;
	Partitioner* ca_naive;
	Partitioner* ca_aff_naive;
	Partitioner* ca_hll;
	Partitioner* ca_aff_hll;
	Partitioner* la_naive;
	Partitioner* la_hll;

	Experiment::DebsChallenge::FrequentRoutePartition experiment;

	for (uint16_t i = 0; i < task_number; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();

	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &ca_policy);
	ca_aff_naive = new CaPartitionLib::CA_Exact_Aff_Partitioner(tasks);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &la_policy);
	ca_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &ca_policy, 12);
	ca_aff_hll = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 12);
	la_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &la_policy, 12);

	std::string sh_file_name = "sh_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string fld_file_name = "fld_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string pkg_file_name = "pk_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_naive_file_name = "ca_naive_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_hll_file_name = "ca_hll_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_naive_file_name = "la_naive_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_hll_file_name = "la_hll_debs_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::cout << "DEBS Q2 ***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-num,min-s1-msec,max-s1-msec,avg-s1-msec,min-s2-msec,max-s2-msec,avg-s2-msec,avg-aggr-msec,io-msec,avg-part-med-msec,avg-part-cell-msec," <<
		"med-imb,med-key-imb,dropoff-imb,dropoff-key-imb,fare-imb,fare-key-imb\n";
	std::cout << info_stream.str();
	most_profitable_partitioner_simulation(lines, tasks, rrg, "sh", sh_file_name);
	most_profitable_partitioner_simulation(lines, tasks, fld, "fld", fld_file_name);
	most_profitable_partitioner_simulation(lines, tasks, pkg, "pk", pkg_file_name);
	most_profitable_partitioner_simulation(lines, tasks, ca_naive, "ca_naive", ca_naive_file_name);
	most_profitable_partitioner_simulation(lines, tasks, ca_aff_naive, "ca_aff_naive", ca_aff_naive_file_name);
	most_profitable_partitioner_simulation(lines, tasks, ca_hll, "ca_hll", ca_hll_file_name);
	most_profitable_partitioner_simulation(lines, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
	most_profitable_partitioner_simulation(lines, tasks, la_naive, "la_naive", la_naive_file_name);
	most_profitable_partitioner_simulation(lines, tasks, la_hll, "la_hll", la_hll_file_name);

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete ca_aff_naive;
	delete la_naive;
	delete ca_hll;
	delete ca_aff_hll;
	delete la_hll;
	tasks.clear();

	std::remove(sh_file_name.c_str());
	std::remove(fld_file_name.c_str());
	std::remove(pkg_file_name.c_str());
	std::remove(ca_naive_file_name.c_str());
	std::remove(ca_aff_naive_file_name.c_str());
	std::remove(ca_hll_file_name.c_str());
	std::remove(ca_aff_hll_file_name.c_str());
	std::remove(la_naive_file_name.c_str());
	std::remove(la_hll_file_name.c_str());
}

void Experiment::DebsChallenge::ProfitableAreaPartition::partition_thread_operate_step_one(bool writer, std::string partitioner_name, Partitioner* partitioner, 
	std::vector<Experiment::DebsChallenge::CompactRide>* buffer, size_t task_number, float* imbalance, float* key_imbalance, double* duration, 
	std::vector<std::vector<CompactRide>>* worker_buffer)
{
	std::chrono::duration<double, std::milli> total_duration;
	DebsFrequentRideKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::DebsChallenge::CompactRide, std::string> imbalance_aggregator(task_number, key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	if (writer)
	{
		for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = buffer->cbegin(); it != buffer->cend(); ++it)
		{
			char med_buffer[33];
			memcpy(med_buffer, it->medallion, 32 * sizeof(char));
			med_buffer[32] = '\0';
			std::string key = med_buffer;
			uint16_t task = p_copy->partition_next(key.c_str(), key.length());
			imbalance_aggregator.incremental_measure_score(task, *it);
			(*worker_buffer)[task].push_back(*it);
		}
	}
	else
	{
		for (std::vector<Experiment::DebsChallenge::CompactRide>::const_iterator it = buffer->cbegin(); it != buffer->cend(); ++it)
		{
			char med_buffer[33];
			memcpy(med_buffer, it->medallion, 32 * sizeof(char));
			med_buffer[32] = '\0';
			std::string key = med_buffer;
			uint16_t task = p_copy->partition_next(key.c_str(), key.length());
			imbalance_aggregator.incremental_measure_score_tuple_count(task, *it);
		}
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	total_duration = (part_end - part_start);
	*duration = total_duration.count();
	*imbalance = imbalance_aggregator.imbalance();
	*key_imbalance = imbalance_aggregator.cardinality_imbalance();
	delete p_copy;
}

void Experiment::DebsChallenge::ProfitableAreaPartition::partition_thread_operate_step_two(bool writer, std::string partitioner_name, 
	Partitioner* partitioner, std::unordered_map<std::string, std::vector<float>>* fares,
	std::unordered_map<std::string, std::pair<std::string, std::time_t>>* dropoffs, size_t task_number, float* fare_imbalance, float* fare_key_imbalance,
	float* dropoff_imbalance, float* dropoff_key_imbalance, double* duration, std::vector<std::vector<std::pair<std::string, std::vector<float>>>>* fare_sub_table,
	std::vector<std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>>* dropoffs_sub_table)
{
	std::chrono::duration<double, std::milli> total_duration;
	DebsProfCellCompleteFareKeyExtractor complete_fare_key_extractor;
	DebsProfCellDropoffCellKeyExtractor dropoff_cell_key_extractor;
	ImbalanceScoreAggr<std::pair<std::string, std::vector<float>>, std::string> complete_fare_imb_aggregator(task_number, 
		complete_fare_key_extractor);
	ImbalanceScoreAggr<std::pair<std::string, std::pair<std::string, std::time_t>>, std::string> dropoff_cell_imb_aggregator(task_number, 
		dropoff_cell_key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	std::chrono::system_clock::time_point part_one_start = std::chrono::system_clock::now();
	if (writer == true)
	{
		for (auto it = fares->cbegin(); it != fares->cend(); ++it)
		{
			std::string key = it->first;
			uint16_t task = p_copy->partition_next(key.c_str(), key.length());
			complete_fare_imb_aggregator.incremental_measure_score(task, *it);
			(*fare_sub_table)[task].push_back(std::make_pair(it->first, it->second));
		}
	}
	else
	{
		for (auto it = fares->cbegin(); it != fares->cend(); ++it)
		{
			std::string key = it->first;
			uint16_t task = p_copy->partition_next(key.c_str(), key.length());
			complete_fare_imb_aggregator.incremental_measure_score_tuple_count(task, *it);
		}
	}
	std::chrono::system_clock::time_point part_one_end = std::chrono::system_clock::now();
	p_copy->init();
	std::chrono::system_clock::time_point part_two_start = std::chrono::system_clock::now();
	if (writer)
	{
		for (auto it = dropoffs->cbegin(); it != dropoffs->cend(); ++it)
		{
			std::string key = it->second.first;
			uint16_t task = p_copy->partition_next(key.c_str(), key.length());
			dropoff_cell_imb_aggregator.incremental_measure_score(task, *it);
			(*dropoffs_sub_table)[task].push_back(std::make_pair(it->first, it->second));
		}
	}
	else
	{
		for (auto it = dropoffs->cbegin(); it != dropoffs->cend(); ++it)
		{
			std::string key = it->second.first;
			uint16_t task = p_copy->partition_next(key.c_str(), key.length());
			dropoff_cell_imb_aggregator.incremental_measure_score_tuple_count(task, *it);
		}
	}
	std::chrono::system_clock::time_point part_two_end = std::chrono::system_clock::now();
	total_duration = std::chrono::duration<double, std::milli>((part_one_end - part_one_start) + (part_two_end - part_two_start));
	*duration = total_duration.count();
	*fare_imbalance = complete_fare_imb_aggregator.imbalance();
	*fare_key_imbalance = complete_fare_imb_aggregator.cardinality_imbalance();
	*dropoff_imbalance = dropoff_cell_imb_aggregator.imbalance();
	*dropoff_key_imbalance = dropoff_cell_imb_aggregator.imbalance();
	delete p_copy;
}


void Experiment::DebsChallenge::ProfitableAreaPartition::thread_execution_step_one(bool writer, const std::string partitioner_name, 
	const std::vector<Experiment::DebsChallenge::CompactRide>* input_buffer,
	std::unordered_map<std::string, std::vector<float>>* fare_table, std::unordered_map<std::string, std::vector<float>>* fare_table_out, 
	std::unordered_map<std::string, std::pair<std::string, time_t>>* dropoff_table, std::unordered_map<std::string, std::pair<std::string, time_t>>* dropoff_table_out, 
	double* total_duration)
{
	bool m_choice_partitioner = partitioner_name.compare("fld") != 0 ? true : false;
	Experiment::DebsChallenge::ProfitableArea worker;
	std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
	// TIME CRITICAL CODE - START
	for (auto it = input_buffer->cbegin(); it != input_buffer->cend(); ++it)
	{
		worker.update(*it);
	}
	worker.first_round_aggregation(*fare_table, *fare_table_out, *dropoff_table, *dropoff_table_out, m_choice_partitioner, writer);
	// TIME CRITICAL CODE - END
	std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> execution_time = end - start;
	*total_duration = execution_time.count();
}

void Experiment::DebsChallenge::ProfitableAreaPartition::most_profitable_partitioner_simulation(std::vector<Experiment::DebsChallenge::CompactRide>* rides, 
	const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	std::thread** threads;
	std::vector<double> part_medallion_durations, part_cell_durations;
	float med_imbalance[7], med_key_imbalance[7], dropoff_cell_imbalance[7], dropoff_cell_key_imbalance[7], 
		complete_fare_imbalance[7], complete_fare_key_imbalance[7];
	double med_part_durations[7], part_two_durations[7];
	// get maximum and minimum running times
	std::vector<double> first_round_exec_duration(tasks.size(), double(0));
	double step_one_durations[7];
	std::vector<double> second_round_exec_duration(tasks.size(), double(0));
	double final_aggr_duration, output_to_file_duration;
	// buffers
	std::vector<std::vector<CompactRide>> worker_input_buffer(tasks.size(), std::vector<CompactRide>());
	std::unordered_map<std::string, std::vector<float>> complete_fare_table;
	std::unordered_map<std::string, std::pair<std::string, std::time_t>> dropoff_table;
	std::vector<std::vector<std::pair<std::string, std::vector<float>>>> complete_fare_sub_table(tasks.size(), 
		std::vector<std::pair<std::string, std::vector<float>>>());
	std::vector<std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>> dropoff_cell_sub_table(tasks.size(), 
		std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>());
	std::unordered_map<std::string, std::pair<float, int>> cell_partial_result_buffer;
	std::unordered_map<std::string, float> cell_full_result_buffer;
	// Partition Data - Key: Medallion
	threads = new std::thread*[7];
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		if (part_run == 0)
		{
			threads[part_run] = new std::thread(Experiment::DebsChallenge::ProfitableAreaPartition::partition_thread_operate_step_one,
				true, std::string(partitioner_name), partitioner, rides, tasks.size(), &med_imbalance[part_run], &med_key_imbalance[part_run],
				&med_part_durations[part_run], &worker_input_buffer);
		}
		else
		{
			threads[part_run] = new std::thread(Experiment::DebsChallenge::ProfitableAreaPartition::partition_thread_operate_step_one,
				false, std::string(partitioner_name), partitioner, rides, tasks.size(), &med_imbalance[part_run], &med_key_imbalance[part_run],
				&med_part_durations[part_run], &worker_input_buffer);
		}
	}
	// start gathering results
	for (size_t part_run_thread = 0; part_run_thread < 7; ++part_run_thread)
	{
		threads[part_run_thread]->join();
		delete threads[part_run_thread];
		part_medallion_durations.push_back(med_part_durations[part_run_thread]);
	}
	delete[] threads;
	for (size_t i = 0; i < tasks.size(); i++)
	{
		worker_input_buffer[i].shrink_to_fit();
	}
	worker_input_buffer.shrink_to_fit();
	// first parallel round of processing (for each pickup cell gather full-fares, for each medallion keep track of the latest dropoff-cell)
	for (size_t task = 0; task < tasks.size(); ++task)
	{
		std::vector<double> durations;
		std::unordered_map<std::string, std::vector<float>> complete_fare_table_aux(complete_fare_table);
		std::unordered_map<std::string, std::pair<std::string, std::time_t>> dropoff_table_aux(dropoff_table);
		threads = new std::thread*[7];
		for (size_t run = 0; run < 7; ++run)
		{
			if (run == 0)
			{
				threads[run] = new std::thread(thread_execution_step_one, true, partitioner_name, &worker_input_buffer[task], &complete_fare_table_aux, &complete_fare_table,
					&dropoff_table_aux, &dropoff_table, &step_one_durations[run]);
			}
			else
			{
				threads[run] = new std::thread(thread_execution_step_one, false, partitioner_name, &worker_input_buffer[task], &complete_fare_table_aux, &complete_fare_table,
					&dropoff_table_aux, &dropoff_table, &step_one_durations[run]);
			}
		}
		for (size_t run = 0; run < 7; ++run)
		{
			threads[run]->join();
			delete threads[run];
			durations.push_back(step_one_durations[run]);
		}
		delete[] threads;
		auto min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		auto max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);
		first_round_exec_duration[task] = std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size();
		worker_input_buffer[task].clear();
		durations.clear();
	}
	// Partition data - Keys: pickup_cell, dropoff_cell
	threads = new std::thread*[7];
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		if (part_run == 0)
		{
			threads[part_run] = new std::thread(Experiment::DebsChallenge::ProfitableAreaPartition::partition_thread_operate_step_two,
				true, std::string(partitioner_name), partitioner, &complete_fare_table, &dropoff_table, tasks.size(), &complete_fare_imbalance[part_run],
				&complete_fare_key_imbalance[part_run], &dropoff_cell_imbalance[part_run], &dropoff_cell_key_imbalance[part_run], &part_two_durations[part_run], 
				&complete_fare_sub_table, &dropoff_cell_sub_table);
		}
		else
		{
			threads[part_run] = new std::thread(Experiment::DebsChallenge::ProfitableAreaPartition::partition_thread_operate_step_two,
				false, std::string(partitioner_name), partitioner, &complete_fare_table, &dropoff_table, tasks.size(), &complete_fare_imbalance[part_run],
				&complete_fare_key_imbalance[part_run], &dropoff_cell_imbalance[part_run], &dropoff_cell_key_imbalance[part_run], &part_two_durations[part_run],
				&complete_fare_sub_table, &dropoff_cell_sub_table);
		}
	}
	for (size_t i = 0; i < 7; i++)
	{
		threads[i]->join();
		delete threads[i];
		part_cell_durations.push_back(part_two_durations[i]);
	}
	delete[] threads;
	complete_fare_sub_table.shrink_to_fit();
	dropoff_cell_sub_table.shrink_to_fit();
	complete_fare_table.clear();
	dropoff_table.clear();
	
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 7; ++run)
		{
			Experiment::DebsChallenge::ProfitableArea worker;
			worker.second_round_init();
			std::unordered_map<std::string, std::pair<float, int>> cell_partial_result_buffer_copy(cell_partial_result_buffer);
			std::unordered_map<std::string, float> cell_full_result_buffer_copy(cell_full_result_buffer);
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
				worker.partial_finalize(cell_partial_result_buffer_copy);
			}
			else
			{
				worker.finalize(cell_full_result_buffer_copy);
			}
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			// TIME CRITICAL CODE - END
			std::chrono::duration<double, std::milli> execution_time = end - start;
			durations.push_back(execution_time.count());
			if (run >= 6)
			{
				cell_partial_result_buffer = cell_partial_result_buffer_copy;
				cell_full_result_buffer = cell_full_result_buffer_copy;
			}
		}
		complete_fare_sub_table[i].clear();
		dropoff_cell_sub_table[i].clear();
		auto min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		auto max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);
		second_round_exec_duration[i] = std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size();
		durations.clear();
	}
	complete_fare_sub_table.clear();
	dropoff_cell_sub_table.clear();
	// final aggregation
	std::vector<double> aggr_durations;
	for (size_t aggr_run = 0; aggr_run < 7; ++aggr_run)
	{
		std::vector<std::pair<float, std::string>> final_result;
		Experiment::DebsChallenge::ProfitableAreaOfflineAggregator aggregator;
		// TIME CRITICAL CODE - START
		std::chrono::system_clock::time_point aggr_start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") != 0)
		{
			aggregator.calculate_and_sort_final_aggregation(cell_partial_result_buffer, final_result);
		}
		else
		{
			aggregator.sort_final_aggregation(cell_full_result_buffer, final_result);
		}
		std::chrono::system_clock::time_point aggr_end = std::chrono::system_clock::now();
		// TIME CRITICAL CODE - END
		std::chrono::duration<double, std::milli> aggr_time = aggr_end - aggr_start;
		aggr_durations.push_back(aggr_time.count());
		if (aggr_run >= 6)
		{
			std::chrono::system_clock::time_point out_start = std::chrono::system_clock::now();
			aggregator.output_result_to_file(final_result, worker_output_file_name);
			std::chrono::system_clock::time_point out_end = std::chrono::system_clock::now();
			output_to_file_duration = (out_end - out_start).count();
		}
	}
	auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(min_it);
	auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(max_it);
	final_aggr_duration = std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) / aggr_durations.size();
	aggr_durations.clear();

	//part_medallion_durations, part_cell_durations
	auto part_it = std::max_element(part_medallion_durations.begin(), part_medallion_durations.end());
	part_medallion_durations.erase(part_it);
	part_it = std::min_element(part_medallion_durations.begin(), part_medallion_durations.end());
	part_medallion_durations.erase(part_it);
	part_it = std::max_element(part_cell_durations.begin(), part_cell_durations.end());
	part_cell_durations.erase(part_it);
	part_it = std::min_element(part_cell_durations.begin(), part_cell_durations.end());
	part_cell_durations.erase(part_it);
	double mean_part_med_duration = std::accumulate(part_medallion_durations.begin(), part_medallion_durations.end(), 0.0) / part_medallion_durations.size();
	double mean_part_cell_duration = std::accumulate(part_cell_durations.begin(), part_cell_durations.end(), 0.0) / part_cell_durations.size();

	std::stringstream result_stream;
	result_stream << partitioner_name << "," << tasks.size() << "," << *std::min_element(first_round_exec_duration.begin(), first_round_exec_duration.end()) << "," <<
		*std::max_element(first_round_exec_duration.begin(), first_round_exec_duration.end()) << "," <<
		(std::accumulate(first_round_exec_duration.begin(), first_round_exec_duration.end(), 0.0) / first_round_exec_duration.size()) << "," <<
		*std::min_element(second_round_exec_duration.begin(), second_round_exec_duration.end()) << "," <<
		*std::max_element(second_round_exec_duration.begin(), second_round_exec_duration.end()) << "," <<
		(std::accumulate(second_round_exec_duration.begin(), second_round_exec_duration.end(), 0.0) / second_round_exec_duration.size()) << "," <<
		final_aggr_duration << "," << output_to_file_duration << "," << mean_part_med_duration << "," << mean_part_cell_duration << "," << med_imbalance[0] << "," <<
		med_key_imbalance[0] << "," << dropoff_cell_imbalance[0] << "," << dropoff_cell_key_imbalance[0] << "," <<
		complete_fare_imbalance[0] << "," << complete_fare_key_imbalance[0] << "\n";
	std::cout << result_stream.str();
}

void Experiment::DebsChallenge::ProfitableAreaPartition::debs_profitable_area_worker(Experiment::DebsChallenge::ProfitableArea* profitable_area)
{
	profitable_area->operate();
}