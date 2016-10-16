#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif // GOOGLE_CLUSTER_MONITOR_QUERY_H_

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::TotalCpuPerCategoryWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex * mu, std::condition_variable * cond)
{
	this->input_queue = input_queue;
	this->mu = mu;
	this->cond = cond;
}

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::~TotalCpuPerCategoryWorker()
{
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Experiment::GoogleClusterMonitor::task_event task_event = input_queue->back();
		input_queue->pop();
		// process
		if (task_event.job_id >= 0)
		{
			update(task_event);
		}
		else
		{
			//finalize();
			locker.unlock();
			cond->notify_all();
			break;
		}
		locker.unlock();
		cond->notify_all();
	}
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::update(Experiment::GoogleClusterMonitor::task_event & task_event)
{
	auto it = result.find(task_event.scheduling_class);
	if (it != result.end())
	{
		it->second.total_cpu += task_event.cpu_request;
	}
	else
	{
		result.insert(std::make_pair(task_event.scheduling_class, 
			cm_one_result(task_event.timestamp, task_event.scheduling_class, task_event.cpu_request)));
	}
}

//void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::finalize()
//{
//	// do nothing since the result is already materialized
//}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::partial_finalize(std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_result)
{
	for (std::unordered_map<int, Experiment::GoogleClusterMonitor::cm_one_result>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		partial_result.push_back(cm_one_result(it->second));
	}
}

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::TotalCpuPerCategoryOfflineAggregator()
{
	result.clear();
}

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::~TotalCpuPerCategoryOfflineAggregator()
{
	result.clear();
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& full_aggregates, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	/*std::map<int, cm_one_result> result;
	for (std::vector<cm_one_result>::const_iterator cit = full_aggregates.cbegin(); cit != full_aggregates.cend(); ++cit)
	{
		result[cit->category] = *cit;
	}
	for (std::map<int, cm_one_result>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		std::string buffer = std::to_string(it->second.category) + "," + std::to_string(it->second.total_cpu) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}*/
	for (std::vector<cm_one_result>::const_iterator it = full_aggregates.cbegin(); it != full_aggregates.cend(); ++it)
	{
		std::string buffer = std::to_string(it->timestamp) + "," + std::to_string(it->category) + "," + std::to_string(it->total_cpu) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_aggregates, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	for (std::vector<cm_one_result>::const_iterator it = partial_aggregates.cbegin(); it != partial_aggregates.cend(); ++it)
	{
		auto cit = result.find(it->category);
		if (cit != result.end())
		{
			cit->second.total_cpu += it->total_cpu;
		}
		else
		{
			result.insert(std::make_pair(it->category, cm_one_result(*it)));
		}
	}
	for (std::map<int, cm_one_result>::const_iterator cit = result.cbegin(); cit != result.cend(); ++cit)
	{
		std::string buffer = std::to_string(cit->second.category) + "," + std::to_string(cit->second.total_cpu) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events(const std::string input_file_name, std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer)
{
	std::string line;
	std::ifstream input(input_file_name);
	if (!input.is_open())
	{
		std::cout << "GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events(): failed to open file: " << input_file_name << ".\n";
		exit(1);
	}
	while (getline(input, line))
	{
		buffer.push_back(Experiment::GoogleClusterMonitor::task_event(line));
	}
	input.close();
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events_from_directory(const std::string input_dir_name, std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer)
{
	const std::string file_suffix = ".csv";
	std::vector<std::string> file_names;
	Experiment::GoogleClusterMonitor::get_files(file_names, input_dir_name);
	for (std::vector<std::string>::const_iterator file_it = file_names.cbegin(); file_it != file_names.cend(); ++file_it)
	{
		// check if it is a task_event file
		std::cout << "GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events_from_dir(): parsing task-events from file: " << *file_it << "...\n";
		if (Experiment::GoogleClusterMonitor::ends_with(*file_it, file_suffix))
		{
			std::vector<Experiment::GoogleClusterMonitor::task_event> partial_buffer;
			GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events(*file_it, partial_buffer);
			buffer.reserve(buffer.size() + partial_buffer.size());
			std::move(partial_buffer.begin(), partial_buffer.end(), std::inserter(buffer, buffer.end()));
		}
	}
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const size_t task_number)
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
	for (uint16_t i = 0; i < task_number; i++)
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
	query_partitioner_simulation(buffer, tasks, *rrg, "shg", "shuffle_total_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *fld, "fld", "fld_total_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *pkg, "pkg", "pkg_total_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *ca_naive, "ca-naive", "ca_naive_total_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *la_naive, "la-naive", "la_naive_total_cpu_result.csv");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_partitioner_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, 
	const std::vector<uint16_t> tasks, Partitioner & partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	std::queue<Experiment::GoogleClusterMonitor::task_event> queue;
	std::mutex mu;
	std::condition_variable cond;
	// get maximum and minimum running times
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::vector<Experiment::GoogleClusterMonitor::cm_one_result> intermediate_buffer;
	std::vector<std::vector<GoogleClusterMonitor::task_event>> worker_input_buffer(tasks.size(), std::vector<GoogleClusterMonitor::task_event>());

	// partition tuples
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		int key = it->scheduling_class;
		uint16_t task = partitioner.partition_next(&key, sizeof(it->scheduling_class));
		worker_input_buffer[task].push_back(*it);
	}
	worker_input_buffer.shrink_to_fit();

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker worker(&queue, &mu, &cond);
		// TIME CRITICAL - START
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = worker_input_buffer[i].begin(); it != worker_input_buffer[i].end(); ++it)
		{
			worker.update(*it);
		}
		worker.partial_finalize(intermediate_buffer);
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		// TIME CRITICAL - END
		std::chrono::duration<double, std::milli> execution_time = end - start;
		sum_of_durations += execution_time.count();
		if (max_duration < execution_time.count())
		{
			max_duration = execution_time.count();
		}
		min_duration = i == 0 ? execution_time.count() : (min_duration > execution_time.count() ? execution_time.count() : min_duration);
		worker_input_buffer[i].clear();
	}
	Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator aggregator;
	// TIME CRITICAL - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_sort_final_aggregation(intermediate_buffer, worker_output_file_name);
	}
	else
	{
		aggregator.sort_final_aggregation(intermediate_buffer, worker_output_file_name);
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";
	intermediate_buffer.clear();
}

Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::MeanCpuPerJobIdWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex * mu, std::condition_variable * cond)
{
	this->input_queue = input_queue;
	this->mu = mu;
	this->cond = cond;
}

Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::~MeanCpuPerJobIdWorker()
{
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Experiment::GoogleClusterMonitor::task_event task_event = input_queue->back();
		input_queue->pop();
		// process
		if (task_event.job_id >= 0)
		{
			update(task_event);
		}
		else
		{
			//finalize();
			locker.unlock();
			cond->notify_all();
			break;
		}
		locker.unlock();
		cond->notify_all();
	}
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::update(Experiment::GoogleClusterMonitor::task_event & task_event)
{
	auto it = this->result.find(task_event.job_id);
	if (it != result.end())
	{
		it->second.sum_cpu += task_event.cpu_request;
		it->second.count += 1;
	}
	else
	{
		result[task_event.job_id] = cm_two_result(task_event.timestamp, task_event.job_id, task_event.cpu_request, 1);
	}
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::partial_finalize(std::vector<Experiment::GoogleClusterMonitor::cm_two_result>& buffer)
{
	for (std::unordered_map<int, cm_two_result>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		buffer.push_back(it->second);
	}
}

Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::MeanCpuPerJobIdOfflineAggregator()
{
	result.clear();
}

Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::~MeanCpuPerJobIdOfflineAggregator()
{
	result.clear();
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_two_result>& full_aggregates, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	/*std::map<int, Experiment::GoogleClusterMonitor::cm_two_result> result;
	for (std::vector<cm_two_result>::const_iterator it = full_aggregates.cbegin(); it != full_aggregates.cend(); ++it)
	{
		result[it->job_id] = *it;
	}
	for (std::map<int, Experiment::GoogleClusterMonitor::cm_two_result>::const_iterator cit = result.cbegin(); cit != result.cend(); ++cit)
	{
		std::string buffer = std::to_string(cit->second.job_id) + "," + std::to_string(cit->second.sum_cpu / cit->second.count) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}*/
	for (std::vector<cm_two_result>::const_iterator it = full_aggregates.cbegin(); it != full_aggregates.cend(); ++it)
	{
		std::string buffer = std::to_string(it->job_id) + "," + std::to_string(it->sum_cpu / it->count) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_two_result>& partial_aggregates, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	for (std::vector<cm_two_result>::const_iterator it = partial_aggregates.cbegin(); it != partial_aggregates.cend(); ++it)
	{
		auto cit = result.find(it->job_id);
		if (cit != result.end())
		{
			cit->second.sum_cpu += it->sum_cpu;
			cit->second.count += it->count;
		}
		else
		{
			result.insert(std::make_pair(it->job_id, cm_two_result(*it)));
		}
	}
	for (std::map<int, cm_two_result>::const_iterator cit = result.cbegin(); cit != result.cend(); ++cit)
	{
		std::string buffer = std::to_string(cit->second.job_id) + "," + std::to_string(cit->second.sum_cpu / cit->second.count) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const size_t task_number)
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
	for (uint16_t i = 0; i < task_number; i++)
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
	query_partitioner_simulation(buffer, tasks, *rrg, "shg", "shuffle_mean_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *fld, "fld", "fld_mean_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *pkg, "pkg", "pkg_mean_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *ca_naive, "ca-naive", "ca_naive_mean_cpu_result.csv");
	query_partitioner_simulation(buffer, tasks, *la_naive, "la-naive", "la_naive_mean_cpu_result.csv");

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_partitioner_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, 
	const std::vector<uint16_t> tasks, Partitioner & partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	std::queue<Experiment::GoogleClusterMonitor::task_event> queue;
	std::mutex mu;
	std::condition_variable cond;
	// get maximum and minimum running times
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::vector<Experiment::GoogleClusterMonitor::cm_two_result> intermediate_buffer;
	std::vector<std::vector<GoogleClusterMonitor::task_event>> worker_input_buffer(tasks.size(), std::vector<GoogleClusterMonitor::task_event>());

	// partition tuples
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		int key = it->scheduling_class;
		uint16_t task = partitioner.partition_next(&key, sizeof(it->scheduling_class));
		worker_input_buffer[task].push_back(*it);
	}
	worker_input_buffer.shrink_to_fit();

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker worker(&queue, &mu, &cond);
		// TIME CRITICAL - START
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = worker_input_buffer[i].begin(); it != worker_input_buffer[i].end(); ++it)
		{
			worker.update(*it);
		}
		worker.partial_finalize(intermediate_buffer);
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		// TIME CRITICAL - END
		std::chrono::duration<double, std::milli> execution_time = end - start;
		sum_of_durations += execution_time.count();
		if (max_duration < execution_time.count())
		{
			max_duration = execution_time.count();
		}
		min_duration = i == 0 ? execution_time.count() : (min_duration > execution_time.count() ? execution_time.count() : min_duration);
		worker_input_buffer[i].clear();
	}
	Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator aggregator;
	// TIME CRITICAL - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_sort_final_aggregation(intermediate_buffer, worker_output_file_name);
	}
	else
	{
		aggregator.sort_final_aggregation(intermediate_buffer, worker_output_file_name);
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";
	intermediate_buffer.clear();
}
