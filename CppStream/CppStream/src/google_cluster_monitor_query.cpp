#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif // GOOGLE_CLUSTER_MONITOR_QUERY_H_

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::TotalCpuPerCategoryWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex * mu, 
	std::condition_variable * cond)
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

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::partial_finalize(std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_result)
{
	for (std::unordered_map<int, Experiment::GoogleClusterMonitor::cm_one_result>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		partial_result.push_back(cm_one_result(it->second));
	}
}

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::TotalCpuPerCategoryOfflineAggregator()
{
}

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::~TotalCpuPerCategoryOfflineAggregator()
{
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& full_aggregates, 
	std::vector<cm_one_result>& final_result)
{
	final_result = full_aggregates;
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_aggregates, 
	std::vector<cm_one_result>& final_result)
{
	
	std::map<int, cm_one_result> result;
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
		final_result.push_back(cit->second);
	}
	
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::write_output_to_file(const std::vector<GoogleClusterMonitor::cm_one_result>& final_result, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	for (auto it = final_result.cbegin(); it != final_result.cend(); ++it)
	{
		std::string buffer = std::to_string(it->category) + "," + std::to_string(it->timestamp) + "," + std::to_string(it->total_cpu) + "\n";
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
		Experiment::GoogleClusterMonitor::task_event t_e;
		try 
		{
			t_e.deserealize(line);
			buffer.push_back(t_e);
		}
		catch (std::exception& e)
		{
			std::cout << "Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::parse_task_events() threw an exception: " << e.what() << "\n";
		}
	}
	input.close();
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events_from_directory(const std::string input_dir_name, std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer)
{
	unsigned int number_of_files_scanned = 0;
	const std::string file_suffix = ".csv";
	std::vector<std::string> file_names;
	Experiment::GoogleClusterMonitor::Util::get_files(file_names, input_dir_name);
	//std::cout << "parse_task_events_from_directory():: total files found in directory " << input_dir_name << " are: " << file_names.size() << ".\n";
	for (std::vector<std::string>::const_iterator file_it = file_names.cbegin(); file_it != file_names.cend(); ++file_it)
	{
		// check if it is a task_event file
		// std::cout << "GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events_from_dir(): parsing task-events from file: " << *file_it << "...\n";
		if (Experiment::GoogleClusterMonitor::Util::ends_with(*file_it, file_suffix))
		{
			//std::vector<Experiment::GoogleClusterMonitor::task_event> partial_buffer;
			Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_task_events(*file_it, buffer);
			// buffer.reserve(buffer.size() + partial_buffer.size());
			// std::move(partial_buffer.begin(), partial_buffer.end(), std::inserter(buffer, buffer.end()));
			number_of_files_scanned++;
		}
	}
	std::cout << "parse_task_events_from_directory():: total files scanned: " << number_of_files_scanned << ".\n";
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_simulation(std::vector<Experiment::GoogleClusterMonitor::task_event>* buffer, const size_t task_number)
{
	std::vector<uint16_t> tasks;
	LoadAwarePolicy la_policy;
	CardinalityAwarePolicy ca_policy;
	Partitioner* sh;
	Partitioner* fld;
	Partitioner* pk;
	Partitioner* ca_naive;
	Partitioner* ca_aff_naive;
	Partitioner* ca_hll;
	Partitioner* ca_aff_hll;
	Partitioner* la_naive;
	Partitioner* la_hll;
	for (uint16_t i = 0; i < task_number; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	sh = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pk = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &ca_policy);
	ca_aff_naive = new CaPartitionLib::CA_Exact_Aff_Partitioner(tasks);
	ca_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &ca_policy, 12);
	ca_aff_hll = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 12);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &la_policy);
	la_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &la_policy, 12);

	std::string sh_file_name = "shg_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string fld_file_name = "fld_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string pkg_file_name = "pkg_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_naive_file_name = "ca_naive_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_hll_file_name = "ca_hll_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_naive_file_name = "la_naive_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_hll_file_name = "la_hll_google_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::cout << "GCM Q1 ***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-num,min-exec-msec,max-exec-msec,avg-exec-msec,avg-aggr-msec,io-msec,avg-part-msec,imb,key-imb\n";
	std::cout << info_stream.str();
	query_partitioner_simulation(buffer, tasks, sh, "sh", sh_file_name);
	query_partitioner_simulation(buffer, tasks, fld, "fld", fld_file_name);
	query_partitioner_simulation(buffer, tasks, pk, "pk", pkg_file_name);
	query_partitioner_simulation(buffer, tasks, ca_naive, "ca_naive", ca_naive_file_name);
	query_partitioner_simulation(buffer, tasks, ca_aff_naive, "ca_aff_naive", ca_aff_naive_file_name);
	query_partitioner_simulation(buffer, tasks, ca_hll, "ca_hll", ca_hll_file_name);
	query_partitioner_simulation(buffer, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
	query_partitioner_simulation(buffer, tasks, la_naive, "la_naive", la_naive_file_name);
	query_partitioner_simulation(buffer, tasks, la_hll, "la_hll", la_hll_file_name);

	delete sh;
	delete fld;
	delete pk;
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

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::partition_thread_operate(std::string partitioner_name, Partitioner* partitioner, 
	std::vector<Experiment::GoogleClusterMonitor::task_event>* buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration)
{
	std::chrono::duration<double, std::milli> duration;
	GCMTaskEventKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::GoogleClusterMonitor::task_event, int> sch_class_imb_aggregator(task_number, key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::GoogleClusterMonitor::task_event>::const_iterator it = buffer->cbegin(); it != buffer->cend(); ++it)
	{
		int key = it->scheduling_class;
		uint16_t task = p_copy->partition_next(&key, sizeof(it->scheduling_class));
		sch_class_imb_aggregator.incremental_measure_score_tuple_count(task, *it);
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	duration = (part_end - part_start);
	*total_duration = duration.count();
	*imbalance = sch_class_imb_aggregator.imbalance(); 
	*key_imbalance = sch_class_imb_aggregator.cardinality_imbalance();
	delete p_copy;
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::query_partitioner_simulation(std::vector<Experiment::GoogleClusterMonitor::task_event>* buffer, 
	const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	std::vector<double> sched_class_part_durations;
	float class_imbalance[7], class_key_imbalance[7];
	std::thread** threads;
	double part_durations[7];
	std::queue<Experiment::GoogleClusterMonitor::task_event> queue;
	std::mutex mu;
	std::condition_variable cond;
	// get maximum and minimum running times
	std::vector<double> exec_durations(tasks.size(), 0.0);
	double aggr_duration, write_output_duration;
	std::vector<Experiment::GoogleClusterMonitor::cm_one_result> intermediate_buffer;
	std::vector<std::vector<GoogleClusterMonitor::task_event>> worker_input_buffer(tasks.size(), std::vector<GoogleClusterMonitor::task_event>());
	// partition tuples - use 6 auxiliary threads and this main thread to scan buffer
	threads = new std::thread*[7];
	for (size_t part_run = 1; part_run < 7; ++part_run)
	{
		threads[part_run] = new std::thread(Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::partition_thread_operate, 
			std::string(partitioner_name), partitioner, buffer, tasks.size(), &class_imbalance[part_run], &class_key_imbalance[part_run], 
			&part_durations[part_run]);
	}
	std::chrono::duration<double, std::milli> duration;
	GCMTaskEventKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::GoogleClusterMonitor::task_event, int> sch_class_imb_aggregator(tasks.size(), key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	std::unordered_set<int> scheduling_class_keys;
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::GoogleClusterMonitor::task_event>::const_iterator it = buffer->cbegin(); it != buffer->cend(); ++it)
	{
		int key = it->scheduling_class;
		uint16_t task = p_copy->partition_next(&key, sizeof(it->scheduling_class));
		sch_class_imb_aggregator.incremental_measure_score(task, *it);
		worker_input_buffer[task].push_back(*it);
		scheduling_class_keys.insert(key);
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	duration = (part_end - part_start);
	delete p_copy;
	std::cout << "total scheduling class keys: " << scheduling_class_keys.size() << ".\n";
	scheduling_class_keys.clear();
	// start gathering results
	part_durations[0] = duration.count();
	class_imbalance[0] = sch_class_imb_aggregator.imbalance();
	class_key_imbalance[0] = sch_class_imb_aggregator.cardinality_imbalance();
	for (size_t part_run_thread = 1; part_run_thread < 7; ++part_run_thread)
	{
		threads[part_run_thread]->join();
		delete threads[part_run_thread];
		sched_class_part_durations.push_back(part_durations[part_run_thread]);
	}
	delete[] threads;
	for	(size_t i = 0; i < tasks.size(); ++i)
	{
		worker_input_buffer[i].shrink_to_fit();
		/*std::cout << "MeanCpuPerJobIdPartition::query_partitioner_simulation():: partitioner: " << partitioner_name << ", input workload for task " << i << 
			" has size: " << worker_input_buffer[i].size() << " tuples.\n";*/
	}
	worker_input_buffer.shrink_to_fit();
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 7; ++run)
		{
			Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker worker(&queue, &mu, &cond);
			std::vector<Experiment::GoogleClusterMonitor::cm_one_result> intermediate_buffer_copy(intermediate_buffer);
			// TIME CRITICAL - START
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			for (auto it = worker_input_buffer[i].begin(); it != worker_input_buffer[i].end(); ++it)
			{
				worker.update(*it);
			}
			worker.partial_finalize(intermediate_buffer_copy);
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			// TIME CRITICAL - END
			std::chrono::duration<double, std::micro> execution_time = end - start;
			durations.push_back(execution_time.count());
			if (run >= 6)
			{
				intermediate_buffer = intermediate_buffer_copy;
			}
		}
		auto min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		auto max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);
		exec_durations[i] = std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size();
		durations.clear();
		worker_input_buffer[i].clear();
	}
	/*std::cout << "partitioner: " << partitioner_name << " intermediate buffer size: " << intermediate_buffer.size() << 
	", with size of each element: " << sizeof(cm_one_result) << " (bytes).\n";*/
	std::vector<double> aggr_durations;
	for (size_t aggr_run = 0; aggr_run < 7; ++aggr_run)
	{
		Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator aggregator;
		std::vector<cm_one_result> final_result;
		// TIME CRITICAL - START
		std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") != 0 && partitioner_name.compare("ca_aff_naive") != 0 && partitioner_name.compare("ca_aff_hll") != 0)
		{
			aggregator.calculate_and_sort_final_aggregation(intermediate_buffer, final_result);
		}
		else
		{
			aggregator.sort_final_aggregation(intermediate_buffer, final_result);
		}
		std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
		// TIME CRITICAL - END
		std::chrono::duration<double, std::micro> aggregation_time = aggregate_end - aggregate_start;
		aggr_durations.push_back(aggregation_time.count());
		if (aggr_run >= 6)
		{
			std::chrono::system_clock::time_point output_start = std::chrono::system_clock::now();
			aggregator.write_output_to_file(final_result, worker_output_file_name);
			std::chrono::system_clock::time_point output_end = std::chrono::system_clock::now();
			write_output_duration = (output_end - output_start).count();
		}
	}
	intermediate_buffer.clear();
	auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(min_it);
	auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(max_it);
	aggr_duration = std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) / aggr_durations.size();
	aggr_durations.clear();
	auto part_it = std::max_element(sched_class_part_durations.begin(), sched_class_part_durations.end());
	sched_class_part_durations.erase(part_it);
	part_it = std::min_element(sched_class_part_durations.begin(), sched_class_part_durations.end());
	sched_class_part_durations.erase(part_it);
	double mean_part_time = std::accumulate(sched_class_part_durations.begin(), sched_class_part_durations.end(), 0.0) / sched_class_part_durations.size();

	std::stringstream result_stream;
	result_stream << partitioner_name << "," << tasks.size() << "," << *std::min_element(exec_durations.begin(), exec_durations.end()) << "," <<
		*std::max_element(exec_durations.begin(), exec_durations.end()) << "," <<
		(std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) / exec_durations.size()) << "," << aggr_duration << "," << 
		write_output_duration << "," << mean_part_time << "," << class_imbalance[0] << "," << class_key_imbalance[0] << "\n";
	std::cout << result_stream.str();
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

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_two_result>& full_aggregates, 
	std::vector<cm_two_result>& final_result)
{
	final_result = full_aggregates;
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_two_result>& partial_aggregates, 
	std::vector<cm_two_result>& final_result)
{
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
		final_result.push_back(cit->second);
	}
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator::write_output_to_file(const std::vector<cm_two_result>& final_result, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	for (auto it = final_result.cbegin(); it != final_result.cend(); ++it)
	{
		std::string buffer = std::to_string(it->job_id) + "," + std::to_string(it->sum_cpu / it->count) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_simulation(std::vector<Experiment::GoogleClusterMonitor::task_event>* buffer, const size_t task_number)
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
	ca_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &ca_policy, 12);
	ca_aff_hll = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 12);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &la_policy);
	la_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &la_policy, 12);
	std::cout << "GCM Q2 ***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-num,min-exec-msec,max-exec-msec,avg-exec-msec,avg-aggr-msec,io-msec\n";
	std::cout << info_stream.str();
	std::string sh_file_name = "shg_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string fld_file_name = "fld_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string pkg_file_name = "pkg_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_naive_file_name = "ca_naive_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_hll_file_name = "ca_hll_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_naive_file_name = "la_naive_google_q2_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_hll_file_name = "la_hll_google_q2_" + std::to_string(tasks.size()) + "_result.csv";

	query_partitioner_simulation(buffer, tasks, rrg, "sh", sh_file_name);
	query_partitioner_simulation(buffer, tasks, fld, "fld", fld_file_name);
	query_partitioner_simulation(buffer, tasks, pkg, "pk", pkg_file_name);
	query_partitioner_simulation(buffer, tasks, ca_naive, "ca-naive", ca_naive_file_name);
	query_partitioner_simulation(buffer, tasks, ca_naive, "ca_aff_naive", ca_aff_naive_file_name);
	query_partitioner_simulation(buffer, tasks, ca_hll, "ca_hll", ca_hll_file_name);
	query_partitioner_simulation(buffer, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
	query_partitioner_simulation(buffer, tasks, la_naive, "la_naive", la_naive_file_name);
	query_partitioner_simulation(buffer, tasks, la_hll, "la_hll", la_hll_file_name);

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

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::query_partitioner_simulation(std::vector<Experiment::GoogleClusterMonitor::task_event>* buffer, 
	const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	// get maximum and minimum running times
	std::vector<double> exec_durations;
	double aggr_duration, write_output_duration;
	std::vector<Experiment::GoogleClusterMonitor::cm_two_result> intermediate_buffer;
	std::vector<std::vector<GoogleClusterMonitor::task_event>> worker_input_buffer(tasks.size(), std::vector<GoogleClusterMonitor::task_event>());
	std::unordered_set<int> total_job_ids;
	// partition tuples
	for (auto it = buffer->cbegin(); it != buffer->cend(); ++it)
	{
		long key = it->job_id;
		uint16_t task = partitioner->partition_next(&key, sizeof(long));
		worker_input_buffer[task].push_back(*it);
		total_job_ids.insert(key);
	}
	std::cout << "total number of job-IDs: " << total_job_ids.size() << ".\n";
	total_job_ids.clear();
	for	(size_t i = 0; i < tasks.size(); ++i)
	{
		worker_input_buffer[i].shrink_to_fit();
		/*std::cout << "MeanCpuPerJobIdPartition::query_partitioner_simulation():: partitioner: " << partitioner_name << ", input workload for task " << i << 
		 	" has size: " << worker_input_buffer[i].size() << " tuples.\n";*/
	}
	worker_input_buffer.shrink_to_fit();
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 7; ++run)
		{
			Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker worker;
			std::vector<cm_two_result> intermediate_buffer_copy(intermediate_buffer);
			// TIME CRITICAL - START
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			for (auto it = worker_input_buffer[i].begin(); it != worker_input_buffer[i].end(); ++it)
			{
				worker.update(*it);
			}
			worker.partial_finalize(intermediate_buffer_copy);
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			// TIME CRITICAL - END
			std::chrono::duration<double, std::micro> execution_time = end - start;
			durations.push_back(execution_time.count());
			if (run >= 0)
			{
				intermediate_buffer = intermediate_buffer_copy;
			}
		}
		auto min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		auto max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);

		worker_input_buffer[i].clear();
		exec_durations.push_back(std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size());
	}
	worker_input_buffer.clear();
	std::vector<double> aggr_durations;
	for (size_t aggr_run = 0; aggr_run < 7; ++aggr_run)
	{
		std::vector<cm_two_result> final_result;
		Experiment::GoogleClusterMonitor::MeanCpuPerJobIdOfflineAggregator aggregator;
		// TIME CRITICAL - START
		std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") != 0 && partitioner_name.compare("ca_aff_naive") != 0 && partitioner_name.compare("ca_aff_hll") != 0)
		{
			aggregator.calculate_and_sort_final_aggregation(intermediate_buffer, final_result);
		}
		else
		{
			aggregator.sort_final_aggregation(intermediate_buffer, final_result);
		}
		std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
		// TIME CRITICAL - END
		std::chrono::duration<double, std::micro> aggregation_time = aggregate_end - aggregate_start;
		aggr_durations.push_back(aggregation_time.count());
		if (aggr_run >= 6)
		{
			std::chrono::system_clock::time_point output_start = std::chrono::system_clock::now();
			aggregator.write_output_to_file(final_result, worker_output_file_name);
			std::chrono::system_clock::time_point output_end = std::chrono::system_clock::now();
			write_output_duration = (output_end - output_start).count();
		}
	}
	auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(min_it);
	auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(max_it);
	aggr_duration = std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) / aggr_durations.size();
	intermediate_buffer.clear();

	std::stringstream result_stream;
	result_stream << partitioner_name << "," << tasks.size() << "," << *std::min_element(exec_durations.begin(), exec_durations.end()) << "," <<
		*std::max_element(exec_durations.begin(), exec_durations.end()) << "," <<
		(std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) / exec_durations.size()) << "," << aggr_duration << "," << write_output_duration << "\n";
	std::cout << result_stream.str();
	exec_durations.clear();
	aggr_durations.clear();
}

Experiment::GoogleClusterMonitor::SimpleScanWorker::SimpleScanWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex * mu, std::condition_variable * cond)
{
	this->input_queue = input_queue;
	this->mu = mu;
	this->cond = cond;
}

	Experiment::GoogleClusterMonitor::SimpleScanWorker::SimpleScanWorker()
	{
	}

	Experiment::GoogleClusterMonitor::SimpleScanWorker::~SimpleScanWorker()
	{
		result.clear();
	}

	void Experiment::GoogleClusterMonitor::SimpleScanWorker::operate()
	{
	}

	void Experiment::GoogleClusterMonitor::SimpleScanWorker::update(Experiment::GoogleClusterMonitor::task_event & task_event)
	{
		// need to think of a predicate
	}

	void Experiment::GoogleClusterMonitor::SimpleScanWorker::finalize(std::vector<Experiment::GoogleClusterMonitor::task_event>& task_event_buffer)
	{
		for (auto it = result.cbegin(); it != result.cend(); ++it)
		{
			task_event_buffer.push_back(*it);
		}
	}

	Experiment::GoogleClusterMonitor::SimpleScanOfflineAggregator::SimpleScanOfflineAggregator()
	{
	}

	Experiment::GoogleClusterMonitor::SimpleScanOfflineAggregator::~SimpleScanOfflineAggregator()
	{
	}

	void Experiment::GoogleClusterMonitor::SimpleScanOfflineAggregator::write_output_to_file(const std::vector<GoogleClusterMonitor::task_event>& final_result, 
		const std::string & outfile_name)
	{
		FILE* fd;
		fd = fopen(outfile_name.c_str(), "w");
		for (auto it = final_result.cbegin(); it != final_result.cend(); ++it)
		{
			std::string buffer = it->serialize() + "\n";
			fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		}
		fflush(fd);
		fclose(fd);
	}

	Experiment::GoogleClusterMonitor::SimpleAggregateWorker::SimpleAggregateWorker(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex * mu, 
		std::condition_variable * cond)
	{
		this->input_queue = input_queue;
		this->mu = mu;
		this->cond = cond;
	}

	Experiment::GoogleClusterMonitor::SimpleAggregateWorker::SimpleAggregateWorker()
	{
	}

	Experiment::GoogleClusterMonitor::SimpleAggregateWorker::~SimpleAggregateWorker()
	{
	}

	void Experiment::GoogleClusterMonitor::SimpleAggregateWorker::operate()
	{
	}

	void Experiment::GoogleClusterMonitor::SimpleAggregateWorker::update(Experiment::GoogleClusterMonitor::task_event & task_event)
	{
		// TODO: figure out a predicate and an aggregates
	}

	void Experiment::GoogleClusterMonitor::SimpleAggregateWorker::finalize(std::vector<float>& aggregate_result)
	{
		aggregate_result.push_back(this->aggregate_result);
	}

	Experiment::GoogleClusterMonitor::SimpleAggregateOfflineAggregator::SimpleAggregateOfflineAggregator()
	{
	}

	Experiment::GoogleClusterMonitor::SimpleAggregateOfflineAggregator::~SimpleAggregateOfflineAggregator()
	{
	}

	void Experiment::GoogleClusterMonitor::SimpleAggregateOfflineAggregator::aggregate(const std::vector<float>& partial_aggregate_buffer, float * aggregate_result)
	{
		float aggregate = 0.0f;
		for (std::vector<float>::const_iterator it = partial_aggregate_buffer.cbegin(); it != partial_aggregate_buffer.cend(); ++it)
		{
			// TODO: update aggregate
			aggregate += *it;
		}
	}

	void Experiment::GoogleClusterMonitor::SimpleAggregateOfflineAggregator::write_output_to_file(const float aggregate_result, const std::string & outfile_name)
	{
		FILE* fd;
		fd = fopen(outfile_name.c_str(), "w");
		std::string buffer = std::to_string(aggregate_result) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		fflush(fd);
		fclose(fd);
	}

	void Experiment::GoogleClusterMonitor::SimpleScanPartition::query_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, const size_t task_number)
	{
		CardinalityAwarePolicy ca_policy;
		LoadAwarePolicy la_policy;
		std::vector<uint16_t> tasks;
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
			tasks.push_back(i);
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

		std::string sh_file_name = "shg_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string fld_file_name = "fld_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string pkg_file_name = "pkg_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string ca_naive_file_name = "ca_naive_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string ca_aff_naive_file_name = "ca_aff_naive_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string ca_hll_file_name = "ca_hll_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string ca_aff_hll_file_name = "ca_aff_hll_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string la_naive_file_name = "la_naive_google_q3_" + std::to_string(tasks.size()) + "_result.csv";
		std::string la_hll_file_name = "la_hll_google_q3_" + std::to_string(tasks.size()) + "_result.csv";

		query_partitioner_simulation(buffer, tasks, rrg, "sh", sh_file_name);
		query_partitioner_simulation(buffer, tasks, fld, "fld", fld_file_name);
		query_partitioner_simulation(buffer, tasks, pkg, "pk", pkg_file_name);
		query_partitioner_simulation(buffer, tasks, ca_naive, "ca_naive", ca_naive_file_name);
		query_partitioner_simulation(buffer, tasks, ca_naive, "ca_aff_naive", ca_aff_naive_file_name);
		query_partitioner_simulation(buffer, tasks, ca_hll, "ca_hll", ca_hll_file_name);
		query_partitioner_simulation(buffer, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
		query_partitioner_simulation(buffer, tasks, la_naive, "la_naive", la_naive_file_name);
		query_partitioner_simulation(buffer, tasks, la_hll, "la_hll", la_hll_file_name);

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

	void Experiment::GoogleClusterMonitor::SimpleScanPartition::query_partitioner_simulation(const std::vector<Experiment::GoogleClusterMonitor::task_event>& buffer, 
		const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
	{
	}
