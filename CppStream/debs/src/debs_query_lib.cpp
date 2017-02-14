#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // DEBS_QUERY_LIB_H_

#ifndef STREAM_PARTITION_LIB_UTILS_
#include "stat_util.h"
#endif // !STREAM_PARTITION_LIB_UTILS_

#ifndef STREAM_PARTITION_LIB_NAME_UTIL_
#include "name_util.h"
#endif

void Experiment::DebsChallenge::FrequentRouteWorker::update(const Experiment::DebsChallenge::CompactRide& ride)
{
	std::stringstream str_stream;
	str_stream << (unsigned short)ride.pickup_cell.first << "." << (unsigned short)ride.pickup_cell.second << "-" << (unsigned short)ride.dropoff_cell.first << "." <<
		(unsigned short)ride.dropoff_cell.second;
	std::unordered_map<std::string, uint64_t>::iterator it = result.find(str_stream.str());
	if (it != result.end())
	{
		it->second += 1;
	}
	else
	{
		result[str_stream.str()] = 1;
	}
}

void Experiment::DebsChallenge::FrequentRouteWorker::finalize(bool write, std::vector<Experiment::DebsChallenge::frequent_route>* partial_result)
{
	if (write)
	{
		for (std::unordered_map<std::string, uint64_t>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
		{
			Experiment::DebsChallenge::frequent_route fr(it->first, it->second);
			partial_result->push_back(fr);
		}
	}
	else
	{
		std::vector<Experiment::DebsChallenge::frequent_route> partial_result_aux;
		for (std::unordered_map<std::string, uint64_t>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
		{
			Experiment::DebsChallenge::frequent_route fr(it->first, it->second);
			partial_result_aux.push_back(fr);
		}
	}
}

void Experiment::DebsChallenge::FrequentRouteAggregator::final_aggregate(const std::vector<Experiment::DebsChallenge::frequent_route>& partial_results, 
	std::unordered_map<std::string, uint64_t>& sorted_full_aggregates)
{
	for (auto cit = partial_results.cbegin(); cit != partial_results.cend(); ++cit)
	{
		auto it = sorted_full_aggregates.find(cit->route);
		if (it != sorted_full_aggregates.end())
		{
			it->second += cit->count;
		}
		else
		{
			sorted_full_aggregates[cit->route] = cit->count;
		}
	}
}

void Experiment::DebsChallenge::FrequentRouteAggregator::order_final_result(const std::vector<Experiment::DebsChallenge::frequent_route>& full_aggregates,
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
	for (auto c = final_result.crbegin(); c != final_result.crend(); ++c)
	{
		for (auto i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			result.push_back(std::make_pair(c->first, *i));
		}
		if (result.size() >= 10)
		{
			break;
		}
	}
	final_result.clear();
}

void Experiment::DebsChallenge::FrequentRouteAggregator::order_final_result(const std::unordered_map<std::string, uint64_t>& full_aggregates,
	std::vector<std::pair<unsigned long, std::string>>& result)
{
	// START: sort full result (all partitioners required)
	std::map<unsigned long, std::vector<std::string>> final_result;
	for (auto cit = full_aggregates.cbegin(); cit != full_aggregates.cend(); ++cit)
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
	// END: sort full result (all partitioners required)
	// START: result is materialized
	for (auto c = final_result.crbegin(); c != final_result.crend(); ++c)
	{
		for (auto i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			result.push_back(std::make_pair(c->first, *i));
		}
		if (result.size() >= 10)
		{
			break;
		}
	}
	// END: result is materialized
	final_result.clear();
}

void Experiment::DebsChallenge::FrequentRouteAggregator::write_output_to_file(const std::vector<std::pair<unsigned long, std::string>>& result, 
	const std::string & outfile_name)
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

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines, const size_t task_number)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks(task_number, 0);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
	std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)), fld(new HashFieldPartitioner(tasks)),
		pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
		ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)), ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
		ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)), la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
		la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator));
	for (uint16_t i = 0; i < task_number; ++i)
	{
		tasks[i] = i;
	}
	tasks.shrink_to_fit();

	const std::string sh_file_name = "sh_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string fld_file_name = "fld_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string pkg_file_name = "pk_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string ca_naive_file_name = "cn_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string ca_aff_naive_file_name = "an_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string ca_hll_file_name = "chll_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string ca_aff_hll_file_name = "ahll_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string la_naive_file_name = "ln_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	const std::string la_hll_file_name = "lhll_debs_q1_" + std::to_string(tasks.size()) + ".csv";
	std::cout << "DEBS Q1***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-number,min-exec-msec,max-exec-msec,avg-exec-msec,avg-aggr-msec,io-msec,order-msec,imb,key-imb\n";
	std::cout << info_stream.str();
	frequent_route_partitioner_simulation(true, lines, tasks, rrg, "sh", sh_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, fld, "fld", fld_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, pkg, "pk", pkg_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, ca_naive, "cn", ca_naive_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, ca_aff_naive, "an", ca_aff_naive_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, ca_hll, "chll", ca_hll_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, ca_aff_hll, "ahll", ca_aff_hll_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, la_naive, "ln", la_naive_file_name);
	frequent_route_partitioner_simulation(true, lines, tasks, la_hll, "lhll", la_hll_file_name);
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

void Experiment::DebsChallenge::FrequentRoutePartition::partition(std::unique_ptr<Partitioner>& partitioner, 
	const std::vector<CompactRide>& rides, std::vector<std::vector<CompactRide>>& worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance)
{
	DebsFrequentRideKeyExtractor key_extractor;
	ImbalanceScoreAggr<CompactRide, std::string> imbalance_aggregator(task_number, key_extractor);
	for (auto it = rides.cbegin(); it != rides.cend(); ++it)
	{
		std::string key = key_extractor.extract_key(*it);
		uint16_t task = partitioner->partition_next(key.c_str(), strlen(key.c_str()));
		if (task < worker_input_buffer.size())
		{
			worker_input_buffer[task].push_back(*it);
			imbalance_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (auto it = worker_input_buffer.begin(); it != worker_input_buffer.end(); ++it)
	{
		it->shrink_to_fit();
	}
	worker_input_buffer.shrink_to_fit();
	*imbalance = imbalance_aggregator.imbalance();
	*key_imbalance = imbalance_aggregator.cardinality_imbalance();
}

void Experiment::DebsChallenge::FrequentRoutePartition::worker_thread_operate(bool write, std::vector<Experiment::DebsChallenge::CompactRide>* input, 
	std::vector<Experiment::DebsChallenge::frequent_route>* result_buffer, double* total_duration)
{
	Experiment::DebsChallenge::FrequentRouteWorker worker;
	std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
	for (auto it = input->cbegin(); it != input->cend(); ++it)
	{
		worker.update(*it);
	}
	worker.finalize(write, result_buffer);
	std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> execution_time = end - start;
	*total_duration = execution_time.count();
}

void Experiment::DebsChallenge::FrequentRoutePartition::aggregation_thread_operate(bool write, std::string partitioner_name, 
	std::vector<Experiment::DebsChallenge::frequent_route>* input_buffer, std::vector<std::pair<unsigned long, std::string>>* result, 
	double* total_duration, std::string worker_output_file_name, double* io_duration, double* order_duration)
{
	std::unordered_map<std::string, uint64_t> final_results;
	std::chrono::duration<double, std::milli> aggregation_time, io_time, order_time;
	std::chrono::system_clock::time_point start, end;
	Experiment::DebsChallenge::FrequentRouteAggregator aggregator;
	std::vector<std::pair<unsigned long, std::string>> result_aux;
	if (write)
	{
		if (StreamPartitionLib::Name::Util::single_choice_partitioner(partitioner_name) == false)
		{
			start = std::chrono::system_clock::now();
			aggregator.final_aggregate(*input_buffer, final_results);
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(*input_buffer, *result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		else
		{
			start = std::chrono::system_clock::now();
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(final_results, *result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		start = std::chrono::system_clock::now();
		//aggregator.write_output_to_file(*result, worker_output_file_name);
		end = std::chrono::system_clock::now();
		io_time = (end - start);
		*io_duration = io_time.count();
	}
	else
	{
		if (StreamPartitionLib::Name::Util::single_choice_partitioner(partitioner_name) == false)
		{
			start = std::chrono::system_clock::now();
			aggregator.final_aggregate(*input_buffer, final_results);
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(*input_buffer, result_aux);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		else
		{
			start = std::chrono::system_clock::now();
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(*input_buffer, result_aux);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
	}
	*total_duration = aggregation_time.count();
	*order_duration = order_time.count();
}

std::vector<double> Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_partitioner_simulation(const bool write, const std::vector<CompactRide>& rides, 
	const std::vector<uint16_t>& tasks, std::unique_ptr<Partitioner>& partitioner, const std::string& partitioner_name, const std::string& worker_output_file_name)
{
	float imbalance, key_imbalance;
	std::thread** threads;
	double aggregate_durations[5], aggr_duration, write_output_duration, order_duration;
	std::vector<double> exec_durations(tasks.size(), double(0)), aggr_durations, partition_duration_vector;
	std::vector<frequent_route> partial_result;
	std::vector<std::vector<CompactRide>> worker_input_buffer(tasks.size(), std::vector<CompactRide>());
	std::vector<std::pair<unsigned long, std::string>> result;
	// partition tuples
	partition(partitioner, rides, worker_input_buffer, tasks.size(), &imbalance, &key_imbalance);
	// for every task - calculate (partial) workload
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> task_durations;
		std::vector<Experiment::DebsChallenge::frequent_route> p;
		double durations[5];
		threads = new std::thread*[5];
		for (size_t run = 0; run < 5; ++run)
		{
			threads[run] = new std::thread(Experiment::DebsChallenge::FrequentRoutePartition::worker_thread_operate, 
				(run == 0), &worker_input_buffer[i], &p, &durations[run]);
		}
		for (size_t run = 0; run < 5; ++run)
		{
			threads[run]->join();
			delete threads[run];
			task_durations.push_back(durations[run]);
		}
		delete[] threads;
		worker_input_buffer[i].clear();
		// move any results from p to partial result
		partial_result.insert(partial_result.end(), p.begin(), p.end());
		auto max_it = std::max_element(task_durations.begin(), task_durations.end());
		task_durations.erase(max_it);
		auto min_it = std::min_element(task_durations.begin(), task_durations.end());
		task_durations.erase(min_it);
		double average_exec_duration = std::accumulate(task_durations.begin(), task_durations.end(), 0.0) / task_durations.size();
		exec_durations[i] = average_exec_duration;
	}
	worker_input_buffer.clear();
	threads = new std::thread*[5];
	for (size_t run = 0; run < 5; ++run)
	{
		threads[run] = new std::thread(Experiment::DebsChallenge::FrequentRoutePartition::aggregation_thread_operate, (run == 0), partitioner_name, 
			&partial_result, &result, &aggregate_durations[run], worker_output_file_name, &write_output_duration, &order_duration);
	}
	for (size_t run = 0; run < 5; ++run)
	{
		threads[run]->join();
		delete threads[run];
		aggr_durations.push_back(aggregate_durations[run]);
	}
	delete[] threads;
	auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(max_it);
	auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(min_it);
	aggr_duration = std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) / aggr_durations.size();

	if (write)
	{
		std::stringstream result_stream;
		result_stream << partitioner_name << "," << tasks.size() << "," << *std::min_element(exec_durations.begin(), exec_durations.end()) << "," <<
			*std::max_element(exec_durations.begin(), exec_durations.end()) << "," <<
			(std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) / exec_durations.size()) << "," << aggr_duration << "," <<
			write_output_duration << "," << order_duration << "," << imbalance << "," << key_imbalance << "\n";
		std::cout << result_stream.str();
	}
	std::vector<double> partition_results;
	partition_results.push_back(*std::min_element(exec_durations.begin(), exec_durations.end()));
	partition_results.push_back(*std::max_element(exec_durations.begin(), exec_durations.end()));
	partition_results.push_back((std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) / exec_durations.size()));
	partition_results.push_back(aggr_duration);
	partition_results.push_back(write_output_duration);
	partition_results.push_back(order_duration);
	partition_results.push_back(imbalance);
	partition_results.push_back(key_imbalance);
	partial_result.clear();
	exec_durations.clear();
	aggr_durations.clear();
	return partition_results;
}

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_window_simulation(const std::string& file, const size_t task_number)
{
	
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks(task_number, 0);
	for (uint16_t i = 0; i < task_number; i++)
	{
		tasks[i] = i;
	}
	tasks.shrink_to_fit();
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
	std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)), fld(new HashFieldPartitioner(tasks)), 
	pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), 
	ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)), ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)), 
	ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)), la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)), 
	la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator)), mpk(new MultiPkPartitioner(tasks)),
		man(new CaPartitionLib::MultiAN<uint64_t>(tasks, naive_estimator));

	std::stringstream info_stream;
	info_stream << "partitioner,task-num,min-s1-msec,max-s1-msec,75ile-max-s1-msec,99ile-max-s1-msec,avg-s1-msec," << 
		"avg-s1-aggr-msec,99ile-s1-aggre,avg-order-msec,avg-imb,avg-key-imb,mean-window\n";
	std::cout << info_stream.str();
	frequent_route_partitioner_window_simulation(file, tasks, rrg, StreamPartitionLib::Name::Util::shuffle_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, fld, StreamPartitionLib::Name::Util::field_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, pkg, StreamPartitionLib::Name::Util::partial_key_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, ca_naive, StreamPartitionLib::Name::Util::cardinality_naive_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, ca_aff_naive, StreamPartitionLib::Name::Util::affinity_naive_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, ca_hll, StreamPartitionLib::Name::Util::cardinality_hip_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, ca_aff_hll, StreamPartitionLib::Name::Util::affinity_hip_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, la_naive, StreamPartitionLib::Name::Util::load_naive_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, la_hll, StreamPartitionLib::Name::Util::load_hip_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, man, StreamPartitionLib::Name::Util::multi_affinity_naive_partitioner());
	frequent_route_partitioner_window_simulation(file, tasks, mpk, StreamPartitionLib::Name::Util::multi_partial_key_partitioner());
}

void Experiment::DebsChallenge::FrequentRoutePartition::frequent_route_partitioner_window_simulation(const std::string& rides, 
	const std::vector<uint16_t>& tasks, std::unique_ptr<Partitioner>& partitioner, const std::string& partitioner_name)
{
	std::vector<double> results, min_parallel_durations, max_parallel_durations, mean_parallel_durations; 
	std::vector<double> aggregate_durations, io_durations, order_durations, imbalances, key_imbalances, window_sizes;
	std::vector<CompactRide> window_rides;
	const int debs_window_size_in_sec = 1800; // 30 minutes
	std::ifstream infile(rides);
	std::string line;
	std::getline(infile, line);
	CompactRide r(line);
	std::time_t window_start;
	// first parse the first drop-off time
	if (line.size() > 0)
		window_start = r.dropoff_datetime;
	else
		return;
	infile.clear();
	infile.seekg(0, std::ios::beg);
	while (std::getline(infile, line))
	{
		CompactRide ride(line);
		double sec_diff = std::difftime(ride.dropoff_datetime, window_start);
		if (sec_diff >= debs_window_size_in_sec)
		{
			// I am not sure about the following
			partitioner->init();
			// process window
			auto window_results = frequent_route_partitioner_simulation(false, window_rides, tasks, 
				partitioner, partitioner_name, "result");
			window_results.push_back(window_rides.size());
			window_results.shrink_to_fit();
			min_parallel_durations.push_back(std::move(window_results[0]));
			max_parallel_durations.push_back(std::move(window_results[1]));
			mean_parallel_durations.push_back(std::move(window_results[2]));
			aggregate_durations.push_back(std::move(window_results[3]));
			io_durations.push_back(std::move(window_results[4]));
			order_durations.push_back(std::move(window_results[5]));
			imbalances.push_back(std::move(window_results[6]));
			key_imbalances.push_back(std::move(window_results[7]));
			window_sizes.push_back(window_rides.size());
			// progress window
			window_start = ride.dropoff_datetime;
			window_rides.clear();
			window_rides.push_back(ride);
		}
		else
		{
			window_rides.push_back(ride);
		}
	}
	if (window_rides.size() > 0)
	{
		// I am not sure about the following
		partitioner->init();
		auto window_results = frequent_route_partitioner_simulation(false, window_rides, tasks, partitioner, 
			partitioner_name, "result");
		min_parallel_durations.push_back(std::move(window_results[0]));
		max_parallel_durations.push_back(std::move(window_results[1]));
		mean_parallel_durations.push_back(std::move(window_results[2]));
		aggregate_durations.push_back(std::move(window_results[3]));
		io_durations.push_back(std::move(window_results[4]));
		order_durations.push_back(std::move(window_results[5]));
		imbalances.push_back(std::move(window_results[6]));
		key_imbalances.push_back(std::move(window_results[7]));
		window_sizes.push_back(window_rides.size());
		window_results.clear();
	}
	double mean_min_parallel_duration = StreamPartitionLib::Stats::Util::get_mean(min_parallel_durations);
	double mean_max_parallel_duration = StreamPartitionLib::Stats::Util::get_mean(max_parallel_durations);
	double seventyfive_ile_max_parallel_duration = StreamPartitionLib::Stats::Util::get_percentile(max_parallel_durations, 0.75);
	double ninety_ile_max_parallel_duration = StreamPartitionLib::Stats::Util::get_percentile(max_parallel_durations, 0.9);
	double mean_mean_parallel_duration = StreamPartitionLib::Stats::Util::get_mean(mean_parallel_durations);
	double mean_aggr_duration = StreamPartitionLib::Stats::Util::get_mean(aggregate_durations);
	double mean_aggr_99ile_duration = StreamPartitionLib::Stats::Util::get_percentile(aggregate_durations, 0.99);
	double mean_order_duration = StreamPartitionLib::Stats::Util::get_mean(order_durations);
	double mean_imbalance = StreamPartitionLib::Stats::Util::get_mean(imbalances);
	double mean_key_imbalance = StreamPartitionLib::Stats::Util::get_mean(key_imbalances);
	double mean_window_size = StreamPartitionLib::Stats::Util::get_mean(window_sizes);
	std::stringstream s_stream;
	s_stream << partitioner_name << "," << tasks.size() << "," << mean_min_parallel_duration << "," << mean_max_parallel_duration << "," <<
		seventyfive_ile_max_parallel_duration << "," << ninety_ile_max_parallel_duration << "," << mean_mean_parallel_duration << "," <<
		mean_aggr_duration << "," << mean_aggr_99ile_duration << "," << mean_order_duration << "," << mean_imbalance << "," << mean_key_imbalance << "," <<
		mean_window_size << "\n";
	std::cout << s_stream.str();
}

void Experiment::DebsChallenge::ProfitableAreaWorker::update(const DebsChallenge::CompactRide & ride)
{
	float total = ride.fare_amount + ride.tip_amount;
	auto pickup_cell = std::to_string(ride.pickup_cell.first) + "." + std::to_string(ride.pickup_cell.second);
	auto dropoff_cell = std::to_string(ride.dropoff_cell.first) + "." + std::to_string(ride.dropoff_cell.second);
	auto medal = std::string(ride.medallion, sizeof(ride.medallion) / sizeof(ride.medallion[0]));
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
			med_it->second.first = dropoff_cell;
			med_it->second.second = ride.dropoff_datetime;
		}
	}
	else
	{
		dropoff_table[medal] = std::make_pair(dropoff_cell, ride.dropoff_datetime);
	}
}

void Experiment::DebsChallenge::ProfitableAreaWorker::first_round_gather(std::vector<std::pair<std::string, std::vector<float>>>& fare_table, 
	std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>& dropoff_table) const
{
	for (auto it = fare_map.cbegin(); it != fare_map.cend(); ++it)
	{
		fare_table.push_back(*it);
	}
	for (auto it = this->dropoff_table.cbegin(); it != this->dropoff_table.cend(); ++it)
	{
		dropoff_table.push_back(*it);
	}
}

void Experiment::DebsChallenge::ProfitableAreaWorker::second_round_init()
{
	this->pickup_cell_median_fare.clear();
	this->dropoff_cell_empty_taxi_count.clear();
}

void Experiment::DebsChallenge::ProfitableAreaWorker::second_round_update(const std::string & pickup_cell, const std::vector<float>& fare_list)
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
			std::cout << "second_round_update() identified duplicate pickup-cell record. the cell-id: " << pickup_cell <<
				" was encountered twice. current median value found: " << it->second <<
				", new value: " << median_fare << ".\n";
		}
		else
		{
			pickup_cell_median_fare[pickup_cell] = median_fare;
		}
	}
}

void Experiment::DebsChallenge::ProfitableAreaWorker::second_round_update(const std::string & medallion, const std::string & dropoff_cell, const time_t & timestamp)
{
	auto it = this->dropoff_cell_empty_taxi_count.find(dropoff_cell);
	if (it != dropoff_cell_empty_taxi_count.end())
	{
		it->second += 1;
	}
	else
	{
		dropoff_cell_empty_taxi_count[dropoff_cell] = 1;
	}
}

void Experiment::DebsChallenge::ProfitableAreaWorker::partial_second_step_finalize(std::vector<std::pair<std::string, std::pair<float, int>>>& partial_result_table) const
{
	std::unordered_map<std::string, std::pair<float, int>> buffer;
	for (auto dropoff_cell_it = dropoff_cell_empty_taxi_count.cbegin(); dropoff_cell_it != dropoff_cell_empty_taxi_count.cend(); dropoff_cell_it++)
	{
		buffer[dropoff_cell_it->first] = std::make_pair(0.0, dropoff_cell_it->second);
	}
	for (auto fare_it = pickup_cell_median_fare.cbegin(); fare_it != pickup_cell_median_fare.cend(); ++fare_it)
	{
		auto buffer_it = buffer.find(fare_it->first);
		if (buffer_it != buffer.end())
		{
			buffer_it->second.first = fare_it->second;
		}
		else
		{
			buffer[fare_it->first] = std::make_pair(fare_it->second, 0);
		}
	}
	for (auto buffer_it = buffer.cbegin(); buffer_it != buffer.cend(); ++buffer_it)
	{
		partial_result_table.push_back(*buffer_it);
	}
}

void Experiment::DebsChallenge::ProfitableAreaWorker::partial_finalize(std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer) const
{
	for (auto dropoff_cell_it = dropoff_cell_empty_taxi_count.cbegin(); dropoff_cell_it != dropoff_cell_empty_taxi_count.cend(); ++dropoff_cell_it)
	{
		auto cell_profit_buffer_it = cell_profit_buffer.find(dropoff_cell_it->first);
		if (cell_profit_buffer_it != cell_profit_buffer.end())
		{
			cell_profit_buffer_it->second.second += dropoff_cell_it->second;
		}
		else
		{
			cell_profit_buffer[dropoff_cell_it->first] = std::make_pair(0.0, dropoff_cell_it->second);
		}
	}
	for (auto fare_it = pickup_cell_median_fare.cbegin(); fare_it != pickup_cell_median_fare.cend(); ++fare_it)
	{
		auto cell_profit_buffer_it = cell_profit_buffer.find(fare_it->first);
		if (cell_profit_buffer_it != cell_profit_buffer.end())
		{
			cell_profit_buffer_it->second.first += fare_it->second;
		}
		else
		{
			cell_profit_buffer[fare_it->first] = std::make_pair(fare_it->second, 0);
		}
	}
}

void Experiment::DebsChallenge::ProfitableAreaWorker::finalize(std::unordered_map<std::string, float>& cell_profit_buffer)
{
	for (auto dropoff_cell_it = dropoff_cell_empty_taxi_count.cbegin(); dropoff_cell_it != dropoff_cell_empty_taxi_count.cend(); ++dropoff_cell_it)
	{
		auto pickup_cell_it = pickup_cell_median_fare.find(dropoff_cell_it->first);
		if (pickup_cell_it != pickup_cell_median_fare.end())
		{
			float profit = pickup_cell_it->second / dropoff_cell_it->second;
			cell_profit_buffer[dropoff_cell_it->first] = profit;
		}
	}
}

void Experiment::DebsChallenge::ProfitableAreaAggregator::step_one_materialize_aggregation(const std::vector<std::pair<std::string, std::vector<float>>>& fare_table,
	std::vector<std::pair<std::string, std::vector<float>>>& final_fare_table, const std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>& dropoff_table,
	std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>& final_dropoff_table)
{
	std::unordered_map<std::string, std::vector<float>> complete_fare_table;
	std::unordered_map<std::string, std::pair<std::string, std::time_t>> complete_dropoff_table;
	for (auto it = fare_table.cbegin(); it != fare_table.cend(); ++it)
	{
		auto c_ft = complete_fare_table.find(it->first);
		if (c_ft != complete_fare_table.end())
		{
			c_ft->second.insert(c_ft->second.end(), it->second.begin(), it->second.end());
		}
		else
		{
			complete_fare_table[it->first] = it->second;
		}
	}
	for (auto it = complete_fare_table.cbegin(); it != complete_fare_table.cend(); ++it)
	{
		final_fare_table.push_back(*it);
	}
	for (auto it = dropoff_table.cbegin(); it != dropoff_table.cend(); ++it)
	{
		auto c_dt = complete_dropoff_table.find(it->first);
		if (c_dt != complete_dropoff_table.end())
		{
			if (c_dt->second.second < it->second.second)
			{
				c_dt->second = it->second;
			}
		}
		else
		{
			complete_dropoff_table[it->first] = it->second;
		}
	}
	for (auto it = complete_dropoff_table.cbegin(); it != complete_dropoff_table.cend(); ++it)
	{
		final_dropoff_table.push_back(*it);
	}
}

void Experiment::DebsChallenge::ProfitableAreaAggregator::order_final_result(const std::unordered_map<std::string, float>& cell_profit_buffer, 
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
	for (std::map<float, std::vector<std::string>>::const_reverse_iterator c = ordered_result.crbegin(); c != ordered_result.crend(); ++c)
	{
		for (std::vector<std::string>::const_iterator i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			final_result.push_back(std::make_pair(c->first, *i));
		}
		if (final_result.size() >= 10)
		{
			break;
		}
	}
	ordered_result.clear();
}

void Experiment::DebsChallenge::ProfitableAreaAggregator::order_final_result(const std::unordered_map<std::string, std::pair<float, int>>& profit_final_buffer, 
	std::vector<std::pair<float, std::string>>& final_result)
{
	std::map<float, std::vector<std::string>> ordered_result;
	for (auto cit = profit_final_buffer.cbegin(); cit != profit_final_buffer.cend(); ++cit)
	{
		if (cit->second.second > 0)
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
	}
	for (auto c = ordered_result.crbegin(); c != ordered_result.crend(); ++c)
	{
		for (auto i = c->second.cbegin(); i != c->second.cend(); ++i)
		{
			final_result.push_back(std::make_pair(c->first, *i));
		}
		if (final_result.size() >= 10)
		{
			break;
		}
	}
	ordered_result.clear();
}

void Experiment::DebsChallenge::ProfitableAreaAggregator::calculate_final_result(const std::unordered_map<std::string, std::pair<float, int>>& cell_profit_buffer, 
	std::unordered_map<std::string, std::pair<float, int>>& profit_final_buffer)
{
	for (auto cit = cell_profit_buffer.cbegin(); cit != cell_profit_buffer.cend(); ++cit)
	{
		auto it = profit_final_buffer.find(cit->first);
		if (it != profit_final_buffer.end())
		{
			it->second.first += cit->second.first;
			it->second.second += cit->second.second;
		}
		else
		{
			profit_final_buffer[cit->first] = cit->second;
		}
	}
}

void Experiment::DebsChallenge::ProfitableAreaAggregator::output_result_to_file(const std::vector<std::pair<float, std::string>>& final_result, const std::string & out_file)
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

void Experiment::DebsChallenge::ProfitableAreaPartition::most_profitable_cell_simulation(const std::vector<Experiment::DebsChallenge::CompactRide>& lines, const size_t task_number)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks;
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
	std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)), fld(new HashFieldPartitioner(tasks)),
		pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
		ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)), ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
		ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)), la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
		la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator));
	for (uint16_t i = 0; i < task_number; ++i)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();

	std::string sh_file_name = "sh_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string fld_file_name = "fld_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string pkg_file_name = "pk_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_naive_file_name = "cn_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_aff_naive_file_name = "an_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_hll_file_name = "chll_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_aff_hll_file_name = "ahll_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string la_naive_file_name = "ln_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::string la_hll_file_name = "lhll_debs_q2_" + std::to_string(tasks.size()) + ".csv";
	std::cout << "DEBS Q2 ***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-num,min-s1-msec,max-s1-msec,avg-s1-msec,avg-s1-aggr-msec,min-s2-msec,max-s2-msec,avg-s2-msec,avg-aggr-msec,io-msec,order-msec,avg-part-cell-msec," <<
		"med-imb,med-key-imb,dropoff-imb,dropoff-key-imb,fare-imb,fare-key-imb\n";
	std::cout << info_stream.str();
	most_profitable_partitioner_simulation(true, lines, tasks, rrg, "sh", sh_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, fld, "fld", fld_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, pkg, "pk", pkg_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, ca_naive, "cn", ca_naive_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, ca_aff_naive, "an", ca_aff_naive_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, ca_hll, "chll", ca_hll_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, ca_aff_hll, "ahll", ca_aff_hll_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, la_naive, "ln", la_naive_file_name);
	most_profitable_partitioner_simulation(true, lines, tasks, la_hll, "lhll", la_hll_file_name);

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

void Experiment::DebsChallenge::ProfitableAreaPartition::partition_medallion(std::unique_ptr<Partitioner>& partitioner, 
	const std::vector<Experiment::DebsChallenge::CompactRide>& buffer, size_t task_number, float* imbalance, float* key_imbalance, 
	std::vector<std::vector<CompactRide>>* worker_buffer)
{
	std::chrono::duration<double, std::milli> total_duration;
	DebsFrequentRideKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::DebsChallenge::CompactRide, std::string> imbalance_aggregator(task_number, key_extractor);
	std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		char med_buffer[33];
		memcpy(med_buffer, it->medallion, 32 * sizeof(char));
		med_buffer[32] = '\0';
		uint16_t task = partitioner->partition_next(med_buffer, strlen(med_buffer));
		if (task < (*worker_buffer).size())
		{
			(*worker_buffer)[task].push_back(*it);
			imbalance_aggregator.incremental_measure_score(task, *it);
		}
	}
	std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
	total_duration = (part_end - part_start);
	size_t total_elements = 0;
	for (size_t i = 0; i < task_number; i++)
	{
		(*worker_buffer)[i].shrink_to_fit();
		total_elements += (*worker_buffer)[i].size();
	}
	(*worker_buffer).shrink_to_fit();
	*imbalance = imbalance_aggregator.imbalance();
	*key_imbalance = imbalance_aggregator.cardinality_imbalance();
}

void Experiment::DebsChallenge::ProfitableAreaPartition::partition_step_two(std::unique_ptr<Partitioner>& partitioner, 
	std::vector<std::pair<std::string, std::vector<float>>>* fares, std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>* dropoffs, 
	size_t task_number, float* fare_imbalance, float* fare_key_imbalance, float* dropoff_imbalance, float* dropoff_key_imbalance, 
	std::vector<std::vector<std::pair<std::string, std::vector<float>>>>* fare_sub_table,
	std::vector<std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>>* dropoffs_sub_table)
{
	DebsProfCellCompleteFareKeyExtractor complete_fare_key_extractor;
	DebsProfCellDropoffCellKeyExtractor dropoff_cell_key_extractor;
	ImbalanceScoreAggr<std::pair<std::string, std::vector<float>>, std::string> complete_fare_imb_aggregator(task_number, 
		complete_fare_key_extractor);
	ImbalanceScoreAggr<std::pair<std::string, std::pair<std::string, std::time_t>>, std::string> dropoff_cell_imb_aggregator(task_number, 
		dropoff_cell_key_extractor);
	for (auto it = fares->cbegin(); it != fares->cend(); ++it)
	{
		std::string key = it->first;
		char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
		strcpy(c_key, key.c_str());
		uint16_t task = partitioner->partition_next(c_key, strlen(c_key));
		free(c_key);
		if (task < (*fare_sub_table).size())
		{
			(*fare_sub_table)[task].push_back(std::make_pair(it->first, it->second));
			complete_fare_imb_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (size_t i = 0; i < task_number; ++i)
	{
		(*fare_sub_table)[i].shrink_to_fit();
	}
	(*fare_sub_table).shrink_to_fit();
	// do not re-initialize partitioner so that affinity works properly
	for (auto it = dropoffs->cbegin(); it != dropoffs->cend(); ++it)
	{
		std::string key = it->second.first;
		char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
		strcpy(c_key, key.c_str());
		uint16_t task = partitioner->partition_next(c_key, strlen(c_key));
		free(c_key);
		if (task < (*dropoffs_sub_table).size())
		{
			(*dropoffs_sub_table)[task].push_back(std::make_pair(it->first, it->second));
			dropoff_cell_imb_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (size_t i = 0; i < task_number; ++i)
	{
		(*dropoffs_sub_table)[i].shrink_to_fit();
	}
	(*dropoffs_sub_table).shrink_to_fit();
	*fare_imbalance = complete_fare_imb_aggregator.imbalance();
	*fare_key_imbalance = complete_fare_imb_aggregator.cardinality_imbalance();
	*dropoff_imbalance = dropoff_cell_imb_aggregator.imbalance();
	*dropoff_key_imbalance = dropoff_cell_imb_aggregator.imbalance();
}


void Experiment::DebsChallenge::ProfitableAreaPartition::thread_execution_step_one(bool write, 
	const std::vector<Experiment::DebsChallenge::CompactRide>* input_buffer, std::vector<std::pair<std::string, std::vector<float>>>* fare_table, 
	std::vector<std::pair<std::string, std::pair<std::string, time_t>>>* dropoff_table, double* total_duration)
{
	Experiment::DebsChallenge::ProfitableAreaWorker worker;
	std::vector<std::pair<std::string, std::vector<float>>> fare_table_aux;
	std::vector<std::pair<std::string, std::pair<std::string, time_t>>> dropoff_table_aux;
	if (write)
	{
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = input_buffer->cbegin(); it != input_buffer->cend(); ++it)
		{
			worker.update(*it);
		}
		worker.first_round_gather(*fare_table, *dropoff_table);
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> execution_time = end - start;
		*total_duration = execution_time.count();
	}
	else
	{
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = input_buffer->cbegin(); it != input_buffer->cend(); ++it)
		{
			worker.update(*it);
		}
		worker.first_round_gather(fare_table_aux, dropoff_table_aux);
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> execution_time = end - start;
		*total_duration = execution_time.count();
	}
}

void Experiment::DebsChallenge::ProfitableAreaPartition::thread_execution_aggregation_step_one(bool write, const std::string partitioner_name, 
	const std::vector<std::pair<std::string, std::vector<float>>>* fare_table,
	const std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>* dropoff_table, std::vector<std::pair<std::string, std::vector<float>>>* fare_table_out,
	std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>* dropoff_table_out, double* total_duration)
{
	Experiment::DebsChallenge::ProfitableAreaAggregator aggregator;
	std::vector<std::pair<std::string, std::vector<float>>> fare_table_aux;
	std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>> dropoff_table_aux;
	std::chrono::duration<double, std::milli> aggregation_time;
	if (write)
	{
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		if (StreamPartitionLib::Name::Util::single_choice_partitioner(partitioner_name) == true)
		{
			//aggregator.step_one_aggregation(*fare_table, *fare_table_out, *dropoff_table, *dropoff_table_out);
			// fld and Aff can not just transfer tuples in this because the partitioning on step one takes place 
			// on Medallion. However, in this aggregation the keys are the pickup and dropoff cells. Therefore, this 
			// aggregation has to take place
			aggregator.step_one_materialize_aggregation(*fare_table, *fare_table_out, *dropoff_table, *dropoff_table_out);
		}
		else
		{
			aggregator.step_one_materialize_aggregation(*fare_table, *fare_table_out, *dropoff_table, *dropoff_table_out);
		}
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		aggregation_time = end - start;
	}
	else
	{
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("an") == 0 || partitioner_name.compare("ahll") == 0)
		{
			aggregator.step_one_materialize_aggregation(*fare_table, fare_table_aux, *dropoff_table, dropoff_table_aux);
		}
		else
		{
			aggregator.step_one_materialize_aggregation(*fare_table, fare_table_aux, *dropoff_table, dropoff_table_aux);
		}
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		aggregation_time = end - start;
		fare_table_aux.clear();
		dropoff_table_aux.clear();
	}
	*total_duration = aggregation_time.count();
}

void Experiment::DebsChallenge::ProfitableAreaPartition::thread_final_aggregation(bool write, const std::string partitioner_name, std::unordered_map<std::string, std::pair<float, int>>* partial_input_buffer, 
	const std::unordered_map<std::string, float>* full_input_buffer, std::vector<std::pair<float, std::string>>* final_result, const std::string worker_output_file_name, 
	double* total_duration, double* io_duration, double* order_duration)
{
	ProfitableAreaAggregator aggregator;
	std::vector<std::pair<float, std::string>> final_result_aux;
	std::chrono::duration<double, std::milli> aggr_time, order_time;
	std::chrono::system_clock::time_point start, end;
	std::unordered_map<std::string, std::pair<float, int>> profit_final_buffer;
	if (write)
	{
		
		if (StreamPartitionLib::Name::Util::single_choice_partitioner(partitioner_name) == false)
		{
			start = std::chrono::system_clock::now();
			aggregator.calculate_final_result(*partial_input_buffer, profit_final_buffer);
			end = std::chrono::system_clock::now();
			aggr_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(profit_final_buffer, *final_result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		else
		{
			start = std::chrono::system_clock::now();
			end = std::chrono::system_clock::now();
			aggr_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(*full_input_buffer, *final_result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		start = std::chrono::system_clock::now();
		//aggregator.output_result_to_file(*final_result, worker_output_file_name);
		end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> output_to_file_duration = (end - start);
		*io_duration = output_to_file_duration.count();
		*order_duration = order_time.count();
	}
	else
	{
		if (StreamPartitionLib::Name::Util::single_choice_partitioner(partitioner_name) == false)
		{
			start = std::chrono::system_clock::now();
			aggregator.calculate_final_result(*partial_input_buffer, profit_final_buffer);
			end = std::chrono::system_clock::now();
			aggr_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(profit_final_buffer, final_result_aux);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		else
		{
			start = std::chrono::system_clock::now();
			end = std::chrono::system_clock::now();
			aggr_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_final_result(*full_input_buffer, final_result_aux);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		*order_duration = order_time.count();
	}
	*total_duration = aggr_time.count();
}

std::vector<double> Experiment::DebsChallenge::ProfitableAreaPartition::most_profitable_partitioner_simulation(bool write, 
	const std::vector<CompactRide>& rides, const std::vector<uint16_t>& tasks, std::unique_ptr<Partitioner>& partitioner,
	const std::string& partitioner_name, const std::string& worker_output_file_name)
{
	std::thread** threads;
	float med_imbalance, med_key_imbalance, dropoff_cell_imbalance, dropoff_cell_key_imbalance, complete_fare_imbalance, complete_fare_key_imbalance;
	double step_one_aggr_durations[5], step_one_durations[5], final_aggr_duration, output_to_file_duration, order_duration;
	std::vector<double> first_round_exec_duration(tasks.size(), double(0)), first_aggr_durations, second_round_exec_duration(tasks.size(), double(0));
	// buffers
	std::vector<std::vector<CompactRide>> worker_input_buffer(tasks.size(), std::vector<CompactRide>());
	std::vector<std::pair<std::string, std::vector<float>>> fare_table, final_fare_table;
	std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>> dropoff_table, final_dropoff_table;
	std::vector<std::vector<std::pair<std::string, std::vector<float>>>> complete_fare_sub_table(tasks.size(), 
		std::vector<std::pair<std::string, std::vector<float>>>());
	std::vector<std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>> dropoff_cell_sub_table(tasks.size(), 
		std::vector<std::pair<std::string, std::pair<std::string, std::time_t>>>());
	std::unordered_map<std::string, std::pair<float, int>> cell_partial_result_buffer;
	std::unordered_map<std::string, float> cell_full_result_buffer;
	// Partition Data: Medallion
	partition_medallion(partitioner, rides, tasks.size(), &med_imbalance, 
		&med_key_imbalance, &worker_input_buffer);
	// S1 (parallel): for each pickup-cell gather fares, for each medallion keep track of the latest dropoff-cell
	for (size_t task = 0; task < tasks.size(); ++task)
	{
		std::vector<double> durations;
		threads = new std::thread*[5];
		for (size_t run = 0; run < 5; ++run)
		{
			threads[run] = new std::thread(Experiment::DebsChallenge::ProfitableAreaPartition::thread_execution_step_one, (run == 0), 
				&worker_input_buffer[task], &fare_table, &dropoff_table, &step_one_durations[run]);
		}
		for (size_t run = 0; run < 5; ++run)
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
	// S1 aggr (serial): fare_table and dropoff_table reduction (duplicate elimination)
	threads = new std::thread*[5];
	for (size_t run = 0; run < 5; ++run)
	{
		threads[run] = new std::thread(Experiment::DebsChallenge::ProfitableAreaPartition::thread_execution_aggregation_step_one, 
			(run == 0), partitioner_name, &fare_table, &dropoff_table, &final_fare_table, &final_dropoff_table, &step_one_aggr_durations[run]);
	}
	for (size_t run = 0; run < 5; ++run)
	{
		threads[run]->join();
		delete threads[run];
		first_aggr_durations.push_back(step_one_aggr_durations[run]);
	}
	delete[] threads;
	fare_table.clear();
	dropoff_table.clear();
	// Partition Data: table fare_table on pickup_cell, table dropoff_table on dropoff_cell
	partition_step_two(
			partitioner, &final_fare_table, &final_dropoff_table, tasks.size(), &complete_fare_imbalance,
			&complete_fare_key_imbalance, &dropoff_cell_imbalance, &dropoff_cell_key_imbalance,
			&complete_fare_sub_table, &dropoff_cell_sub_table);
	final_fare_table.clear();
 	final_dropoff_table.clear();
	// S2 (parallel): Calculate median for each pickup_cell, and number of empty taxis per dropoff_cell - at this point there is a unique record 
	//					for each pickup_cell and for each medallion (but medallions might end up in a different place)
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 5; ++run)
		{
			ProfitableAreaWorker worker;
			worker.second_round_init();
			std::unordered_map<std::string, std::pair<float, int>> cell_partial_result_buffer_copy(cell_partial_result_buffer);
			std::unordered_map<std::string, float> cell_full_result_buffer_copy(cell_full_result_buffer);
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			for (auto it = complete_fare_sub_table[i].cbegin(); it != complete_fare_sub_table[i].cend(); ++it)
			{
				worker.second_round_update(it->first, it->second);
			}
			for (auto it = dropoff_cell_sub_table[i].cbegin(); it != dropoff_cell_sub_table[i].cend(); ++it)
			{
				worker.second_round_update(it->first, it->second.first, it->second.second);
			}
			if (StreamPartitionLib::Name::Util::single_choice_partitioner(partitioner_name) == false)
			{
				worker.partial_finalize(cell_partial_result_buffer_copy);
			}
			else
			{
				worker.finalize(cell_full_result_buffer_copy);
			}
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			std::chrono::duration<double, std::milli> execution_time = end - start;
			durations.push_back(execution_time.count());
			if (run >= 4)
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
	// Final aggregation (serial): Gather all records and materialize result. Sort and produce Top-10.
	std::vector<std::pair<float, std::string>> final_result;
	std::vector<double> aggr_durations;
	double final_aggregate_durations[5];
	threads = new std::thread*[5];
	for (size_t aggr_run = 0; aggr_run < 5; ++aggr_run)
	{
		threads[aggr_run] = new std::thread(thread_final_aggregation, (aggr_run == 0), partitioner_name, &cell_partial_result_buffer, &cell_full_result_buffer,
			&final_result, worker_output_file_name, &final_aggregate_durations[aggr_run], &output_to_file_duration, &order_duration);
	}
	for (size_t aggr_run = 0; aggr_run < 5; ++aggr_run)
	{
		threads[aggr_run]->join();
		delete threads[aggr_run];
		aggr_durations.push_back(final_aggregate_durations[aggr_run]);
	}
	delete[] threads;
	// gather run-times
	auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(min_it);
	auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
	aggr_durations.erase(max_it);
	final_aggr_duration = std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) / aggr_durations.size();
	aggr_durations.clear();
	min_it = std::min_element(first_aggr_durations.begin(), first_aggr_durations.end());
	first_aggr_durations.erase(min_it);
	max_it = std::max_element(first_aggr_durations.begin(), first_aggr_durations.end());
	first_aggr_durations.erase(max_it);
	if (write)
	{
		std::stringstream result_stream;
		result_stream << partitioner_name << "," << tasks.size() << "," << *std::min_element(first_round_exec_duration.begin(), first_round_exec_duration.end()) << "," <<
			*std::max_element(first_round_exec_duration.begin(), first_round_exec_duration.end()) << "," <<
			(std::accumulate(first_round_exec_duration.begin(), first_round_exec_duration.end(), 0.0) / first_round_exec_duration.size()) << "," <<
			(std::accumulate(first_aggr_durations.begin(), first_aggr_durations.end(), 0.0) / first_aggr_durations.size()) << "," <<
			*std::min_element(second_round_exec_duration.begin(), second_round_exec_duration.end()) << "," <<
			*std::max_element(second_round_exec_duration.begin(), second_round_exec_duration.end()) << "," <<
			(std::accumulate(second_round_exec_duration.begin(), second_round_exec_duration.end(), 0.0) / second_round_exec_duration.size()) << "," <<
			final_aggr_duration << "," << output_to_file_duration << "," << order_duration << ",N/A," << med_imbalance << "," <<
			med_key_imbalance << "," << dropoff_cell_imbalance << "," << dropoff_cell_key_imbalance << "," <<
			complete_fare_imbalance << "," << complete_fare_key_imbalance << "\n";
		std::cout << result_stream.str();
	}
	std::vector<double> results;
	results.push_back(*std::min_element(first_round_exec_duration.begin(), 
		first_round_exec_duration.end()));
	results.push_back(*std::max_element(first_round_exec_duration.begin(), first_round_exec_duration.end()));
	results.push_back((std::accumulate(first_round_exec_duration.begin(), first_round_exec_duration.end(), 0.0) / first_round_exec_duration.size()));
	results.push_back((std::accumulate(first_aggr_durations.begin(), first_aggr_durations.end(), 0.0) / first_aggr_durations.size()));
	results.push_back(*std::min_element(second_round_exec_duration.begin(), second_round_exec_duration.end()));
	results.push_back(*std::max_element(second_round_exec_duration.begin(), second_round_exec_duration.end()));
	results.push_back((std::accumulate(second_round_exec_duration.begin(), second_round_exec_duration.end(), 0.0) / second_round_exec_duration.size()));
	results.push_back(final_aggr_duration);
	results.push_back(order_duration);
	results.push_back(med_imbalance);
	results.push_back(med_key_imbalance);
	results.push_back(dropoff_cell_imbalance);
	results.push_back(dropoff_cell_key_imbalance);
	results.push_back(complete_fare_imbalance);
	results.push_back(complete_fare_key_imbalance);
	return results;
}

void Experiment::DebsChallenge::ProfitableAreaPartition::profitable_route_window_simulation(const std::string & file, const size_t task_number)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks(task_number, 0);
	for (uint16_t i = 0; i < task_number; ++i)
	{
		tasks[i] = i;
	}
	tasks.shrink_to_fit();
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
	std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)), fld(new HashFieldPartitioner(tasks)),
		pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
		ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)), ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
		ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)), la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
		la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator)), mpk(new MultiPkPartitioner(tasks)), 
		man(new CaPartitionLib::MultiAN<uint64_t>(tasks, naive_estimator));
	
	std::stringstream info_stream;
	info_stream << "name,task-num,min-s1,max-s1,max-99ile-s1,mean-s1,s1-aggr,s1-aggr-99ile,min-s2,max-s2,max-99ile-s2," <<
		"mean-s2,s2-aggr,s2-aggr-99ile,med-imb,med-k-imb,cell-imb,cell-k-imb,fare-imb,fare-k-imb,mean-window\n";
	std::cout << info_stream.str();
	profitable_route_partitioner_window_simulation(file, tasks, rrg, StreamPartitionLib::Name::Util::shuffle_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, fld, StreamPartitionLib::Name::Util::field_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, pkg, StreamPartitionLib::Name::Util::partial_key_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, ca_naive, StreamPartitionLib::Name::Util::cardinality_naive_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, ca_aff_naive, StreamPartitionLib::Name::Util::affinity_naive_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, ca_hll, StreamPartitionLib::Name::Util::cardinality_hip_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, ca_aff_hll, StreamPartitionLib::Name::Util::affinity_hip_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, la_naive, StreamPartitionLib::Name::Util::load_naive_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, la_hll, StreamPartitionLib::Name::Util::load_hip_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, man, StreamPartitionLib::Name::Util::multi_affinity_naive_partitioner());
	profitable_route_partitioner_window_simulation(file, tasks, mpk, StreamPartitionLib::Name::Util::multi_partial_key_partitioner());
}

void Experiment::DebsChallenge::ProfitableAreaPartition::profitable_route_partitioner_window_simulation(const std::string & rides, 
	const std::vector<uint16_t>& tasks, std::unique_ptr<Partitioner>& partitioner, const std::string & partitioner_name)
{
	std::vector<double> min_s1_parallel_durations, max_s1_parallel_durations, mean_s1_parallel_durations;
	std::vector<double> min_s2_parallel_durations, max_s2_parallel_durations, mean_s2_parallel_durations;
	std::vector<double> aggregate_s1_durations, final_aggr_durations, order_durations, med_imbalances, cell_imbalances, fare_imbalances, 
		med_key_imbalances, cell_key_imbalances, fare_key_imbalances, window_sizes;
	std::vector<std::vector<double>> results;
	std::vector<CompactRide> window_rides;
	const int debs_window_size_in_sec = 2700; // 30 minutes
	std::ifstream infile(rides);
	std::string line;
	std::getline(infile, line);
	CompactRide r(line);
	std::time_t window_start;
	// first parse the first drop-off time
	if (line.size() > 0)
		window_start = r.dropoff_datetime;
	else
		return;
	infile.clear();
	infile.seekg(0, std::ios::beg);
	while (std::getline(infile, line))
	{
		CompactRide ride(line);
		double sec_diff = std::difftime(ride.dropoff_datetime, window_start);
		if (sec_diff >= debs_window_size_in_sec)
		{
			// process window
			// I am not sure about the following, but I think it is required
			partitioner->init();
			std::vector<double> window_results = most_profitable_partitioner_simulation(false, window_rides, tasks, partitioner, partitioner_name, "result");
			window_results.push_back(window_rides.size());
			window_results.shrink_to_fit();
			min_s1_parallel_durations.push_back(std::move(window_results[0]));
			max_s1_parallel_durations.push_back(std::move(window_results[1]));
			mean_s1_parallel_durations.push_back(std::move(window_results[2]));
			aggregate_s1_durations.push_back(std::move(window_results[3]));
			min_s2_parallel_durations.push_back(std::move(window_results[4]));
			max_s2_parallel_durations.push_back(std::move(window_results[5]));
			mean_s2_parallel_durations.push_back(std::move(window_results[6]));
			final_aggr_durations.push_back(std::move(window_results[7]));
			order_durations.push_back(std::move(window_results[8]));
			med_imbalances.push_back(std::move(window_results[9]));
			med_key_imbalances.push_back(std::move(window_results[10]));
			cell_imbalances.push_back(std::move(window_results[11]));
			cell_key_imbalances.push_back(std::move(window_results[12]));
			fare_imbalances.push_back(std::move(window_results[13]));
			fare_key_imbalances.push_back(std::move(window_results[14]));
			window_sizes.push_back(window_rides.size());
			window_start = ride.dropoff_datetime;
			window_rides.clear();
			window_rides.push_back(ride);
		}
		else
		{
			window_rides.push_back(ride);
		}
	}
	if (window_rides.size() > 0)
	{
		// I am not sure about the following, but I think it is required
		partitioner->init();
		std::vector<double> window_results = most_profitable_partitioner_simulation(false, window_rides, tasks, partitioner, partitioner_name, "result");
		window_results.push_back(window_rides.size());
		window_results.shrink_to_fit();
		min_s1_parallel_durations.push_back(std::move(window_results[0]));
		max_s1_parallel_durations.push_back(std::move(window_results[1]));
		mean_s1_parallel_durations.push_back(std::move(window_results[2]));
		aggregate_s1_durations.push_back(std::move(window_results[3]));
		min_s2_parallel_durations.push_back(std::move(window_results[4]));
		max_s2_parallel_durations.push_back(std::move(window_results[5]));
		mean_s2_parallel_durations.push_back(std::move(window_results[6]));
		final_aggr_durations.push_back(std::move(window_results[7]));
		order_durations.push_back(std::move(window_results[8]));
		med_imbalances.push_back(std::move(window_results[9]));
		med_key_imbalances.push_back(std::move(window_results[10]));
		cell_imbalances.push_back(std::move(window_results[11]));
		cell_key_imbalances.push_back(std::move(window_results[12]));
		fare_imbalances.push_back(std::move(window_results[13]));
		fare_key_imbalances.push_back(std::move(window_results[14]));
		window_sizes.push_back(window_rides.size());
		window_results.clear();
	}
	std::stringstream s_stream;
	s_stream << partitioner_name << "," << tasks.size() << "," << 
		StreamPartitionLib::Stats::Util::get_mean(min_s1_parallel_durations) << "," << 
		StreamPartitionLib::Stats::Util::get_mean(max_s1_parallel_durations) << "," <<
		StreamPartitionLib::Stats::Util::get_percentile(max_s1_parallel_durations, 0.99) << "," << 
			StreamPartitionLib::Stats::Util::get_mean(mean_s1_parallel_durations) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(aggregate_s1_durations) << "," << 
			StreamPartitionLib::Stats::Util::get_percentile(aggregate_s1_durations, 0.99) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(min_s2_parallel_durations) << "," << 
			StreamPartitionLib::Stats::Util::get_mean(max_s2_parallel_durations) << "," <<
		StreamPartitionLib::Stats::Util::get_percentile(max_s2_parallel_durations, 0.99) << "," << 
			StreamPartitionLib::Stats::Util::get_mean(mean_s2_parallel_durations) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(final_aggr_durations) << "," << 
			StreamPartitionLib::Stats::Util::get_percentile(final_aggr_durations, 0.99) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(med_imbalances) << "," << 
			StreamPartitionLib::Stats::Util::get_mean(med_key_imbalances) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(cell_imbalances) << "," << 
			StreamPartitionLib::Stats::Util::get_mean(cell_key_imbalances) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(fare_imbalances) << "," << 
			StreamPartitionLib::Stats::Util::get_mean(fare_key_imbalances) << "," <<
		StreamPartitionLib::Stats::Util::get_mean(window_sizes) << "\n";
	std::cout << s_stream.str();
}
