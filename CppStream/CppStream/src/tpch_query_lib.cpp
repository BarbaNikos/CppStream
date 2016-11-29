#ifndef TPCH_QUERY_LIB_H_
#include "../include/tpch_query_lib.h"
#endif

Experiment::Tpch::QueryOneWorker::QueryOneWorker(std::queue<Experiment::Tpch::lineitem>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

Experiment::Tpch::QueryOneWorker::~QueryOneWorker()
{
	result.clear();
}

void Experiment::Tpch::QueryOneWorker::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Tpch::lineitem line_item = input_queue->back();
		input_queue->pop();
		// process
		if (line_item.l_linenumber >= 0)
		{
			update(line_item);
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

void Experiment::Tpch::QueryOneWorker::update(const Experiment::Tpch::lineitem& line_item)
{
	const Experiment::Tpch::date pred_date(1998, 11, 29);
	if (line_item.l_shipdate <= pred_date)
	{
		std::string key = std::to_string(line_item.l_returnflag) + std::to_string(line_item.l_linestatus);
		std::unordered_map<std::string, Tpch::query_one_result>::iterator it = result.find(key);
		if (it != result.end())
		{
			it->second.sum_qty += line_item.l_quantity;
			it->second.sum_base_price += line_item.l_extendedprice;
			it->second.sum_disc_price += (line_item.l_extendedprice * (1 - line_item.l_discount));
			it->second.sum_charge += (line_item.l_extendedprice * (1 - line_item.l_discount) * (1 + line_item.l_tax));
			it->second.avg_qty += line_item.l_quantity;
			it->second.avg_price += line_item.l_extendedprice;
			it->second.avg_disc += line_item.l_discount;
			it->second.count_order += 1;
		}
		else
		{
			Tpch::query_one_result tmp;
			tmp.return_flag = line_item.l_returnflag;
			tmp.line_status = line_item.l_linestatus;
			tmp.sum_qty = line_item.l_quantity;
			tmp.sum_base_price = line_item.l_extendedprice;
			tmp.sum_disc_price = line_item.l_extendedprice * (1 - line_item.l_discount);
			tmp.sum_charge = line_item.l_extendedprice * (1 - line_item.l_discount) * (1 + line_item.l_tax);
			tmp.avg_qty = line_item.l_quantity;
			tmp.avg_price = line_item.l_extendedprice;
			tmp.avg_disc = line_item.l_discount;
			tmp.count_order = 1;
			result.insert(std::make_pair(key, tmp));
		}
	}
}

void Experiment::Tpch::QueryOneWorker::finalize(std::vector<query_one_result>& buffer)
{
	for (std::unordered_map<std::string, Tpch::query_one_result>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		buffer.push_back(it->second);
	}
}

Experiment::Tpch::QueryOneOfflineAggregator::QueryOneOfflineAggregator()
{
}

Experiment::Tpch::QueryOneOfflineAggregator::~QueryOneOfflineAggregator()
{
}

void Experiment::Tpch::QueryOneOfflineAggregator::sort_final_result(const std::vector<query_one_result>& buffer, 
	std::map<std::string, query_one_result>& result)
{
	for (std::vector<query_one_result>::const_iterator cit = buffer.cbegin(); cit != buffer.cend(); ++cit)
	{
		std::string key = cit->return_flag + "," + cit->line_status;
		result[key] = *cit;
	}
}

void Experiment::Tpch::QueryOneOfflineAggregator::calculate_and_produce_final_result(const std::vector<query_one_result>& buffer, 
	std::map<std::string, query_one_result>& result)
{
	// first merge results
	for (std::vector<query_one_result>::const_iterator it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		std::string key = it->return_flag + "," + it->line_status;
		auto i = result.find(key);
		if (i != result.end())
		{
			i->second.sum_qty += it->sum_qty;
			i->second.sum_base_price += it->sum_base_price;
			i->second.sum_disc_price += it->sum_disc_price;
			i->second.sum_charge += it->sum_charge;
			i->second.avg_qty += it->avg_qty;
			i->second.avg_price += it->avg_price;
			i->second.avg_disc += it->avg_disc;
			i->second.count_order += it->count_order;
		}
		else
		{
			result[key] = *it;
		}
	}
}

void Experiment::Tpch::QueryOneOfflineAggregator::write_output_result(const std::map<std::string, query_one_result>& result, const std::string & output_file)
{
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	for (std::map<std::string, query_one_result>::const_iterator it = result.cbegin(); it != result.cend(); ++it)
	{
		std::string buffer = it->second.to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::QueryOnePartition::query_one_simulation(const std::vector<Experiment::Tpch::lineitem>& lines, const size_t task_num)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks;

	Partitioner* rrg;
	Partitioner* fld;
	Partitioner* naive_shed_fld;
	Partitioner* pkg;
	Partitioner* ca_naive;
	Partitioner* ca_aff_naive;
	Partitioner* ca_hll;
	Partitioner* ca_aff_hll;
	Partitioner* la_naive;
	Partitioner* la_hll;

	for (uint16_t i = 0; i < task_num; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();

	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	naive_shed_fld = new NaiveShedFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &ca_policy);
	ca_aff_naive = new CaPartitionLib::CA_Exact_Aff_Partitioner(tasks);
	ca_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &ca_policy, 12);
	ca_aff_hll = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 12);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &la_policy);
	la_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &la_policy, 12);

	std::string sh_file_name = "shg_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string fld_file_name = "fld_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string naive_shed_fld_file_name = "naive_shed_fld_tpch_q1_" + std::to_string(tasks.size()) + "_results.csv";
	std::string pkg_file_name = "pkg_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_naive_file_name = "ca_naive_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_hll_file_name = "ca_hll_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_naive_file_name = "la_naive_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_hll_file_name = "la_hll_tpch_q1_" + std::to_string(tasks.size()) + "_result.csv";

	std::cout << "TPC-H Q1 ***\n";
	std::cout << "partitioner,task-num,max-exec-msec,min-exec-msec,avg-exec-msec,avg-aggr-msec,io-msec,avg-part-msec,imbalance,key-imbalance\n";
	/*query_one_partitioner_simulation(lines, tasks, rrg, "sh", sh_file_name);*/
	query_one_partitioner_simulation(lines, tasks, fld, "fld", fld_file_name);
	query_one_partitioner_simulation(lines, tasks, naive_shed_fld, "naive_shed_fld", naive_shed_fld_file_name);
	/*query_one_partitioner_simulation(lines, tasks, pkg, "pk", pkg_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_naive, "ca_naive", ca_naive_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_aff_naive, "ca_aff_naive", ca_aff_naive_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_hll, "ca_hll", ca_hll_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
	query_one_partitioner_simulation(lines, tasks, la_naive, "la_naive", la_naive_file_name);
	query_one_partitioner_simulation(lines, tasks, la_hll, "la_hll", la_hll_file_name);*/

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete ca_aff_naive;
	delete ca_hll;
	delete ca_aff_hll;
	delete la_naive;
	delete la_hll;
	delete naive_shed_fld;
	tasks.clear();

	std::remove(sh_file_name.c_str());
	std::remove(fld_file_name.c_str());
	std::remove(naive_shed_fld_file_name.c_str());
	std::remove(pkg_file_name.c_str());
	std::remove(ca_naive_file_name.c_str());
	std::remove(ca_aff_naive_file_name.c_str());
	std::remove(ca_hll_file_name.c_str());
	std::remove(ca_aff_hll_file_name.c_str());
	std::remove(la_naive_file_name.c_str());
	std::remove(la_hll_file_name.c_str());
}

void Experiment::Tpch::QueryOnePartition::thread_lineitem_partition(bool write, size_t task_num, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::lineitem>* input_buffer, 
	std::vector<std::vector<Experiment::Tpch::lineitem>>* worker_input_buffer, float *imbalance, float* key_imbalance, double *total_duration)
{
	std::chrono::duration<double, std::milli> duration;
	TpchQueryOneKeyExtractor key_extractor;
	ImbalanceScoreAggr<Experiment::Tpch::lineitem, std::string> imbalance_aggregator(task_num, key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	if (write)
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = input_buffer->cbegin(); it != input_buffer->cend(); ++it)
		{
			std::string key = std::to_string(it->l_returnflag) + "," + std::to_string(it->l_linestatus);
			char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
			strcpy(c_key, key.c_str());
			uint16_t task = p_copy->partition_next(c_key, strlen(c_key));
			if (task < (*worker_input_buffer).size())
			{
				(*worker_input_buffer)[task].push_back(*it);
				imbalance_aggregator.incremental_measure_score(task, *it);
			}
			else
			{
				std::cout << "out of range\n";
			}
			free(c_key);
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = (part_end - part_start);
		for (size_t i = 0; i < task_num; i++)
		{
			(*worker_input_buffer)[i].shrink_to_fit();
		}
		(*worker_input_buffer).shrink_to_fit();
	}
	else
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = input_buffer->cbegin(); it != input_buffer->cend(); ++it)
		{
			std::string key = std::to_string(it->l_returnflag) + "," + std::to_string(it->l_linestatus);
			char* c_key = static_cast<char*>(malloc(sizeof(char) * (key.length() + 1)));
			strcpy(c_key, key.c_str());
			uint16_t task = p_copy->partition_next(c_key, strlen(c_key));
			free(c_key);
			if (task < (*worker_input_buffer).size())
			{
				imbalance_aggregator.incremental_measure_score_tuple_count(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = (part_end - part_start);
	}
	*total_duration = duration.count();
	*imbalance = imbalance_aggregator.imbalance();
	*key_imbalance = imbalance_aggregator.cardinality_imbalance();
	delete p_copy;
}

void Experiment::Tpch::QueryOnePartition::thread_worker_operate(const bool write, const std::vector<Experiment::Tpch::lineitem>* input_buffer, 
	std::vector<Experiment::Tpch::query_one_result>* result_buffer, double* operate_duration)
{
	Experiment::Tpch::QueryOneWorker worker;
	std::chrono::duration<double, std::milli> execution_time;
	std::vector<Experiment::Tpch::query_one_result> aux_buffer;
	if (write)
	{
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = input_buffer->begin(); it != input_buffer->end(); ++it)
		{
			worker.update(*it);
		}
		worker.finalize(*result_buffer);
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		execution_time = end - start;
	}
	else
	{
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = input_buffer->begin(); it != input_buffer->end(); ++it)
		{
			worker.update(*it);
		}
		worker.finalize(aux_buffer);
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		execution_time = end - start;
	}
	*operate_duration = execution_time.count();
}

void Experiment::Tpch::QueryOnePartition::thread_aggregate(const bool write, const std::string partitioner_name, const std::vector<Experiment::Tpch::query_one_result>* input_buffer,
	std::map<std::string, Experiment::Tpch::query_one_result>* result, const std::string worker_output_file_name, double* total_duration, double* io_duration)
{
	Experiment::Tpch::QueryOneOfflineAggregator aggregator;
	std::map<std::string, Experiment::Tpch::query_one_result> aux_result;
	std::chrono::duration<double, std::milli> aggregation_time, write_output_duration;
	if (write)
	{
		std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") != 0 && partitioner_name.compare("ca_aff_naive") != 0 && partitioner_name.compare("ca_aff_hll") != 0)
		{
			aggregator.calculate_and_produce_final_result(*input_buffer, *result);
		}
		else
		{
			aggregator.sort_final_result(*input_buffer, *result);
		}
		std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
		aggregation_time = (aggregate_end - aggregate_start);
		std::chrono::system_clock::time_point write_output_start = std::chrono::system_clock::now();
		aggregator.write_output_result(*result, worker_output_file_name);
		std::chrono::system_clock::time_point write_output_end = std::chrono::system_clock::now();
		write_output_duration = write_output_end - write_output_start;
		*io_duration = write_output_duration.count();
	}
	else
	{
		std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") != 0 && partitioner_name.compare("ca_aff_naive") != 0 && partitioner_name.compare("ca_aff_hll") != 0)
		{
			aggregator.calculate_and_produce_final_result(*input_buffer, aux_result);
		}
		else
		{
			aggregator.sort_final_result(*input_buffer, aux_result);
		}
		std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
		aggregation_time = (aggregate_end - aggregate_start);
	}
	*total_duration = aggregation_time.count();
}

void Experiment::Tpch::QueryOnePartition::query_one_partitioner_simulation(const std::vector<Experiment::Tpch::lineitem>& lineitem_table, 
	const std::vector<uint16_t> tasks, Partitioner* partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	float imbalance[7], cardinality_imbalance[7];
	double part_durations[7], aggregate_durations[7];
	std::thread** threads;
	std::vector<double> duration_vector(tasks.size(), 0.0);
	std::vector<double> partition_duration_vector;
	std::vector<double> aggregation_duration_vector;
	double write_output_duration_in_msec;
	std::vector<Tpch::query_one_result> intermediate_buffer;
	std::vector<std::vector<Tpch::lineitem>> worker_input_buffer(tasks.size(), std::vector<Tpch::lineitem>());
	std::map<std::string, Experiment::Tpch::query_one_result> result_buffer;
	// partition tuples - run 7 times to measure partitioning latency
	threads = new std::thread*[7];
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run] = new std::thread(Experiment::Tpch::QueryOnePartition::thread_lineitem_partition, (part_run == 0), 
			tasks.size(), partitioner_name, partitioner, &lineitem_table, &worker_input_buffer, &imbalance[part_run], 
			&cardinality_imbalance[part_run], &part_durations[part_run]);
	}
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run]->join();
		delete threads[part_run];
		partition_duration_vector.push_back(part_durations[part_run]);
		std::cout << "task " << part_run << ": " << worker_input_buffer[part_run].size() << ".\n";
	}
	delete[] threads;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		double exec_durations[7];
		threads = new std::thread*[7];
		for (size_t part_run = 0; part_run < 7; ++part_run)
		{
			threads[part_run] = new std::thread(Experiment::Tpch::QueryOnePartition::thread_worker_operate, (part_run == 0), 
				&worker_input_buffer[i], &intermediate_buffer, &exec_durations[part_run]);
		}
		for (size_t part_run = 0; part_run < 7; ++part_run)
		{
			threads[part_run]->join();
			delete threads[part_run];
			durations.push_back(exec_durations[part_run]);
		}
		delete[] threads;
		worker_input_buffer[i].clear();
		std::vector<double>::iterator max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);
		std::vector<double>::iterator min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		double sum_of_durations = std::accumulate(durations.begin(), durations.end(), 0.0);
		double average_duration = sum_of_durations / durations.size();
		duration_vector[i] = average_duration;
	}
	threads = new std::thread*[7];
	for (size_t run = 0; run < 7; ++run)
	{
		threads[run] = new std::thread(Experiment::Tpch::QueryOnePartition::thread_aggregate, (run == 0), partitioner_name, 
			&intermediate_buffer, &result_buffer, worker_output_file_name, &aggregate_durations[run], &write_output_duration_in_msec);
	}
	for (size_t run = 0; run < 7; ++run)
	{
		threads[run]->join();
		delete threads[run];
		aggregation_duration_vector.push_back(aggregate_durations[run]);
	}
	delete[] threads;
	std::vector<double>::iterator aggr_max_it = std::max_element(aggregation_duration_vector.begin(), aggregation_duration_vector.end());
	aggregation_duration_vector.erase(aggr_max_it);
	std::vector<double>::iterator aggr_min_it = std::min_element(aggregation_duration_vector.begin(), aggregation_duration_vector.end());
	aggregation_duration_vector.erase(aggr_min_it);
	double sum_of_aggr_durations = std::accumulate(aggregation_duration_vector.begin(), aggregation_duration_vector.end(), 0.0);
	double average_aggr_duration = sum_of_aggr_durations / aggregation_duration_vector.size();
	
	std::vector<double>::iterator max_it = std::max_element(duration_vector.begin(), duration_vector.end());
	std::vector<double>::iterator min_it = std::min_element(duration_vector.begin(), duration_vector.end());
	double sum_of_exec_times = std::accumulate(duration_vector.begin(), duration_vector.end(), 0.0);
	double avg_exec_time = sum_of_exec_times / duration_vector.size();

	std::vector<double>::iterator part_max_it = std::max_element(partition_duration_vector.begin(), partition_duration_vector.end());
	partition_duration_vector.erase(part_max_it);
	std::vector<double>::iterator part_min_it = std::min_element(partition_duration_vector.begin(), partition_duration_vector.end());
	partition_duration_vector.erase(part_min_it);
	double average_part_duration = std::accumulate(partition_duration_vector.begin(), partition_duration_vector.end(), 0.0) / 
		partition_duration_vector.size();
	std::string result = partitioner_name + "," + std::to_string(tasks.size()) + "," + std::to_string(*max_it) + "," + std::to_string(*min_it) + "," +
		std::to_string(avg_exec_time) + "," + std::to_string(average_aggr_duration) + "," + std::to_string(write_output_duration_in_msec) + "," +
		std::to_string(average_part_duration) + "," + std::to_string(imbalance[0]) + "," + std::to_string(cardinality_imbalance[0]) + "\n";
	std::cout << result;
	intermediate_buffer.clear();
}

Experiment::Tpch::QueryThreeJoinWorker::QueryThreeJoinWorker(const Experiment::Tpch::query_three_predicate & predicate)
{
	this->predicate = predicate;
}

Experiment::Tpch::QueryThreeJoinWorker::~QueryThreeJoinWorker()
{	
}

void Experiment::Tpch::QueryThreeJoinWorker::step_one_update(const Experiment::Tpch::q3_customer& customer, std::string partitioner_name)
{
	if (strncmp(this->predicate.c_mktsegment, customer.c_mktsegment, 10) == 0)
	{
		this->cu_index.insert(customer.c_custkey);
	}
}

void Experiment::Tpch::QueryThreeJoinWorker::step_one_update(const Experiment::Tpch::order & order, std::string partitioner_name)
{
	if (order.o_orderdate.day < predicate.order_date.day)
	{
		std::unordered_set<uint32_t>::iterator customer_iterator = cu_index.find(order.o_custkey);
		if (customer_iterator != cu_index.end())
		{
			step_one_result[order.o_orderkey] = Tpch::query_three_step_one(order.o_orderdate, order.o_shippriority);
		}
		else
		{
			// you need to add the order, because during aggregation there might be unmatched orders
			o_index[order.o_orderkey] = order;
		}
	}
}

void Experiment::Tpch::QueryThreeJoinWorker::step_one_finalize(std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result_buffer)
{
	// attempt to match all un-matched tuples
	for (auto order_index_it = o_index.cbegin(); order_index_it != o_index.cend(); ++order_index_it)
	{
		std::unordered_set<uint32_t>::iterator customer_it = cu_index.find(order_index_it->second.o_custkey);
		if (customer_it != cu_index.end())
		{
			// materialize the result if it does not exist in the result buffer
			std::unordered_map<uint32_t, Experiment::Tpch::query_three_step_one>::iterator result_buffer_it = step_one_result_buffer.find(order_index_it->first);
			if (result_buffer_it == step_one_result_buffer.end())
			{
				step_one_result_buffer[order_index_it->first] = Tpch::query_three_step_one(order_index_it->second.o_orderdate, 
					order_index_it->second.o_shippriority);
			}
		}
	}
	// transfer all (non-existent) results in the result-buffer
	for (auto it = this->step_one_result.cbegin(); it != this->step_one_result.cend(); ++it)
	{
		std::unordered_map<uint32_t, Experiment::Tpch::query_three_step_one>::iterator result_buffer_it = step_one_result_buffer.find(it->first);
		if (result_buffer_it == step_one_result_buffer.end())
		{
			step_one_result_buffer[it->first] = it->second;
		}
	}
}

void Experiment::Tpch::QueryThreeJoinWorker::step_one_partial_finalize(std::unordered_set<uint32_t>& c_index, std::unordered_map<uint32_t,Tpch::order>& o_index, 
	std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result_buffer) const
{
	// gather all qualifying customers
	for (auto c_it = this->cu_index.cbegin(); c_it != this->cu_index.cend(); c_it++)
	{
		auto c_index_it = c_index.find(*c_it);
		if (c_index_it == c_index.end())
		{
			c_index.insert(*c_it);
		}
	}
	// go over all un-matched orders to check if they match with any qualifying customer
	for (auto o_it = this->o_index.cbegin(); o_it != this->o_index.cend(); o_it++)
	{
		auto c_it = c_index.find(o_it->second.o_custkey);
		if (c_it != c_index.end())
		{
			auto result_buffer_it = step_one_result_buffer.find(o_it->first);
			if (result_buffer_it == step_one_result_buffer.end())
			{
				step_one_result_buffer[o_it->first] = Tpch::query_three_step_one(o_it->second.o_orderdate, o_it->second.o_shippriority);
			}
		}
		else
		{
			// if the customer is not found, add the order in the order-index
			if (o_index.find(o_it->first) == o_index.end())
			{
				o_index[o_it->first] = o_it->second;
			}
		}
	}
	// transfer all (remaining) results from the step_one_result to the result buffer
	for (auto r_it = this->step_one_result.cbegin(); r_it != this->step_one_result.cend(); r_it++)
	{
		auto rb_it = step_one_result_buffer.find(r_it->first);
		if (rb_it == step_one_result_buffer.end())
		{
			step_one_result_buffer[r_it->first] = r_it->second;
		}
	}
}

void Experiment::Tpch::QueryThreeJoinWorker::step_two_init(const std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result)
{
	this->step_one_result.clear();
	this->step_one_result = step_one_result;
}

void Experiment::Tpch::QueryThreeJoinWorker::step_two_update(const Experiment::Tpch::lineitem & line_item)
{
	if (line_item.l_shipdate.day > this->predicate.order_date.day)
	{
		auto it = step_one_result.find(line_item.l_order_key);
		if (it != step_one_result.end())
		{
			std::string key = std::to_string(line_item.l_order_key) + "," +
				it->second.o_orderdate.to_string() + "," + std::to_string(it->second.o_shippriority);
			float revenue_update = (line_item.l_extendedprice * (1 - line_item.l_discount));
			auto result_it = final_result.find(key);
			if (result_it != final_result.end())
			{
				result_it->second.revenue += revenue_update;
			}
			else
			{
				final_result.insert(std::make_pair(key, Experiment::Tpch::query_three_result(it->first, it->second.o_shippriority, 
					it->second.o_orderdate, revenue_update)));
			}
		}
	}
}

void Experiment::Tpch::QueryThreeJoinWorker::finalize(std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>& result_buffer) const
{
	for (auto it = final_result.cbegin(); it != final_result.cend(); it++)
	{
		result_buffer.push_back(std::make_pair(it->first, it->second));
	}
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_one_sort(const std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result_buffer, std::unordered_map<uint32_t, Tpch::query_three_step_one>& result_buffer) const
{
	result_buffer.clear();
	result_buffer.insert(step_one_result_buffer.cbegin(), step_one_result_buffer.cend());
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_one_materialize_and_sort(const std::unordered_set<uint32_t>& c_index, const std::unordered_map<uint32_t, Tpch::order>& o_index, std::unordered_map<uint32_t, Tpch::query_three_step_one>& step_one_result_buffer, std::unordered_map<uint32_t, Tpch::query_three_step_one>& result_buffer) const
{
	for (auto o_it = o_index.cbegin(); o_it != o_index.cend(); ++o_it)
	{
		auto c_it = c_index.find(o_it->second.o_custkey);
		if (c_it != c_index.end())
		{
			auto r_it = step_one_result_buffer.find(o_it->first);
			if (r_it == step_one_result_buffer.end())
			{
				step_one_result_buffer[o_it->first] = Tpch::query_three_step_one(o_it->second.o_orderdate, o_it->second.o_shippriority);
			}
		}
	}
	result_buffer.clear();
	result_buffer.insert(step_one_result_buffer.cbegin(), step_one_result_buffer.cend());
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_two_sort(const std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>& result_buffer, 
	std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>& final_result) const
{
	std::map<float, std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>> index_result;
	for (auto it = result_buffer.cbegin(); it != result_buffer.cend(); it++)
	{
		auto index_it = index_result.find(it->second.revenue);
		if (index_it != index_result.end())
		{
			index_it->second.push_back(*it);
		}
		else
		{
			index_result[it->second.revenue] = std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>();
			index_result[it->second.revenue].push_back(*it);
		}
	}
	for (auto it = index_result.crbegin(); it != index_result.crend(); ++it)
	{
		for (auto it_2 = it->second.cbegin(); it_2 != it->second.cend(); ++it_2)
		{
			final_result.push_back(*it_2);
		}
		if (final_result.size() >= 20)
		{
			break;
		}
	}
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_two_calculate_and_sort(const std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>& result_buffer, 
	std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>& final_result) const
{
	std::unordered_map<std::string, Experiment::Tpch::query_three_result> full_result;
	for (auto it = result_buffer.cbegin(); it != result_buffer.cend(); it++)
	{
		auto f_it = full_result.find(it->first);
		if (f_it != full_result.end())
		{
			f_it->second.revenue += it->second.revenue;
		}
		else
		{
			full_result[it->first] = it->second;
		}
	}
	std::map<float, std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>> index_result;
	for (auto it = full_result.cbegin(); it != full_result.cend(); it++)
	{
		auto index_it = index_result.find(it->second.revenue);
		if (index_it != index_result.end())
		{
			index_it->second.push_back(*it);
		}
		else
		{
			index_result[it->second.revenue] = std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>>();
			index_result[it->second.revenue].push_back(*it);
		}
	}
	for (auto it = index_result.crbegin(); it != index_result.crend(); ++it)
	{
		for (auto it_2 = it->second.cbegin(); it_2 != it->second.cend(); ++it_2)
		{
			final_result.push_back(*it_2);
		}
		if (final_result.size() >= 20)
		{
			break;
		}
	}
}

void Experiment::Tpch::QueryThreeOfflineAggregator::write_output_to_file(const std::vector<std::pair<std::string, query_three_result>>& result, const std::string & output_file) const
{
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	for (std::vector<std::pair<std::string, query_three_result>>::const_iterator i = result.cbegin(); i != result.cend(); ++i)
	{
		std::stringstream stream;
		stream << std::fixed << std::setprecision(2) << i->second.revenue;
		std::string buffer = i->first + "," + stream.str() + "," +
			i->second.o_orderdate.to_string() + "," + std::to_string(i->second.o_shippriority) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::QueryThreePartition::query_three_simulation(const std::vector<Experiment::Tpch::q3_customer>& c_table, 
	const std::vector<Experiment::Tpch::lineitem>& li_table, const std::vector<Experiment::Tpch::order>& o_table, const size_t task_num)
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

	for (uint16_t i = 0; i < task_num; i++)
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

	std::string sh_file_name = "shg_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string fld_file_name = "fld_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string pkg_file_name = "pkg_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_naive_file_name = "ca_naive_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_hll_file_name = "ca_hll_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_naive_file_name = "la_naive_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";
	std::string la_hll_file_name = "la_hll_tpch_q3_" + std::to_string(tasks.size()) + "_result.csv";

	std::cout << "TPC-H Q3 ***\n";
	std::stringstream info_stream;
	info_stream << "partitioner,task-num,max-s1-exec-msec,min-s1-exec-msec,avg-s1-exec-msec,mean-s1-aggr-msec,max-s2-exec-msec,min-s2-exec-msec,avg-s2-exec-msec,mean-s2-aggr-msec,io-msec," <<
		"avg-part-c-msec,avg-part-o-msec,avg-part-li-msec,c-imb,c-key-imb,o-imb,o-key-imb,li-imb,li-key-imb\n";
	std::string info_message = info_stream.str();
	std::cout << info_message;
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, rrg, "sh", sh_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, fld, "fld", fld_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, pkg, "pk", pkg_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_naive, "ca_naive", ca_naive_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_aff_naive, "ca_aff_naive", ca_aff_naive_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_hll, "ca_hll", ca_hll_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_aff_hll, "ca_aff_hll", ca_aff_hll_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, la_naive, "la_naive", la_naive_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, la_hll, "la_hll", la_hll_file_name);

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

void Experiment::Tpch::QueryThreePartition::thread_customer_partition(bool write, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::q3_customer>* c_table,
	std::vector<std::vector<Experiment::Tpch::q3_customer>>* c_worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration)
{
	std::chrono::duration<double, std::milli> duration;
	TpchQueryThreeCustomerKeyExtractor c_key_extractor;
	ImbalanceScoreAggr<Experiment::Tpch::q3_customer, uint32_t> c_imbalance_aggregator(task_number, c_key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	if (write)
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = c_table->cbegin(); it != c_table->cend(); ++it)
		{
			uint16_t task = p_copy->partition_next(&it->c_custkey, sizeof(uint32_t));
			if (task < (*c_worker_input_buffer).size())
			{
				(*c_worker_input_buffer)[task].push_back(*it);
				c_imbalance_aggregator.incremental_measure_score(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = part_end - part_start;
		for (size_t i = 0; i < task_number; ++i)
		{
			(*c_worker_input_buffer)[i].shrink_to_fit();
		}
		(*c_worker_input_buffer).shrink_to_fit();
	}
	else
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = c_table->cbegin(); it != c_table->cend(); ++it)
		{
			uint16_t task = p_copy->partition_next(&it->c_custkey, sizeof(uint32_t));
			if (task < (*c_worker_input_buffer).size())
			{
				c_imbalance_aggregator.incremental_measure_score_tuple_count(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = part_end - part_start;
	}
	*imbalance = c_imbalance_aggregator.imbalance();
	*key_imbalance = c_imbalance_aggregator.cardinality_imbalance();
	*total_duration = duration.count();
	delete p_copy;
}

void Experiment::Tpch::QueryThreePartition::thread_order_partition(bool write, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::order>* o_table,
	std::vector<std::vector<Experiment::Tpch::order>>* o_worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration)
{
	std::chrono::duration<double, std::milli> duration;
	TpchQueryThreeOrderKeyExtractor o_key_extractor;
	ImbalanceScoreAggr<Experiment::Tpch::order, uint32_t> o_imbalance_aggregator(task_number, o_key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	if (write)
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = o_table->cbegin(); it != o_table->cend(); ++it)
		{
			uint16_t task = p_copy->partition_next(&it->o_custkey, sizeof(uint32_t));
			if (task < (*o_worker_input_buffer).size())
			{
				(*o_worker_input_buffer)[task].push_back(*it);
				o_imbalance_aggregator.incremental_measure_score(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = part_end - part_start;
		for (size_t i = 0; i < task_number; ++i)
		{
			(*o_worker_input_buffer)[i].shrink_to_fit();
		}
		(*o_worker_input_buffer).shrink_to_fit();
	}
	else
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = o_table->cbegin(); it != o_table->cend(); ++it)
		{
			uint16_t task = p_copy->partition_next(&it->o_custkey, sizeof(uint32_t));
			if (task < (*o_worker_input_buffer).size())
			{
				o_imbalance_aggregator.incremental_measure_score_tuple_count(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = part_end - part_start;
	}
	*imbalance = o_imbalance_aggregator.imbalance();
	*key_imbalance = o_imbalance_aggregator.cardinality_imbalance();
	*total_duration = duration.count();
	delete p_copy;
}

void Experiment::Tpch::QueryThreePartition::thread_li_partition(bool write, std::string partitioner_name, Partitioner* partitioner, const std::vector<Experiment::Tpch::lineitem>* li_table,
	std::vector<std::vector<Experiment::Tpch::lineitem>>* li_worker_input_buffer, size_t task_number, float* imbalance, float* key_imbalance, double* total_duration)
{
	std::chrono::duration<double, std::milli> duration;
	TpchQueryThreeLineitemKeyExtractor li_key_extractor;
	ImbalanceScoreAggr<Experiment::Tpch::lineitem, uint32_t> li_imbalance_aggregator(task_number, li_key_extractor);
	Partitioner* p_copy = PartitionerFactory::generate_copy(partitioner_name, partitioner);
	p_copy->init();
	if (write)
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = li_table->cbegin(); it != li_table->cend(); ++it)
		{
			uint16_t task = p_copy->partition_next(&it->l_order_key, sizeof(it->l_order_key));
			if (task < (*li_worker_input_buffer).size())
			{
				(*li_worker_input_buffer)[task].push_back(*it);
				li_imbalance_aggregator.incremental_measure_score(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = part_end - part_start;
		for (size_t i = 0; i < task_number; ++i)
		{
			(*li_worker_input_buffer)[i].shrink_to_fit();
		}
		(*li_worker_input_buffer).shrink_to_fit();
	}
	else
	{
		std::chrono::system_clock::time_point part_start = std::chrono::system_clock::now();
		for (auto it = li_table->cbegin(); it != li_table->cend(); ++it)
		{
			uint16_t task = p_copy->partition_next(&it->l_order_key, sizeof(it->l_order_key));
			if (task < (*li_worker_input_buffer).size())
			{
				li_imbalance_aggregator.incremental_measure_score_tuple_count(task, *it);
			}
		}
		std::chrono::system_clock::time_point part_end = std::chrono::system_clock::now();
		duration = part_end - part_start;
	}
	*imbalance = li_imbalance_aggregator.imbalance();
	*key_imbalance = li_imbalance_aggregator.cardinality_imbalance();
	*total_duration = duration.count();
	delete p_copy;
}

void Experiment::Tpch::QueryThreePartition::query_three_partitioner_simulation(const std::vector<Experiment::Tpch::q3_customer>& c_table, 
	const std::vector<Experiment::Tpch::lineitem>& li_table, 
	const std::vector<Experiment::Tpch::order>& o_table, 
	const std::vector<uint16_t> tasks, Partitioner* partitioner, 
	const std::string partitioner_name, const std::string worker_output_file_name)
{
	std::vector<double> part_customer_durations, part_order_durations, part_li_durations;
	std::vector<double> step_one_exec_durations(tasks.size(), double(0)), step_two_exec_durations(tasks.size(), double(0)), 
		step_one_aggr_durations;
	std::chrono::duration<double, std::milli> write_to_output_time;
	Experiment::Tpch::query_three_predicate predicate;
	std::vector<std::vector<Tpch::q3_customer>> c_worker_input_buffer(tasks.size(), std::vector<Tpch::q3_customer>());
	std::vector<std::vector<Tpch::lineitem>> li_worker_input_buffer(tasks.size(), std::vector<Tpch::lineitem>());
	std::vector<std::vector<Tpch::order>> o_worker_input_buffer(tasks.size(), std::vector<Tpch::order>());
	std::unordered_set<uint32_t> step_one_customer_buffer;
	std::unordered_map<uint32_t, Tpch::order> step_one_order_buffer;
	std::unordered_map<uint32_t, Experiment::Tpch::query_three_step_one> step_one_result_buffer, broadcast_result;
	std::vector<std::pair<std::string, Tpch::query_three_result>> result_buffer;
	float c_imbalance[7], c_key_imbalance[7], o_imbalance[7], o_key_imbalance[7], li_imbalance[7], li_key_imbalance[7];
	double c_durations[7], o_durations[7], li_durations[7];
	std::thread** threads;
	// initialize predicate
	predicate.order_date.day = 15;
	predicate.order_date.month = 3;
	predicate.order_date.year = 1995;
	memcpy(predicate.c_mktsegment, "BUILDING\0\0", 10 * sizeof(char));
	// Step One: Join customer and order tables
	// partition order tuples and customer tuples based on cust_key
	threads = new std::thread*[7];
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run] = new std::thread(Experiment::Tpch::QueryThreePartition::thread_customer_partition, (part_run == 0), partitioner_name,
			partitioner, &c_table, &c_worker_input_buffer, tasks.size(), &c_imbalance[part_run], &c_key_imbalance[part_run], &c_durations[part_run]);
	}
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run]->join();
		delete threads[part_run];
		part_customer_durations.push_back(c_durations[part_run]);
	}
	delete[] threads;
	threads = new std::thread*[7];
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run] = new std::thread(Experiment::Tpch::QueryThreePartition::thread_order_partition, (part_run == 0), partitioner_name,
			partitioner, &o_table, &o_worker_input_buffer, tasks.size(), &o_imbalance[part_run], &o_key_imbalance[part_run], &o_durations[part_run]);
	}
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run]->join();
		delete threads[part_run];
		part_order_durations.push_back(o_durations[part_run]);
	}
	delete[] threads;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 7; run++)
		{
			Experiment::Tpch::QueryThreeJoinWorker worker(predicate);
			std::unordered_set<uint32_t> step_one_customer_buffer_copy(step_one_customer_buffer);
			std::unordered_map<uint32_t, Tpch::order> step_one_order_buffer_copy(step_one_order_buffer);
			std::unordered_map<uint32_t, Tpch::query_three_step_one> step_one_result_buffer_copy(step_one_result_buffer);
			// TIME CRITICAL - START
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			// feed customers first
			for (auto it = c_worker_input_buffer[i].cbegin(); it != c_worker_input_buffer[i].cend(); ++it)
			{
				worker.step_one_update(*it, partitioner_name);
			}
			// feed orders
			for (auto it = o_worker_input_buffer[i].cbegin(); it != o_worker_input_buffer[i].cend(); ++it)
			{
				worker.step_one_update(*it, partitioner_name);
			}
			// finalize
			if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("ca_aff_naive") == 0 || partitioner_name.compare("ca_aff_hll") == 0)
			{
				worker.step_one_finalize(step_one_result_buffer_copy);
			}
			else
			{
				worker.step_one_partial_finalize(step_one_customer_buffer_copy, step_one_order_buffer_copy, step_one_result_buffer_copy);
			}
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			// TIME CRITICAL - END
			std::chrono::duration<double, std::milli> execution_time = end - start;
			durations.push_back(execution_time.count());
			if (run >= 6)
			{
				step_one_customer_buffer = step_one_customer_buffer_copy;
				step_one_order_buffer = step_one_order_buffer_copy;
				step_one_result_buffer = step_one_result_buffer_copy;
			}
		}
		std::vector<double>::iterator max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);
		std::vector<double>::iterator min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		double sum_of_durations = std::accumulate(durations.begin(), durations.end(), 0.0);
		double avg_of_durations = sum_of_durations / durations.size();
		step_one_exec_durations[i] = avg_of_durations;
		c_worker_input_buffer[i].clear();
		o_worker_input_buffer[i].clear();
	}
	c_worker_input_buffer.clear();
	o_worker_input_buffer.clear();
	// step one aggregation
	for (size_t i = 0; i < 7; ++i)
	{
		broadcast_result.clear();
		std::unordered_map<uint32_t, Tpch::query_three_step_one> s_one_result_aux(step_one_result_buffer);
		std::unordered_set<uint32_t> s_one_c_aux(step_one_customer_buffer);
		std::unordered_map<uint32_t, Tpch::order> s_one_order_aux(step_one_order_buffer);
		Experiment::Tpch::QueryThreeOfflineAggregator aggregator;
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("ca_aff_naive") == 0 || partitioner_name.compare("ca_aff_hll") == 0)
		{
			aggregator.step_one_sort(s_one_result_aux, broadcast_result);
		}
		else
		{
			aggregator.step_one_materialize_and_sort(s_one_c_aux, s_one_order_aux, s_one_result_aux, broadcast_result);
		}
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> aggr_time = end - start;
		step_one_aggr_durations.push_back(aggr_time.count());
	}
	step_one_customer_buffer.clear();
	step_one_order_buffer.clear();
	step_one_result_buffer.clear();
	// step Two: join lineitem table and calculate group by aggregate-sum()
	// partition lineitem tuples
	threads = new std::thread*[7];
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run] = new std::thread(Experiment::Tpch::QueryThreePartition::thread_li_partition, (part_run == 0), partitioner_name,
			partitioner, &li_table, &li_worker_input_buffer, tasks.size(), &li_imbalance[part_run], &li_key_imbalance[part_run], &li_durations[part_run]);
	}
	for (size_t part_run = 0; part_run < 7; ++part_run)
	{
		threads[part_run]->join();
		delete threads[part_run];
		part_li_durations.push_back(li_durations[part_run]);
	}
	delete[] threads;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 7; run++)
		{
			std::vector<std::pair<std::string, Tpch::query_three_result>> result_buffer_copy(result_buffer);
			Experiment::Tpch::QueryThreeJoinWorker worker(predicate);
			worker.step_two_init(broadcast_result);
			// TIME CRITICAL - START
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			for (auto it = li_worker_input_buffer[i].begin(); it != li_worker_input_buffer[i].end(); ++it)
			{
				worker.step_two_update(*it);
			}
			worker.finalize(result_buffer_copy);
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			// TIME CRITICAL - END
			std::chrono::duration<double, std::milli> execution_time = end - start;
			if (run >= 6)
			{
				result_buffer = result_buffer_copy;
			}
			durations.push_back(execution_time.count());
		}
		li_worker_input_buffer[i].clear();
		std::vector<double>::iterator max_it = std::max_element(durations.begin(), durations.end());
		durations.erase(max_it);
		std::vector<double>::iterator min_it = std::min_element(durations.begin(), durations.end());
		durations.erase(min_it);
		double sum_of_durations = std::accumulate(durations.begin(), durations.end(), 0.0);
		double avg_of_durations = sum_of_durations / durations.size();
		step_two_exec_durations[i] = avg_of_durations;
	}
	li_worker_input_buffer.clear();
	std::vector<double> step_two_aggr_durations;
	for (size_t aggr_run = 0; aggr_run < 7; aggr_run++)
	{
		std::vector<std::pair<std::string, Experiment::Tpch::query_three_result>> final_result;
		Experiment::Tpch::QueryThreeOfflineAggregator aggregator;
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("ca_aff_naive") == 0 || partitioner_name.compare("ca_aff_hll") == 0)
		{
			aggregator.step_two_sort(result_buffer, final_result);
		}
		else
		{
			aggregator.step_two_calculate_and_sort(result_buffer, final_result);
		}
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		std::chrono::duration<double, std::milli> aggregation_time = end - start;
		step_two_aggr_durations.push_back(aggregation_time.count());
		if (aggr_run >= 6)
		{
			std::chrono::system_clock::time_point write_to_output_start = std::chrono::system_clock::now();
			aggregator.write_output_to_file(final_result, worker_output_file_name);
			std::chrono::system_clock::time_point write_to_output_end = std::chrono::system_clock::now();
			write_to_output_time = write_to_output_end - write_to_output_start;
		}
	}
	// S1 exec time
	std::vector<double>::iterator max_step_one_it = std::max_element(step_one_exec_durations.begin(), step_one_exec_durations.end());
	std::vector<double>::iterator min_step_one_it = std::min_element(step_one_exec_durations.begin(), step_one_exec_durations.end());
	double avg_step_one_exec_time = std::accumulate(step_one_exec_durations.begin(), step_one_exec_durations.end(), 0.0) / step_one_exec_durations.size();
	// S1 Aggr. time
	std::vector<double>::iterator max_s1_aggr_time = std::max_element(step_one_aggr_durations.begin(), step_one_aggr_durations.end());
	step_one_aggr_durations.erase(max_s1_aggr_time);
	std::vector<double>::iterator min_s1_aggr_time = std::min_element(step_one_aggr_durations.begin(), step_one_aggr_durations.end());
	step_one_aggr_durations.erase(min_s1_aggr_time);
	double mean_s1_aggr_duration = std::accumulate(step_two_aggr_durations.begin(), step_two_aggr_durations.end(), 0.0) / step_two_aggr_durations.size();
	// S2 exec time
	std::vector<double>::iterator max_step_two_it = std::max_element(step_two_exec_durations.begin(), step_two_exec_durations.end());
	std::vector<double>::iterator min_step_two_it = std::min_element(step_two_exec_durations.begin(), step_two_exec_durations.end());
	double avg_step_two_exec_time = std::accumulate(step_two_exec_durations.begin(), step_two_exec_durations.end(), 0.0) / step_two_exec_durations.size();
	// S2 Aggr. time
	std::vector<double>::iterator max_s2_aggr_time = std::max_element(step_two_aggr_durations.begin(), step_two_aggr_durations.end());
	step_two_aggr_durations.erase(max_s2_aggr_time);
	std::vector<double>::iterator min_s2_aggr_time = std::min_element(step_two_aggr_durations.begin(), step_two_aggr_durations.end());
	step_two_aggr_durations.erase(min_s2_aggr_time);
	double mean_s2_aggr_duration = std::accumulate(step_two_aggr_durations.begin(), step_two_aggr_durations.end(), 0.0) / step_two_aggr_durations.size();
	// Partition times
	std::vector<double>::iterator part_max_it = std::max_element(part_customer_durations.begin(), part_customer_durations.end());
	part_customer_durations.erase(part_max_it);
	std::vector<double>::iterator part_min_it = std::min_element(part_customer_durations.begin(), part_customer_durations.end());
	part_customer_durations.erase(part_min_it);
	double mean_part_customer_time = std::accumulate(part_customer_durations.begin(), part_customer_durations.end(), 0.0) / part_customer_durations.size();
	part_max_it = std::max_element(part_order_durations.begin(), part_order_durations.end());
	part_order_durations.erase(part_max_it);
	part_min_it = std::min_element(part_order_durations.begin(), part_order_durations.end());
	part_order_durations.erase(part_min_it);
	double mean_part_order_time = std::accumulate(part_order_durations.begin(), part_order_durations.end(), 0.0) / part_order_durations.size();
	part_max_it = std::max_element(part_li_durations.begin(), part_li_durations.end());
	part_li_durations.erase(part_max_it);
	part_min_it = std::min_element(part_li_durations.begin(), part_li_durations.end());
	part_li_durations.erase(part_min_it);
	double mean_part_li_time = std::accumulate(part_li_durations.begin(), part_li_durations.end(), 0.0) / part_li_durations.size();
	
	std::stringstream result_stream;
	result_stream << partitioner_name << "," << tasks.size() << "," << *max_step_one_it << "," << *min_step_one_it << "," << avg_step_one_exec_time << "," << mean_s1_aggr_duration << "," <<
		*max_step_two_it << "," << *min_step_two_it << "," << avg_step_two_exec_time << "," << mean_s2_aggr_duration << "," << write_to_output_time.count() << "," <<
		mean_part_customer_time << "," << mean_part_order_time << "," << mean_part_li_time << "," << c_imbalance[0] << "," << c_key_imbalance[0] << "," << o_imbalance[0] << 
		"," << o_key_imbalance[0] << "," << li_imbalance[0] << "," << li_key_imbalance[0] << "\n";
	std::string result_string = result_stream.str();
	std::cout << result_string;
	result_buffer.clear();
	step_one_result_buffer.clear();
}