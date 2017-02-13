#ifndef TPCH_QUERY_LIB_H_
#include "../include/tpch_query_lib.h"
#endif

void Experiment::Tpch::QueryOneWorker::update(const lineitem& line_item)
{
	const Tpch::date pred_date(1998, 11, 29);
	if (line_item.l_shipdate <= pred_date)
	{
		std::string key = std::to_string(line_item.l_returnflag) + std::to_string(line_item.l_linestatus);
		std::unordered_map<std::string, query_one_result>::iterator it = result.find(key);
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
			query_one_result tmp;
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

void Experiment::Tpch::QueryOneWorker::finalize(std::vector<query_one_result>& buffer) const
{
	for (auto it = result.cbegin(); it != result.cend(); ++it)
	{
		buffer.push_back(it->second);
	}
}

void Experiment::Tpch::QueryOneOfflineAggregator::order_result(const std::vector<query_one_result>& buffer, 
	std::map<std::string, query_one_result>& ordered_result)
{
	for (auto cit = buffer.cbegin(); cit != buffer.cend(); ++cit)
	{
		std::string key = cit->return_flag + "," + cit->line_status;
		ordered_result[key] = *cit;
	}
}

void Experiment::Tpch::QueryOneOfflineAggregator::order_result(const std::unordered_map<std::string, query_one_result>& buffer,
	std::map<std::string, query_one_result>& ordered_result)
{
	for (auto cit = buffer.cbegin(); cit != buffer.cend(); ++cit)
	{
		ordered_result[cit->first] = cit->second;
	}
}

void Experiment::Tpch::QueryOneOfflineAggregator::aggregate_final_result(const std::vector<query_one_result>& buffer, 
	std::unordered_map<std::string, query_one_result>& result)
{
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
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
	for (auto it = result.begin(); it != result.end(); ++it)
	{
		unsigned int count = it->second.count_order;
		it->second.avg_qty = it->second.avg_qty / count;
		it->second.avg_price = it->second.avg_price / count;
		it->second.avg_disc = it->second.avg_disc / count;
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

void Experiment::Tpch::QueryOnePartition::query_one_simulation(const std::vector<lineitem>& lines, const size_t task_num)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks;
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
	
	for (uint16_t i = 0; i < task_num; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)), fld(new HashFieldPartitioner(tasks)),
		pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
		ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)), ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
		ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)), la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
		la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator)), man(new CaPartitionLib::MultiAN<uint64_t>(tasks, naive_estimator)), 
	mpk(new MultiPkPartitioner(tasks));

	std::string sh_file_name = "shg_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string fld_file_name = "fld_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	// std::string naive_shed_fld_file_name = "naive_shed_fld_tpch_q1_" + std::to_string(tasks.size()) + "_results.csv";
	std::string pkg_file_name = "pkg_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_naive_file_name = "ca_naive_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_aff_naive_file_name = "ca_aff_naive_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_hll_file_name = "ca_hll_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string ca_aff_hll_file_name = "ca_aff_hll_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string la_naive_file_name = "la_naive_tpch_q1_" + std::to_string(tasks.size()) + ".csv";
	std::string la_hll_file_name = "la_hll_tpch_q1_" + std::to_string(tasks.size()) + ".csv";

	std::cout << "TPC-H Q1 ***\n";
	std::cout << "partitioner,task-num,max-exec-msec,min-exec-msec,avg-exec-msec,avg-aggr-msec,io-msec,avg-order-msec,imbalance,key-imbalance\n";
	query_one_partitioner_simulation(lines, tasks, rrg, "sh", sh_file_name);
	query_one_partitioner_simulation(lines, tasks, fld, "fld", fld_file_name);
	// query_one_partitioner_simulation(lines, tasks, naive_shed_fld, "naive_shed_fld", naive_shed_fld_file_name);
	query_one_partitioner_simulation(lines, tasks, pkg, "pk", pkg_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_naive, "cn", ca_naive_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_aff_naive, "an", ca_aff_naive_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_hll, "chll", ca_hll_file_name);
	query_one_partitioner_simulation(lines, tasks, ca_aff_hll, "ahll", ca_aff_hll_file_name);
	query_one_partitioner_simulation(lines, tasks, la_naive, "ln", la_naive_file_name);
	query_one_partitioner_simulation(lines, tasks, la_hll, "lhll", la_hll_file_name);
	query_one_partitioner_simulation(lines, tasks, man, "man", "man_tpch_q1.csv");
	query_one_partitioner_simulation(lines, tasks, mpk, "mpk", "mpk_tpch_q1.csv");
}

void Experiment::Tpch::QueryOnePartition::lineitem_partition(size_t task_num, std::unique_ptr<Partitioner>& partitioner, const std::vector<lineitem>& input_buffer, 
	std::vector<std::vector<lineitem>>& worker_input_buffer, float *imbalance, float* key_imbalance)
{
	TpchQueryOneKeyExtractor key_extractor;
	ImbalanceScoreAggr<lineitem, std::string> imbalance_aggregator(task_num, key_extractor);
	partitioner->init();
	for (auto it = input_buffer.cbegin(); it != input_buffer.cend(); ++it)
	{
		std::string key = key_extractor.extract_key(*it);
		uint16_t task = partitioner->partition_next(key.c_str(), strlen(key.c_str()));
		if (task < worker_input_buffer.size())
		{
			worker_input_buffer[task].push_back(*it);
			imbalance_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (size_t i = 0; i < task_num; i++)
	{
		worker_input_buffer[i].shrink_to_fit();
	}
	worker_input_buffer.shrink_to_fit();
	*imbalance = imbalance_aggregator.imbalance();
	*key_imbalance = imbalance_aggregator.cardinality_imbalance();
}

void Experiment::Tpch::QueryOnePartition::thread_worker_operate(const bool write, const std::vector<lineitem>* input_buffer, 
	std::vector<query_one_result>* result_buffer, double* operate_duration)
{
	QueryOneWorker worker;
	std::chrono::duration<double, std::milli> execution_time;
	std::vector<query_one_result> aux_buffer;
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

void Experiment::Tpch::QueryOnePartition::thread_aggregate(const bool write, const std::string partitioner_name, const std::vector<query_one_result>* input_buffer,
	std::map<std::string, query_one_result>* result, const std::string worker_output_file_name, double* aggregate_duration, double* order_duration, double* io_duration)
{
	QueryOneOfflineAggregator aggregator;
	std::chrono::system_clock::time_point start, end;
	std::map<std::string, query_one_result> aux_result;
	std::unordered_map < std::string, query_one_result> final_result;
	std::chrono::duration<double, std::milli> aggregation_time, order_time, write_output_duration;
	if (write)
	{
		
		if (partitioner_name.compare("fld") != 0 && partitioner_name.compare("cn") != 0 && partitioner_name.compare("ahll") != 0 && 
			partitioner_name.compare("man") != 0)
		{
			start = std::chrono::system_clock::now();
			aggregator.aggregate_final_result(*input_buffer, final_result);
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_result(final_result, *result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		else
		{
			start = std::chrono::system_clock::now();
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_result(*input_buffer, *result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		start = std::chrono::system_clock::now();
		aggregator.write_output_result(*result, worker_output_file_name);
		end = std::chrono::system_clock::now();
		write_output_duration = end - start;
	}
	else
	{
		if (partitioner_name.compare("fld") != 0 && partitioner_name.compare("cn") != 0 && partitioner_name.compare("ahll") != 0 && 
			partitioner_name.compare("man") != 0)
		{
			start = std::chrono::system_clock::now();
			aggregator.aggregate_final_result(*input_buffer, final_result);
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_result(final_result, aux_result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
		else
		{
			start = std::chrono::system_clock::now();
			end = std::chrono::system_clock::now();
			aggregation_time = end - start;
			start = std::chrono::system_clock::now();
			aggregator.order_result(*input_buffer, aux_result);
			end = std::chrono::system_clock::now();
			order_time = end - start;
		}
	}
	*aggregate_duration = aggregation_time.count();
	*order_duration = order_time.count();
	*io_duration = write_output_duration.count();
}

void Experiment::Tpch::QueryOnePartition::query_one_partitioner_simulation(const std::vector<lineitem>& lineitem_table, 
	const std::vector<uint16_t> tasks, std::unique_ptr<Partitioner>& partitioner, const std::string partitioner_name, const std::string worker_output_file_name)
{
	float imbalance, cardinality_imbalance;
	double aggregate_durations[5], order_durations[5], write_output_duration_in_msec;
	std::thread** threads;
	std::vector<double> exec_duration_vector, aggr_duration_vector, order_duration_vector;
	std::vector<query_one_result> intermediate_buffer;
	std::vector<std::vector<lineitem>> worker_input_buffer(tasks.size(), std::vector<lineitem>());
	std::map<std::string, query_one_result> result_buffer;
	// partition tuples
	lineitem_partition(tasks.size(), partitioner, lineitem_table, worker_input_buffer, &imbalance, &cardinality_imbalance);
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		double exec_durations[5];
		threads = new std::thread*[5];
		for (size_t part_run = 0; part_run < 5; ++part_run)
		{
			threads[part_run] = new std::thread(Experiment::Tpch::QueryOnePartition::thread_worker_operate, (part_run == 0), 
				&worker_input_buffer[i], &intermediate_buffer, &exec_durations[part_run]);
		}
		for (size_t part_run = 0; part_run < 5; ++part_run)
		{
			threads[part_run]->join();
			delete threads[part_run];
			durations.push_back(exec_durations[part_run]);
		}
		delete[] threads;
		worker_input_buffer[i].clear();
		auto it = std::max_element(durations.begin(), durations.end());
		durations.erase(it);
		it = std::min_element(durations.begin(), durations.end());
		durations.erase(it);
		double average_duration = std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size();
		exec_duration_vector.push_back(average_duration);
	}
	threads = new std::thread*[5];
	for (size_t run = 0; run < 5; ++run)
	{
		threads[run] = new std::thread(Experiment::Tpch::QueryOnePartition::thread_aggregate, (run == 0), partitioner_name, 
			&intermediate_buffer, &result_buffer, worker_output_file_name, &aggregate_durations[run], &order_durations[run], &write_output_duration_in_msec);
	}
	for (size_t run = 0; run < 5; ++run)
	{
		threads[run]->join();
		delete threads[run];
		aggr_duration_vector.push_back(aggregate_durations[run]);
		order_duration_vector.push_back(order_durations[run]);
	}
	delete[] threads;
	auto it = std::max_element(aggr_duration_vector.begin(), aggr_duration_vector.end());
	aggr_duration_vector.erase(it);
	it = std::min_element(aggr_duration_vector.begin(), aggr_duration_vector.end());
	aggr_duration_vector.erase(it);
	double average_aggr_duration = std::accumulate(aggr_duration_vector.begin(), aggr_duration_vector.end(), 0.0) / 
		aggr_duration_vector.size();
	it = std::max_element(exec_duration_vector.begin(), exec_duration_vector.end());
	exec_duration_vector.erase(it);
	it = std::min_element(exec_duration_vector.begin(), exec_duration_vector.end());
	exec_duration_vector.erase(it);
	double avg_exec_time = std::accumulate(exec_duration_vector.begin(), exec_duration_vector.end(), 0.0) / exec_duration_vector.size();
	it = std::max_element(order_duration_vector.begin(), order_duration_vector.end());
	order_duration_vector.erase(it);
	it = std::min_element(order_duration_vector.begin(), order_duration_vector.end());
	order_duration_vector.erase(it);
	double avg_order_time = std::accumulate(order_duration_vector.begin(), order_duration_vector.end(), 0.0) / order_duration_vector.size();
	std::string result = partitioner_name + "," + std::to_string(tasks.size()) + "," + 
		std::to_string(*std::max_element(exec_duration_vector.begin(), exec_duration_vector.end())) + "," + 
		std::to_string(*std::min_element(exec_duration_vector.begin(), exec_duration_vector.end())) + "," +
		std::to_string(avg_exec_time) + "," + std::to_string(average_aggr_duration) + "," + std::to_string(write_output_duration_in_msec) + "," +
		std::to_string(avg_order_time) + "," + std::to_string(imbalance) + "," + std::to_string(cardinality_imbalance) + "\n";
	std::cout << result;
	intermediate_buffer.clear();
}

Experiment::Tpch::QueryThreeWorker::QueryThreeWorker(const query_three_predicate & predicate)
{
	this->predicate = predicate;
}

void Experiment::Tpch::QueryThreeWorker::step_one_update(const q3_customer& customer, std::string partitioner_name)
{
	if (strncmp(this->predicate.c_mktsegment, customer.c_mktsegment, 10) == 0)
	{
		this->cu_index.insert(customer.c_custkey);
	}
}

void Experiment::Tpch::QueryThreeWorker::step_one_update(const order & order, std::string partitioner_name)
{
	if (order.o_orderdate.day < predicate.order_date.day)
	{
		auto customer_iterator = cu_index.find(order.o_custkey);
		if (customer_iterator != cu_index.end())
		{
			step_one_result[order.o_orderkey] = query_three_step_one(order.o_orderdate, order.o_shippriority);
		}
		else
		{
			// the order needs to be added, because during aggregation there might be unmatched orders
			o_index[order.o_orderkey] = order;
		}
	}
}

void Experiment::Tpch::QueryThreeWorker::step_one_finalize(std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer)
{
	// attempt to match all un-matched tuples
	for (auto order_index_it = o_index.cbegin(); order_index_it != o_index.cend(); ++order_index_it)
	{
		auto customer_it = cu_index.find(order_index_it->second.o_custkey);
		if (customer_it != cu_index.end())
		{
			// materialize the result if it does not exist in the result buffer
			auto result_buffer_it = step_one_result_buffer.find(order_index_it->first);
			if (result_buffer_it == step_one_result_buffer.end())
			{
				step_one_result_buffer[order_index_it->first] = query_three_step_one(order_index_it->second.o_orderdate, 
					order_index_it->second.o_shippriority);
			}
		}
	}
	// transfer all (non-existent) results in the result-buffer
	for (auto it = this->step_one_result.cbegin(); it != this->step_one_result.cend(); ++it)
	{
		auto result_buffer_it = step_one_result_buffer.find(it->first);
		if (result_buffer_it == step_one_result_buffer.end())
		{
			step_one_result_buffer[it->first] = it->second;
		}
	}
}

void Experiment::Tpch::QueryThreeWorker::step_one_partial_finalize(std::unordered_set<uint32_t>& c_index, std::unordered_map<uint32_t,order>& o_index, 
	std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer) const
{
	// gather all qualifying customers
	for (auto c_it = this->cu_index.cbegin(); c_it != this->cu_index.cend(); ++c_it)
	{
		auto c_index_it = c_index.find(*c_it);
		if (c_index_it == c_index.end())
		{
			c_index.insert(*c_it);
		}
	}
	// go over all un-matched orders to check if they match with any qualifying customer
	for (auto o_it = this->o_index.cbegin(); o_it != this->o_index.cend(); ++o_it)
	{
		auto c_it = c_index.find(o_it->second.o_custkey);
		if (c_it != c_index.end())
		{
			auto result_buffer_it = step_one_result_buffer.find(o_it->first);
			if (result_buffer_it == step_one_result_buffer.end())
			{
				step_one_result_buffer[o_it->first] = query_three_step_one(o_it->second.o_orderdate, o_it->second.o_shippriority);
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
	for (auto r_it = this->step_one_result.cbegin(); r_it != this->step_one_result.cend(); ++r_it)
	{
		auto rb_it = step_one_result_buffer.find(r_it->first);
		if (rb_it == step_one_result_buffer.end())
		{
			step_one_result_buffer[r_it->first] = r_it->second;
		}
	}
}

void Experiment::Tpch::QueryThreeWorker::step_two_init(const std::unordered_map<uint32_t, query_three_step_one>& step_one_result)
{
	this->step_one_result.clear();
	this->step_one_result = step_one_result;
}

void Experiment::Tpch::QueryThreeWorker::step_two_update(const lineitem & line_item)
{
	if (line_item.l_shipdate.day > this->predicate.order_date.day)
	{
		auto it = step_one_result.find(line_item.l_order_key);
		if (it != step_one_result.end())
		{
			auto key = std::to_string(line_item.l_order_key) + "," +
				it->second.o_orderdate.to_string() + "," + std::to_string(it->second.o_shippriority);
			auto revenue_update = (line_item.l_extendedprice * (1 - line_item.l_discount));
			auto result_it = final_result.find(key);
			if (result_it != final_result.end())
			{
				result_it->second.revenue += revenue_update;
			}
			else
			{
				final_result.insert(std::make_pair(key, query_three_result(it->first, it->second.o_shippriority, 
					it->second.o_orderdate, revenue_update)));
			}
		}
	}
}

void Experiment::Tpch::QueryThreeWorker::finalize(std::vector<std::pair<std::string, query_three_result>>& result_buffer) const
{
	for (auto it = final_result.cbegin(); it != final_result.cend(); ++it)
		result_buffer.push_back(std::make_pair(it->first, it->second));
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_one_transfer(const std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer, std::unordered_map<uint32_t, query_three_step_one>& result_buffer) 
{
	result_buffer = step_one_result_buffer;
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_one_materialize(const std::unordered_set<uint32_t>& c_index, const std::unordered_map<uint32_t, order>& o_index, 
	std::unordered_map<uint32_t, query_three_step_one>& step_one_result_buffer, std::unordered_map<uint32_t, query_three_step_one>& result_buffer)
{
	for (auto o_it = o_index.cbegin(); o_it != o_index.cend(); ++o_it)
	{
		auto c_it = c_index.find(o_it->second.o_custkey);
		if (c_it != c_index.end())
		{
			auto r_it = step_one_result_buffer.find(o_it->first);
			if (r_it == step_one_result_buffer.end())
				step_one_result_buffer[o_it->first] = Tpch::query_three_step_one(o_it->second.o_orderdate, o_it->second.o_shippriority);
		}
	}
	result_buffer = step_one_result_buffer;
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_two_order(const std::vector<std::pair<std::string, query_three_result>>& buffer, 
	std::vector<std::pair<std::string, query_three_result>>& final_result)
{
	std::map<float, std::vector<std::pair<std::string, query_three_result>>> index_result;
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		auto index_it = index_result.find(it->second.revenue);
		if (index_it != index_result.end())
		{
			index_it->second.push_back(*it);
		}
		else
		{
			index_result[it->second.revenue] = std::vector<std::pair<std::string, query_three_result>>();
			index_result[it->second.revenue].push_back(*it);
		}
	}
	for (auto it = index_result.crbegin(); it != index_result.crend(); ++it)
	{
		for (auto it_2 = it->second.cbegin(); it_2 != it->second.cend(); ++it_2)
			final_result.push_back(*it_2);
		if (final_result.size() >= 20)
			break;
	}
}

void Experiment::Tpch::QueryThreeOfflineAggregator::step_two_order(const std::unordered_map<std::string, query_three_result>& buffer, 
	std::vector<std::pair<std::string, query_three_result>>& result)
{
	std::map<float, std::vector<std::pair<std::string, query_three_result>>> index_result;
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		auto index_it = index_result.find(it->second.revenue);
		if (index_it != index_result.end())
		{
			index_it->second.push_back(*it);
		}
		else
		{
			index_result[it->second.revenue] = std::vector<std::pair<std::string, query_three_result>>();
			index_result[it->second.revenue].push_back(*it);
		}
	}
	for (auto it = index_result.crbegin(); it != index_result.crend(); ++it)
	{
		for (auto it_2 = it->second.cbegin(); it_2 != it->second.cend(); ++it_2)
			result.push_back(*it_2);
		if (result.size() >= 20)
			break;
	}
}


void Experiment::Tpch::QueryThreeOfflineAggregator::step_two_materialize(const std::vector<std::pair<std::string, query_three_result>>& buffer,
	std::unordered_map<std::string, query_three_result>& result)
{
	for (auto it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		auto f_it = result.find(it->first);
		if (f_it != result.end())
			f_it->second.revenue += it->second.revenue;
		else 
			result[it->first] = it->second;
	}
}

void Experiment::Tpch::QueryThreeOfflineAggregator::write_output_to_file(const std::vector<std::pair<std::string, query_three_result>>& result, 
	const std::string & output_file)
{
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	for (auto i = result.cbegin(); i != result.cend(); ++i)
	{
		std::stringstream stream;
		stream << std::fixed << std::setprecision(2) << i->second.revenue;
		auto buffer = i->first + "," + stream.str() + "," +
			i->second.o_orderdate.to_string() + "," + std::to_string(i->second.o_shippriority) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::QueryThreePartition::query_three_simulation(const std::vector<q3_customer>& c_table, 
	const std::vector<lineitem>& li_table, const std::vector<order>& o_table, const size_t task_num)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks;
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
	for (uint16_t i = 0; i < task_num; i++)
	{
		tasks.push_back(i);
	}
	tasks.shrink_to_fit();
	std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)), fld(new HashFieldPartitioner(tasks)),
		pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)), ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
		ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)), ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
		ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)), la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
		la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator)), man(new CaPartitionLib::MultiAN<uint64_t>(tasks, naive_estimator)),
		mpk(new MultiPkPartitioner(tasks));

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
		"avg-order-msec,c-imb,c-key-imb,o-imb,o-key-imb,li-imb,li-key-imb\n";
	std::string info_message = info_stream.str();
	std::cout << info_message;
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, rrg, "sh", sh_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, fld, "fld", fld_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, pkg, "pk", pkg_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_naive, "cn", ca_naive_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_aff_naive, "an", ca_aff_naive_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_hll, "chll", ca_hll_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, ca_aff_hll, "ahll", ca_aff_hll_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, la_naive, "ln", la_naive_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, la_hll, "lhll", la_hll_file_name);
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, man, "man", "man_tpch_q2.csv");
	query_three_partitioner_simulation(c_table, li_table, o_table, tasks, mpk, "mpk", "mpk_tpch_q2.csv");
}

void Experiment::Tpch::QueryThreePartition::customer_partition(std::unique_ptr<Partitioner>& partitioner, const std::vector<q3_customer>& c_table,
	std::vector<std::vector<q3_customer>>& c_worker_input_buffer, float* imbalance, float* key_imbalance)
{
	TpchQueryThreeCustomerKeyExtractor c_key_extractor;
	ImbalanceScoreAggr<q3_customer, uint32_t> c_imbalance_aggregator(c_worker_input_buffer.size(), c_key_extractor);
	std::unordered_set<uint32_t> distinct_customers;
	for (auto it = c_table.cbegin(); it != c_table.cend(); ++it)
	{
		uint16_t task = partitioner->partition_next(&it->c_custkey, sizeof(uint32_t));
		//distinct_customers.insert(it->c_custkey);
		if (task < c_worker_input_buffer.size())
		{
			c_worker_input_buffer[task].push_back(*it);
			c_imbalance_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (size_t i = 0; i < c_worker_input_buffer.size(); ++i)
		c_worker_input_buffer[i].shrink_to_fit();
	c_worker_input_buffer.shrink_to_fit();
	*imbalance = c_imbalance_aggregator.imbalance();
	*key_imbalance = c_imbalance_aggregator.cardinality_imbalance();
	//std::cout << "number of distinct customer keys: " << distinct_customers.size() << "\n";
}

void Experiment::Tpch::QueryThreePartition::order_partition(std::unique_ptr<Partitioner>& partitioner, const std::vector<order>& o_table,
	std::vector<std::vector<order>>& o_worker_input_buffer, float* imbalance, float* key_imbalance)
{
	TpchQueryThreeOrderKeyExtractor o_key_extractor;
	ImbalanceScoreAggr<Experiment::Tpch::order, uint32_t> o_imbalance_aggregator(o_worker_input_buffer.size(), o_key_extractor);
	std::unordered_set<uint32_t> distinct_orders;
	for (auto it = o_table.cbegin(); it != o_table.cend(); ++it)
	{
		uint16_t task = partitioner->partition_next(&it->o_custkey, sizeof(uint32_t));
		//distinct_orders.insert(it->o_custkey);
		if (task < o_worker_input_buffer.size())
		{
			o_worker_input_buffer[task].push_back(*it);
			o_imbalance_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (size_t i = 0; i < o_worker_input_buffer.size(); ++i)
		o_worker_input_buffer[i].shrink_to_fit();
	o_worker_input_buffer.shrink_to_fit();
	*imbalance = o_imbalance_aggregator.imbalance();
	*key_imbalance = o_imbalance_aggregator.cardinality_imbalance();
	//std::cout << "number of distinct order keys: " << distinct_orders.size() << "\n";
}

void Experiment::Tpch::QueryThreePartition::lineitem_partition(std::unique_ptr<Partitioner>& partitioner, const std::vector<lineitem>& li_table,
	std::vector<std::vector<lineitem>>& li_worker_input_buffer, float* imbalance, float* key_imbalance)
{
	TpchQueryThreeLineitemKeyExtractor li_key_extractor;
	std::unordered_set<uint32_t> distinct_lineitems;
	ImbalanceScoreAggr<Experiment::Tpch::lineitem, uint32_t> li_imbalance_aggregator(li_worker_input_buffer.size(), li_key_extractor);
	for (auto it = li_table.cbegin(); it != li_table.cend(); ++it)
	{
		uint16_t task = partitioner->partition_next(&it->l_order_key, sizeof(it->l_order_key));
		//distinct_lineitems.insert(it->l_order_key);
		if (task < li_worker_input_buffer.size())
		{
			li_worker_input_buffer[task].push_back(*it);
			li_imbalance_aggregator.incremental_measure_score(task, *it);
		}
	}
	for (size_t i = 0; i < li_worker_input_buffer.size(); ++i)
		li_worker_input_buffer[i].shrink_to_fit();
	li_worker_input_buffer.shrink_to_fit();
	*imbalance = li_imbalance_aggregator.imbalance();
	*key_imbalance = li_imbalance_aggregator.cardinality_imbalance();
	//std::cout << "number of distinct lineitem keys: " << distinct_lineitems.size() << "\n";
}

void Experiment::Tpch::QueryThreePartition::query_three_partitioner_simulation(const std::vector<q3_customer>& c_table, const std::vector<lineitem>& li_table,
	const std::vector<order>& o_table, const std::vector<uint16_t>& tasks, std::unique_ptr<Partitioner>& partitioner, const std::string partitioner_name,
	const std::string worker_output_file_name)
{
	std::vector<double> step_one_exec_durations(tasks.size(), double(0)), step_two_exec_durations(tasks.size(), double(0)), 
		step_one_aggr_durations, step_two_aggr_durations, step_two_order_durations;
	std::chrono::duration<double, std::milli> write_to_output_time;
	query_three_predicate predicate;
	std::vector<std::vector<q3_customer>> c_worker_input_buffer(tasks.size(), std::vector<q3_customer>());
	std::vector<std::vector<lineitem>> li_worker_input_buffer(tasks.size(), std::vector<lineitem>());
	std::vector<std::vector<order>> o_worker_input_buffer(tasks.size(), std::vector<order>());
	std::unordered_set<uint32_t> step_one_customer_buffer;
	std::unordered_map<uint32_t, order> step_one_order_buffer;
	std::unordered_map<uint32_t, query_three_step_one> step_one_result_buffer, broadcast_result;
	std::vector<std::pair<std::string, query_three_result>> result_buffer;
	float c_imbalance, c_key_imbalance, o_imbalance, o_key_imbalance, li_imbalance, li_key_imbalance;
	std::thread** threads;
	// initialize predicate
	predicate.order_date.day = 15;
	predicate.order_date.month = 3;
	predicate.order_date.year = 1995;
	memcpy(predicate.c_mktsegment, "BUILDING\0\0", 10 * sizeof(char));
	// Step One: Join customer and order tables
	// partition order tuples and customer tuples based on cust_key
	/**
	 * the partitioner should not be initialized after partitioning customers, 
	 * because it needs to know how much workload has been sent to each worker 
	 * and which cust_key is on which worker.
	 */
	customer_partition(partitioner, c_table, c_worker_input_buffer, &c_imbalance, &c_key_imbalance);
	order_partition(partitioner, o_table, o_worker_input_buffer, &o_imbalance, &o_key_imbalance);
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 5; ++run)
		{
			QueryThreeWorker worker(predicate);
			std::unordered_set<uint32_t> step_one_customer_buffer_copy(step_one_customer_buffer);
			std::unordered_map<uint32_t, order> step_one_order_buffer_copy(step_one_order_buffer);
			std::unordered_map<uint32_t, query_three_step_one> step_one_result_buffer_copy(step_one_result_buffer);
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
			if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("an") == 0 || partitioner_name.compare("ahll") == 0 || partitioner_name.compare("man") == 0)
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
			if (run >= 4)
			{
				step_one_customer_buffer = step_one_customer_buffer_copy;
				step_one_order_buffer = step_one_order_buffer_copy;
				step_one_result_buffer = step_one_result_buffer_copy;
			}
		}
		auto it = std::max_element(durations.begin(), durations.end());
		durations.erase(it);
		it = std::min_element(durations.begin(), durations.end());
		durations.erase(it);
		double avg_of_durations = std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size();
		step_one_exec_durations[i] = avg_of_durations;
		c_worker_input_buffer[i].clear();
		o_worker_input_buffer[i].clear();
	}
	c_worker_input_buffer.clear();
	o_worker_input_buffer.clear();
	// step one aggregation
	for (size_t i = 0; i < 5; ++i)
	{
		broadcast_result.clear();
		std::unordered_map<uint32_t, query_three_step_one> s_one_result_aux(step_one_result_buffer);
		std::unordered_set<uint32_t> s_one_c_aux(step_one_customer_buffer);
		std::unordered_map<uint32_t, order> s_one_order_aux(step_one_order_buffer);
		QueryThreeOfflineAggregator aggregator;
		if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("an") == 0 || partitioner_name.compare("ahll") == 0 || partitioner_name.compare("man") == 0)
		{
			aggregator.step_one_transfer(s_one_result_aux, broadcast_result);
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			std::chrono::duration<double, std::milli> aggr_time = end - start;
			step_one_aggr_durations.push_back(aggr_time.count());
		}
		else
		{
			aggregator.step_one_materialize(s_one_c_aux, s_one_order_aux, s_one_result_aux, broadcast_result);
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			std::chrono::duration<double, std::milli> aggr_time = end - start;
			step_one_aggr_durations.push_back(aggr_time.count());
		}
	}
	step_one_customer_buffer.clear();
	step_one_order_buffer.clear();
	step_one_result_buffer.clear();
	// step Two: join lineitem table and calculate group by aggregate-sum()
	// partition lineitem tuples - Need to initialize since the S1 result is broadcasted to all workers
	partitioner->init();
	lineitem_partition(partitioner, li_table, li_worker_input_buffer, &li_imbalance, &li_key_imbalance);
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		std::vector<double> durations;
		for (size_t run = 0; run < 5; ++run)
		{
			std::vector<std::pair<std::string, Tpch::query_three_result>> result_buffer_copy(result_buffer);
			QueryThreeWorker worker(predicate);
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
			if (run >= 4)
			{
				result_buffer = result_buffer_copy;
			}
			durations.push_back(execution_time.count());
		}
		li_worker_input_buffer[i].clear();
		auto it = std::max_element(durations.begin(), durations.end());
		durations.erase(it);
		it = std::min_element(durations.begin(), durations.end());
		durations.erase(it);
		double avg_of_durations = std::accumulate(durations.begin(), durations.end(), 0.0) / durations.size();
		step_two_exec_durations[i] = avg_of_durations;
	}
	li_worker_input_buffer.clear();
	for (size_t aggr_run = 0; aggr_run < 5; aggr_run++)
	{
		std::vector<std::pair<std::string, query_three_result>> final_result;
		std::unordered_map<std::string, query_three_result> full_result_buffer;
		QueryThreeOfflineAggregator aggregator;
		if (partitioner_name.compare("fld") == 0 || partitioner_name.compare("ca_aff_naive") == 0 || partitioner_name.compare("ca_aff_hll") == 0)
		{
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			step_two_aggr_durations.push_back((end - start).count());
			start = std::chrono::system_clock::now();
			aggregator.step_two_order(result_buffer, final_result);
			end = std::chrono::system_clock::now();
			step_two_order_durations.push_back((end - start).count());
		}
		else
		{
			std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
			aggregator.step_two_materialize(result_buffer, full_result_buffer);
			std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
			step_two_aggr_durations.push_back((end - start).count());
			start = std::chrono::system_clock::now();
			aggregator.step_two_order(full_result_buffer, final_result);
			end = std::chrono::system_clock::now();
			step_two_order_durations.push_back((end - start).count());
		}
		if (aggr_run >= 4)
		{
			std::chrono::system_clock::time_point write_to_output_start = std::chrono::system_clock::now();
			aggregator.write_output_to_file(final_result, worker_output_file_name);
			std::chrono::system_clock::time_point write_to_output_end = std::chrono::system_clock::now();
			write_to_output_time = write_to_output_end - write_to_output_start;
		}
	}
	// S1 exec time
	auto max_step_one_it = std::max_element(step_one_exec_durations.begin(), step_one_exec_durations.end());
	auto min_step_one_it = std::min_element(step_one_exec_durations.begin(), step_one_exec_durations.end());
	double avg_step_one_exec_time = std::accumulate(step_one_exec_durations.begin(), step_one_exec_durations.end(), 0.0) / step_one_exec_durations.size();
	// S1 Aggr. time
	auto max_s1_aggr_time = std::max_element(step_one_aggr_durations.begin(), step_one_aggr_durations.end());
	step_one_aggr_durations.erase(max_s1_aggr_time);
	auto min_s1_aggr_time = std::min_element(step_one_aggr_durations.begin(), step_one_aggr_durations.end());
	step_one_aggr_durations.erase(min_s1_aggr_time);
	double mean_s1_aggr_duration = std::accumulate(step_two_aggr_durations.begin(), step_two_aggr_durations.end(), 0.0) / step_two_aggr_durations.size();
	// S2 exec time
	auto max_step_two_it = std::max_element(step_two_exec_durations.begin(), step_two_exec_durations.end());
	auto min_step_two_it = std::min_element(step_two_exec_durations.begin(), step_two_exec_durations.end());
	double avg_step_two_exec_time = std::accumulate(step_two_exec_durations.begin(), step_two_exec_durations.end(), 0.0) / step_two_exec_durations.size();
	// S2 Aggr. time
	auto max_s2_aggr_time = std::max_element(step_two_aggr_durations.begin(), step_two_aggr_durations.end());
	step_two_aggr_durations.erase(max_s2_aggr_time);
	auto min_s2_aggr_time = std::min_element(step_two_aggr_durations.begin(), step_two_aggr_durations.end());
	step_two_aggr_durations.erase(min_s2_aggr_time);
	double mean_s2_aggr_duration = std::accumulate(step_two_aggr_durations.begin(), step_two_aggr_durations.end(), 0.0) / step_two_aggr_durations.size();
	// S2 Order time
	auto it = std::max_element(step_two_order_durations.begin(), step_two_order_durations.end());
	step_two_order_durations.erase(it);
	it = std::min_element(step_two_order_durations.begin(), step_two_order_durations.end());
	step_two_order_durations.erase(it);
	double mean_s2_order_time = std::accumulate(step_two_order_durations.begin(), step_two_order_durations.end(), 0.0) / step_two_order_durations.size();
	std::stringstream result_stream;
	result_stream << partitioner_name << "," << tasks.size() << "," << *max_step_one_it << "," << *min_step_one_it << "," << avg_step_one_exec_time << "," << mean_s1_aggr_duration << "," <<
		*max_step_two_it << "," << *min_step_two_it << "," << avg_step_two_exec_time << "," << mean_s2_aggr_duration << "," << write_to_output_time.count() << "," <<
		mean_s2_order_time << "," << c_imbalance << "," << c_key_imbalance << "," << o_imbalance << "," << o_key_imbalance << "," << li_imbalance << "," << li_key_imbalance << "\n";
	std::string result_string = result_stream.str();
	std::cout << result_string;
	result_buffer.clear();
	step_one_result_buffer.clear();
}