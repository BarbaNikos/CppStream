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

void Experiment::Tpch::QueryOneWorker::update(Experiment::Tpch::lineitem& line_item)
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

void Experiment::Tpch::QueryOneOfflineAggregator::sort_final_result(const std::vector<query_one_result>& buffer, const std::string & output_file)
{
	// do nothing here - just print out the result
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	/*std::map<std::string, query_one_result> final_result;
	for (std::vector<query_one_result>::const_iterator cit = buffer.cbegin(); cit != buffer.cend(); ++cit)
	{
		std::string key = cit->return_flag + "," + cit->line_status;
		final_result[key] = *cit;
	}
	for (std::map<std::string, query_one_result>::const_iterator it = final_result.begin(); it != final_result.end(); ++it)
	{
		std::string buffer = it->second.to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}*/
	for (std::vector<query_one_result>::const_iterator it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		std::string buffer = it->to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::QueryOneOfflineAggregator::calculate_and_produce_final_result(const std::vector<query_one_result>& buffer, const std::string & output_file)
{
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	// first merge results
	std::map<std::string, query_one_result> final_result;
	for (std::vector<query_one_result>::const_iterator it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		std::string key = it->return_flag + "," + it->line_status;
		auto i = final_result.find(key);
		if (i != final_result.end())
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
			final_result[key] = *it;
		}
	}
	// and then produce them
	for (std::map<std::string, query_one_result>::const_iterator it = final_result.begin(); it != final_result.end(); ++it)
	{
		std::string buffer = it->second.to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::QueryOnePartition::query_one_simulation(const std::vector<Experiment::Tpch::lineitem>& lines, const size_t task_num)
{
	RoundRobinPartitioner* rrg;
	PkgPartitioner* pkg;
	HashFieldPartitioner* fld;
	CardinalityAwarePolicy ca_policy;
	CaPartitionLib::CA_Exact_Partitioner* ca_naive;
	LoadAwarePolicy la_policy;
	CaPartitionLib::CA_Exact_Partitioner* la_naive;
	std::vector<uint16_t> tasks;

	for (uint16_t i = 0; i < task_num; i++)
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
	query_one_partitioner_simulation(lines, tasks, *rrg, "shg", "shuffle_q1_result.csv");
	query_one_partitioner_simulation(lines, tasks, *fld, "fld", "fld_q1_result.csv");
	query_one_partitioner_simulation(lines, tasks, *pkg, "pkg", "pkg_q1_result.csv");
	query_one_partitioner_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_q1_result.csv");
	query_one_partitioner_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_q1_result.csv");
	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void Experiment::Tpch::QueryOnePartition::query_one_partitioner_simulation(const std::vector<Experiment::Tpch::lineitem>& lineitem_table, 
	const std::vector<uint16_t> tasks, Partitioner & partitioner, const std::string partitioner_name, const std::string worker_output_file_name_prefix)
{
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::queue<Experiment::Tpch::lineitem> queue;
	std::mutex mu;
	std::condition_variable cond;
	std::vector<Tpch::query_one_result> intermediate_buffer;
	std::vector<std::vector<Tpch::lineitem>> worker_input_buffer(tasks.size(), std::vector<Tpch::lineitem>());
	
	// partition tuples
	for (auto it = lineitem_table.cbegin(); it != lineitem_table.cend(); ++it)
	{
		std::string key = std::to_string(it->l_returnflag) + "," + std::to_string(it->l_linestatus);
		uint16_t task = partitioner.partition_next(key.c_str(), key.length());
		worker_input_buffer[task].push_back(*it);
	}
	worker_input_buffer.shrink_to_fit();

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::Tpch::QueryOneWorker worker(&queue, &mu, &cond);
		// TIME CRITICAL - START
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = worker_input_buffer[i].begin(); it != worker_input_buffer[i].end(); ++it)
		{
			worker.update(*it);
		}
		worker.finalize(intermediate_buffer);
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
	Experiment::Tpch::QueryOneOfflineAggregator aggregator;
	// TIME CRITICAL - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_produce_final_result(intermediate_buffer, worker_output_file_name_prefix);
	}
	else
	{
		aggregator.sort_final_result(intermediate_buffer, worker_output_file_name_prefix);
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";
	intermediate_buffer.clear();
}

Experiment::Tpch::LineitemOrderWorker::LineitemOrderWorker(std::queue<Experiment::Tpch::lineitem>* li_queue, std::mutex * li_mu, std::condition_variable * li_cond, std::queue<Experiment::Tpch::order>* o_queue, std::mutex * o_mu, std::condition_variable * o_cond)
{
	this->li_queue = li_queue;
	this->li_mu = li_mu;
	this->li_cond = li_cond;
	this->o_queue = o_queue;
	this->o_mu = o_mu;
	this->o_cond = o_cond;
}

Experiment::Tpch::LineitemOrderWorker::~LineitemOrderWorker()
{
	result.clear();
	order_index.clear();
	li_index.clear();
}

void Experiment::Tpch::LineitemOrderWorker::operate()
{
}

void Experiment::Tpch::LineitemOrderWorker::update(Tpch::lineitem & line_item, bool partial_flag)
{
	auto order_index_it = this->order_index.find(line_item.l_order_key);
	if (partial_flag)
	{
		if (order_index_it != order_index.end())
		{
			this->result[line_item.l_order_key ^ line_item.l_linenumber] = Tpch::lineitem_order(order_index_it->second, line_item);
		}
		li_index[line_item.l_order_key ^ line_item.l_linenumber] = line_item;
	}
	else
	{
		if (order_index_it != order_index.end())
		{
			//std::cout << "update():: with partial-flag set to: " << partial_flag << ", found a match: [" << line_item.l_order_key << "," << 
			//	line_item.l_linenumber << "].\n";
			result[line_item.l_order_key ^ line_item.l_linenumber] = Tpch::lineitem_order(order_index_it->second, line_item);
		}
		else
		{
			li_index[line_item.l_order_key ^ line_item.l_linenumber] = line_item;
		}
	}
}

void Experiment::Tpch::LineitemOrderWorker::update(Tpch::order & o)
{
	auto order_index_it = order_index.find(o.o_orderkey);
	if (order_index_it == order_index.end())
	{
		order_index[o.o_orderkey] = o;
	}
}

void Experiment::Tpch::LineitemOrderWorker::finalize(std::vector<lineitem_order>& buffer)
{
	for (std::unordered_map<uint32_t, Tpch::lineitem>::const_iterator l_it = li_index.cbegin(); l_it != li_index.cend(); ++l_it)
	{
		auto o_it = order_index.find(l_it->second.l_order_key);
		if (o_it != order_index.end())
		{
			//std::cout << "LineitemOrderWorker::finalize(): result: " << l_it->second.to_string() << ".\n";
			buffer.push_back(Tpch::lineitem_order(o_it->second, l_it->second));
		}
	}
	for (std::unordered_map<uint32_t, lineitem_order>::const_iterator r_it = result.cbegin(); r_it != result.cend(); ++r_it)
	{
		buffer.push_back(r_it->second);
	}
}

void Experiment::Tpch::LineitemOrderWorker::partial_finalize(std::unordered_map<uint32_t, Tpch::lineitem>& li_buffer, std::unordered_map<uint32_t, Tpch::order>& o_buffer, 
	std::unordered_map<uint32_t, lineitem_order>& result_buffer)
{
	// first calculate remaining results and append records in the li_buffer only if they are not joined
	for (std::unordered_map<uint32_t, Tpch::lineitem>::const_iterator l_it = li_index.cbegin(); l_it != li_index.cend(); ++l_it)
	{
		auto o_it = order_index.find(l_it->second.l_order_key);
		if (o_it != order_index.end())
		{
			auto result_it = result.find(l_it->first);
			if (result_it == result.end())
			{
				result[l_it->first] = Tpch::lineitem_order(o_it->second, l_it->second);
			}
			else
			{
				// THE FOLLOWING IS AN OPTIMIZATION: it can be avoided and have a naive way of iterating
				// over the lineitem buffer one more time to push the line-items in the li_buffer
				auto li_buffer_it = li_buffer.find(l_it->first);
				if (li_buffer_it == li_buffer.end())
				{
					li_buffer[l_it->first] = l_it->second;
				}
			}
		}
	}
	// append records to o_buffer
	for (std::unordered_map<uint32_t, order>::const_iterator o_it = order_index.cbegin(); o_it != order_index.cend(); o_it++)
	{
		auto o_buffer_it = o_buffer.find(o_it->first);
		if (o_buffer_it == o_buffer.end())
		{
			o_buffer[o_it->first] = o_it->second;
		}
	}
	// append records to result_buffer
	for (std::unordered_map<uint32_t, lineitem_order>::const_iterator result_it = result.cbegin(); result_it != result.cend(); result_it++)
	{
		auto r_buffer_it = result_buffer.find(result_it->first);
		if (r_buffer_it == result_buffer.end())
		{
			result_buffer[result_it->first] = result_it->second;
		}
	}
}

Experiment::Tpch::LineitemOrderOfflineAggregator::LineitemOrderOfflineAggregator()
{
}

Experiment::Tpch::LineitemOrderOfflineAggregator::~LineitemOrderOfflineAggregator()
{
}

void Experiment::Tpch::LineitemOrderOfflineAggregator::sort_final_result(const std::vector<lineitem_order>& final_result, const std::string & output_file)
{
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	std::map<uint32_t, lineitem_order> ordered_final_result;
	for (std::vector<lineitem_order>::const_iterator cit = final_result.cbegin(); cit != final_result.cend(); ++cit)
	{
		ordered_final_result[cit->_lineitem.l_order_key ^ cit->_lineitem.l_linenumber] = *cit;
	}
	for (std::map<uint32_t, lineitem_order>::const_iterator it = ordered_final_result.begin(); it != ordered_final_result.end(); ++it)
	{
		std::string buffer = it->second.to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
		//std::cout << "sort_final_result(): writing result: " << buffer;
	}
	/*for (std::vector<lineitem_order>::const_iterator it = final_result.cbegin(); it != final_result.cend(); ++it)
	{
		std::string buffer = it->to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}*/
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::LineitemOrderOfflineAggregator::calculate_and_produce_final_result(std::unordered_map<uint32_t, Tpch::lineitem>& li_buffer, 
	std::unordered_map<uint32_t, Tpch::order>& o_buffer, std::unordered_map<uint32_t, lineitem_order>& result_buffer, const std::string & output_file)
{
	// first materialize result
	FILE* fd;
	fd = fopen(output_file.c_str(), "w");
	std::map<uint32_t, lineitem_order> ordered_final_result;
	for (std::unordered_map<uint32_t, Tpch::lineitem>::const_iterator li_it = li_buffer.cbegin(); li_it != li_buffer.cend(); li_it++)
	{
		auto o_it = o_buffer.find(li_it->second.l_order_key);
		auto r_it = result_buffer.find(li_it->second.l_order_key ^ li_it->second.l_linenumber);
		if (o_it != o_buffer.end() && r_it == result_buffer.end())
		{
			result_buffer[li_it->second.l_order_key ^ li_it->second.l_linenumber] = Tpch::lineitem_order(o_it->second, li_it->second);
		}
	}
	for (std::unordered_map<uint32_t, lineitem_order>::const_iterator cit = result_buffer.cbegin(); cit != result_buffer.cend(); ++cit)
	{
		ordered_final_result[cit->first] = cit->second;
	}
	for (std::map<uint32_t, lineitem_order>::const_iterator it = ordered_final_result.begin(); it != ordered_final_result.end(); ++it)
	{
		std::string buffer = it->second.to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	/*for (std::unordered_map<uint32_t, lineitem_order>::const_iterator it = result_buffer.cbegin(); it != result_buffer.cend(); ++it)
	{
		std::string buffer = it->second.to_string() + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}*/
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::LineitemOrderPartition::lineitem_order_join_simulation(const std::vector<Experiment::Tpch::lineitem>& li_table, 
	const std::vector<Experiment::Tpch::order>& o_table, const size_t task_num)
{
	RoundRobinPartitioner* rrg;
	PkgPartitioner* pkg;
	HashFieldPartitioner* fld;
	CardinalityAwarePolicy ca_policy;
	CaPartitionLib::CA_Exact_Partitioner* ca_naive;
	LoadAwarePolicy la_policy;
	CaPartitionLib::CA_Exact_Partitioner* la_naive;
	std::vector<uint16_t> tasks;

	for (uint16_t i = 0; i < task_num; i++)
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
	lineitem_order_join_partitioner_simulation(li_table, o_table, tasks, *rrg, "shg", "shuffle_li_join_order_result.csv");
	lineitem_order_join_partitioner_simulation(li_table, o_table, tasks, *fld, "fld", "fld_li_join_order_result.csv");
	lineitem_order_join_partitioner_simulation(li_table, o_table, tasks, *pkg, "pkg", "pkg_li_join_order_result.csv");
	lineitem_order_join_partitioner_simulation(li_table, o_table, tasks, *ca_naive, "ca-naive", "ca_naive_li_join_order_result.csv");
	lineitem_order_join_partitioner_simulation(li_table, o_table, tasks, *la_naive, "la-naive", "la_naive_li_join_order_result.csv");
	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete la_naive;
	tasks.clear();
}

void Experiment::Tpch::LineitemOrderPartition::lineitem_order_join_partitioner_simulation(const std::vector<Experiment::Tpch::lineitem>& li_table, 
	const std::vector<Experiment::Tpch::order>& o_table, const std::vector<uint16_t> tasks, Partitioner & partitioner, 
	const std::string partitioner_name, const std::string worker_output_file_name)
{
	double min_duration = -1, max_duration = 0, sum_of_durations = 0;
	std::queue<Experiment::Tpch::lineitem> li_queue;
	std::queue<Experiment::Tpch::order> o_queue;
	std::mutex li_mu, o_mu;
	std::condition_variable li_cond, o_cond;
	std::vector<Tpch::lineitem_order> intermediate_buffer;
	std::unordered_map<uint32_t, Tpch::lineitem> li_inter_buffer; 
	std::unordered_map<uint32_t, Tpch::order> o_inter_buffer;
	std::unordered_map<uint32_t, lineitem_order> result_inter_buffer;
	std::vector<std::vector<Tpch::lineitem>> li_worker_input_buffer(tasks.size(), std::vector<Tpch::lineitem>());
	std::vector<std::vector<Tpch::order>> o_worker_input_buffer(tasks.size(), std::vector<Tpch::order>());
	// partition order tuples
	for (auto it = o_table.cbegin(); it != o_table.cend(); ++it)
	{
		uint16_t task = partitioner.partition_next(&it->o_orderkey, sizeof(it->o_orderkey));
		o_worker_input_buffer[task].push_back(*it);
	}
	o_worker_input_buffer.shrink_to_fit();
	// partition lineitem tuples
	for (auto it = li_table.cbegin(); it != li_table.cend(); ++it)
	{
		uint16_t task = partitioner.partition_next(&it->l_order_key, sizeof(it->l_order_key));
		li_worker_input_buffer[task].push_back(*it);
	}
	li_worker_input_buffer.shrink_to_fit();

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::Tpch::LineitemOrderWorker worker(&li_queue, &li_mu, &li_cond, &o_queue, &o_mu, &o_cond);
		bool partial_flag = true;
		if (partitioner_name.compare("fld") == 0)
		{
			partial_flag = false;
		}
		// TIME CRITICAL - START
		std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
		for (auto it = o_worker_input_buffer[i].begin(); it != o_worker_input_buffer[i].end(); ++it)
		{
			worker.update(*it);
		}
		for (auto it = li_worker_input_buffer[i].begin(); it != li_worker_input_buffer[i].end(); ++it)
		{
			worker.update(*it, partial_flag);
		}
		if (partitioner_name.compare("fld") == 0)
		{
			worker.finalize(intermediate_buffer);
		}
		else
		{
			worker.partial_finalize(li_inter_buffer, o_inter_buffer, result_inter_buffer);
		}
		std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
		// TIME CRITICAL - END
		std::chrono::duration<double, std::milli> execution_time = end - start;
		sum_of_durations += execution_time.count();
		if (max_duration < execution_time.count())
		{
			max_duration = execution_time.count();
		}
		min_duration = i == 0 ? execution_time.count() : (min_duration > execution_time.count() ? execution_time.count() : min_duration);
		li_worker_input_buffer[i].clear();
		o_worker_input_buffer[i].clear();
	}
	Experiment::Tpch::LineitemOrderOfflineAggregator aggregator;
	// TIME CRITICAL - START
	std::chrono::system_clock::time_point aggregate_start = std::chrono::system_clock::now();
	if (partitioner_name.compare("fld") != 0)
	{
		aggregator.calculate_and_produce_final_result(li_inter_buffer, o_inter_buffer, result_inter_buffer, worker_output_file_name);
	}
	else
	{
		aggregator.sort_final_result(intermediate_buffer, worker_output_file_name);
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";
	intermediate_buffer.clear();
	li_inter_buffer.clear();
	o_inter_buffer.clear();
	result_inter_buffer.clear();
}
