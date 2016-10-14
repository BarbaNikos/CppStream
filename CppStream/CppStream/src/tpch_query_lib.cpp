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
	for (std::vector<query_one_result>::const_iterator it = buffer.cbegin(); it != buffer.cend(); ++it)
	{
		std::string buffer = it->to_string().c_str();
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
	std::unordered_map<std::string, query_one_result> final_result;
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
	for (std::unordered_map<std::string, query_one_result>::const_iterator it = final_result.begin(); it != final_result.end(); ++it)
	{
		std::string buffer = it->second.to_string();
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fflush(fd);
	fclose(fd);
}

void Experiment::Tpch::QueryOnePartition::query_one_simulation(const std::vector<Experiment::Tpch::lineitem>& lines)
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
	query_one_partitioner_simulation(lines, tasks, *rrg, "shg", "shuffle_worker_partial_result");
	query_one_partitioner_simulation(lines, tasks, *fld, "fld", "fld_full_result");
	query_one_partitioner_simulation(lines, tasks, *pkg, "pkg", "pkg_worker_partial_result");
	query_one_partitioner_simulation(lines, tasks, *ca_naive, "ca-naive", "ca_naive_worker_partial_result");
	query_one_partitioner_simulation(lines, tasks, *la_naive, "la-naive", "la_naive_worker_partial_result");
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
		aggregator.calculate_and_produce_final_result(intermediate_buffer, partitioner_name + "_full_result.csv");
	}
	else
	{
		aggregator.sort_final_result(intermediate_buffer, partitioner_name + "_full_result.csv");
	}
	std::chrono::system_clock::time_point aggregate_end = std::chrono::system_clock::now();
	// TIME CRITICAL - END
	std::chrono::duration<double, std::milli> aggregation_time = aggregate_end - aggregate_start;

	std::cout << partitioner_name << " :: Min duration: " << min_duration << " (msec). Max duration: " <<
		max_duration << ", average execution worker time: " << sum_of_durations / tasks.size() <<
		" (msec), aggregation time: " << aggregation_time.count() << " (msec).\n";
	intermediate_buffer.clear();
}
