#include "../include/google_cluster_monitor_query.h"

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorkerThread::TotalCpuPerCategoryWorkerThread(std::queue<Experiment::GoogleClusterMonitor::task_event>* input_queue, std::mutex * mu, std::condition_variable * cond)
{
	this->input_queue = input_queue;
	this->mu = mu;
	this->cond = cond;
}

Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorkerThread::~TotalCpuPerCategoryWorkerThread()
{
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorkerThread::operate()
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
			finalize();
			locker.unlock();
			cond->notify_all();
			break;
		}
		locker.unlock();
		cond->notify_all();
	}
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorkerThread::update(Experiment::GoogleClusterMonitor::task_event & task_event)
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

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorkerThread::finalize()
{
	// do nothing since the result is already materialized
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorkerThread::partial_finalize(std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_result)
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

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& full_aggregates, const std::string & outfile_name)
{
	FILE* fd;
	fd = fopen(outfile_name.c_str(), "w");
	for (std::vector<cm_one_result>::const_iterator it = full_aggregates.cbegin(); it != full_aggregates.cend(); ++it)
	{
		std::string buffer = std::to_string(it->timestamp) + "," + std::to_string(it->category) + "," + std::to_string(it->total_cpu) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fclose(fd);
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryOfflineAggregator::calculate_and_sort_final_aggregation(const std::vector<Experiment::GoogleClusterMonitor::cm_one_result>& partial_aggregates, const std::string & outfile_name)
{
	FILE* fd;
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
		std::string buffer = std::to_string(cit->second.timestamp) + "," + std::to_string(cit->second.category) + "," + std::to_string(cit->second.total_cpu) + "\n";
		fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
	}
	fclose(fd);
}
