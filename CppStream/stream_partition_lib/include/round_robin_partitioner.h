#pragma once
#include <vector>
#include <algorithm>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#define ROUND_ROBIN_PARTITIONER_H_
class RoundRobinPartitioner : public Partitioner
{
public:
	RoundRobinPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), task_count(tasks.size(), 0), next_task(-1), max_task_index(tasks.size() - 1), 
		max_task_count(0), min_task_count(0)
	{
		if (tasks.size() <= 0)
		{
			throw "RoundRobinPartitioner(): empty task collection!";
		}
	}
	RoundRobinPartitioner(const RoundRobinPartitioner& p) : tasks(p.tasks), task_count(p.task_count),
		next_task(p.next_task), max_task_index(p.max_task_index), max_task_count(p.max_task_count), min_task_count(p.min_task_count) {}
	
	void init() override
	{
		std::vector<unsigned long long>(tasks.size(), uint64_t(0)).swap(task_count);
		next_task = -1;
		max_task_count = 0;
		min_task_count = 0;
	}

	uint16_t partition_next(const void* key, const size_t key_len) override
	{
		next_task = next_task >= max_task_index ? 0 : next_task + 1;
		task_count[next_task]++;
		max_task_count = std::max(max_task_count, task_count[next_task]);
		min_task_count = *std::min_element(task_count.begin(), task_count.end());
		return tasks[next_task];
	}

	size_t get_min_task_count() const override { return min_task_count; }
	
	size_t get_max_task_count() const override { return max_task_count; }

private:
	
	std::vector<uint16_t> tasks;
	
	std::vector<size_t> task_count;
	
	size_t next_task;
	
	size_t max_task_index;
	
	size_t max_task_count;
	
	size_t min_task_count;
};
#endif // !ROUND_ROBIN_PARTITIONER_H_