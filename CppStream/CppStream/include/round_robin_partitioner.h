#pragma once
#include <vector>
#include <cinttypes>
#include <algorithm>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#define ROUND_ROBIN_PARTITIONER_H_
class RoundRobinPartitioner : public Partitioner
{
public:
	RoundRobinPartitioner(const std::vector<uint16_t>& tasks);
	~RoundRobinPartitioner();
	unsigned int partition_next(const void* key, const size_t key_len);
	unsigned long long get_min_task_count();
	unsigned long long get_max_task_count();
private:
	std::vector<uint16_t> tasks;
	std::vector<unsigned long long> task_count;
	size_t next_task;
	unsigned long long max_task_count;
	unsigned long long min_task_count;
};

RoundRobinPartitioner::RoundRobinPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), 
task_count(tasks.size(), uint64_t(0))
{
	next_task = -1;
	max_task_count = 0;
	min_task_count = 0;
}

RoundRobinPartitioner::~RoundRobinPartitioner()
{
}

inline unsigned int RoundRobinPartitioner::partition_next(const void* key, const size_t key_len)
{
	next_task = next_task >= tasks.size() - 1 ? 0 : next_task + 1;
	task_count[next_task]++;
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[next_task]);
	min_task_count = *std::min_element(task_count.begin(), task_count.end());
	return tasks[next_task];
}
inline unsigned long long RoundRobinPartitioner::get_min_task_count()
{
	return min_task_count;
}

inline unsigned long long RoundRobinPartitioner::get_max_task_count()
{
	return max_task_count;
}
#endif // !ROUND_ROBIN_PARTITIONER_H_

