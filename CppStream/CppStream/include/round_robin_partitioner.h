#pragma once
#include <vector>
#include <cinttypes>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#define ROUND_ROBIN_PARTITIONER_H_
class RoundRobinPartitioner : public Partitioner
{
public:
	RoundRobinPartitioner(const std::vector<uint16_t>& tasks);
	~RoundRobinPartitioner();
	uint16_t partition_next(const void* key, const size_t key_len);
private:
	std::vector<uint16_t> tasks;
	size_t next_task;
};

RoundRobinPartitioner::RoundRobinPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks)
{
	next_task = -1;
}

RoundRobinPartitioner::~RoundRobinPartitioner()
{
}

uint16_t RoundRobinPartitioner::partition_next(const void* key, const size_t key_len)
{
	next_task = next_task >= tasks.size() - 1 ? 0 : next_task + 1;
	return tasks[next_task];
}
#endif // !ROUND_ROBIN_PARTITIONER_H_

