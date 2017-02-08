#pragma once
#include <vector>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef MURMURHASH_3_H_
#include "murmurhash_3.h"
#endif // !MURMURHASH_3_H_

#ifndef HASH_FLD_PARTITIONER_H_
#define HASH_FLD_PARTITIONER_H_
class HashFieldPartitioner : public Partitioner
{
public:
	HashFieldPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), task_count(tasks.size(), 0),
		max_task_count(0), min_task_count(0) {}
	
	HashFieldPartitioner(const HashFieldPartitioner& p) : tasks(p.tasks), task_count(p.task_count),
		max_task_count(p.max_task_count), min_task_count(p.min_task_count) {}
	
	void init() override
	{
		std::vector<size_t>(tasks.size(), 0).swap(task_count);
		max_task_count = 0;
		min_task_count = 0;
	}

	uint16_t partition_next(const void* key, const size_t key_len) override
	{
		uint64_t hash_code, long_hash_code[2];
		size_t selected_task;
		MurmurHash3_x64_128(key, key_len, 13, &long_hash_code);
		hash_code = long_hash_code[0] ^ long_hash_code[1];
		selected_task = hash_code % tasks.size();
		max_task_count = std::max(max_task_count, task_count[selected_task]);
		min_task_count = *std::min_element(task_count.begin(), task_count.end());
		return tasks[selected_task];
	}
	
	size_t get_min_task_count() const override { return min_task_count; }
	
	size_t get_max_task_count() const override { return max_task_count; }

private:
	
	std::vector<uint16_t> tasks;
	
	std::vector<size_t> task_count;
	
	size_t max_task_count;
	
	size_t min_task_count;
};
#endif // !HASH_FLD_PARTITIONER_H_