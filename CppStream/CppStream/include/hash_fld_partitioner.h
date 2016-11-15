#pragma once
#include <vector>
#include <cinttypes>

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_

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
	HashFieldPartitioner() {}
	HashFieldPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), task_count(tasks.size(), 0),
		max_task_count(0), min_task_count(0) {}
	HashFieldPartitioner(const HashFieldPartitioner& p) : tasks(p.tasks), task_count(p.task_count),
		max_task_count(p.max_task_count), min_task_count(p.min_task_count) {}
	~HashFieldPartitioner()
	{
		tasks.clear();
		task_count.clear();
	}
	void init() override
	{
		std::vector<unsigned long long>(tasks.size(), 0).swap(task_count);
		max_task_count = 0;
		min_task_count = 0;
	}
	unsigned int partition_next(const void* key, const size_t key_len) override
	{
		uint64_t hash_code, long_hash_code[2];
		unsigned int selected_task;
		MurmurHash3_x64_128(key, key_len, 13, &long_hash_code);
		hash_code = long_hash_code[0] ^ long_hash_code[1];
		selected_task = static_cast<unsigned int>(hash_code) % tasks.size();
		max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_task]);
		min_task_count = *std::min_element(task_count.begin(), task_count.end());
		return tasks[selected_task];
	}
	unsigned long long get_min_task_count() override { return min_task_count; }
	unsigned long long get_max_task_count() override { return max_task_count; }
private:
	std::vector<uint16_t> tasks;
	std::vector<unsigned long long> task_count;
	unsigned long long max_task_count;
	unsigned long long min_task_count;
};
#endif // !HASH_FLD_PARTITIONER_H_