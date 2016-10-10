#pragma once
#include <vector>
#include <cinttypes>

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
	HashFieldPartitioner(const std::vector<uint16_t>& tasks);
	~HashFieldPartitioner();
	unsigned int partition_next(const void* key, const size_t key_len);
	unsigned long long get_min_task_count();
	unsigned long long get_max_task_count();
private:
	std::vector<uint16_t> tasks;
	std::vector<unsigned long long> task_count;
	unsigned long long max_task_count;
	unsigned long long min_task_count;
};

HashFieldPartitioner::HashFieldPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), 
task_count(tasks.size(), uint64_t(0))
{
	max_task_count = 0;
	min_task_count = 0;
}

HashFieldPartitioner::~HashFieldPartitioner()
{
}

inline unsigned int HashFieldPartitioner::partition_next(const void* key, const size_t key_len)
{
	uint64_t hash_code, long_hash_code[2];
	unsigned int selected_task;
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_code);
	hash_code = long_hash_code[0] ^ long_hash_code[1];
	selected_task = hash_code % tasks.size();
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_task]);
	min_task_count = *std::min_element(task_count.begin(), task_count.end());
	return tasks[selected_task];
}

inline unsigned long long HashFieldPartitioner::get_min_task_count()
{
	return min_task_count;
}

inline unsigned long long HashFieldPartitioner::get_max_task_count()
{
	return max_task_count;
}
#endif // !HASH_FLD_PARTITIONER_H_
