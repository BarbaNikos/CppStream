#pragma once
#include <vector>
#include <cinttypes>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef MURMURHASH_3_H_
#include "murmurhash_3.h"
#endif // !MURMURHASH_3_H_

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_

#ifndef PARTITION_POLICY_H_
#include "partition_policy.h"
#endif // !PARTITION_POLICY_H_


#ifndef PKG_PARTITIONER_H_
#define PKG_PARTITIONER_H_
class PkgPartitioner : public Partitioner
{
public:
	PkgPartitioner() {}
	PkgPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), task_count(tasks.size(), 0),
		max_task_count(0), min_task_count(0) {}
	PkgPartitioner(const PkgPartitioner& p) : tasks(p.tasks), task_count(p.task_count),
		max_task_count(p.max_task_count), min_task_count(p.min_task_count) {}
	~PkgPartitioner()
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
		uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
		unsigned int first_choice, second_choice;
		MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
		MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
		hash_one = long_hash_one[0] ^ long_hash_one[1];
		hash_two = long_hash_two[0] ^ long_hash_two[1];
		first_choice = static_cast<unsigned int>(hash_one) % tasks.size();
		second_choice = static_cast<unsigned int>(hash_two) % tasks.size();
		unsigned int selected_choice = policy.get_score(first_choice, task_count[first_choice], 0,
			second_choice, task_count[second_choice], 0, min_task_count, max_task_count, 0, 0);
		task_count[selected_choice] += 1;
		max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
		min_task_count = *std::min_element(task_count.begin(), task_count.end());
		return tasks[selected_choice];
	}
	unsigned long long get_min_task_count() override { return min_task_count; }
	unsigned long long get_max_task_count() override { return max_task_count; }
private:
	std::vector<uint16_t> tasks;
	std::vector<unsigned long long> task_count;
	CountAwarePolicy policy;
	unsigned long long max_task_count;
	unsigned long long min_task_count;
};
#endif // !PKG_PARTITIONER_H_