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
	PkgPartitioner(const std::vector<uint16_t>& tasks);
	~PkgPartitioner();
	uint16_t partition_next(const void* key, const size_t key_len);
	uint64_t get_min_count();
	uint64_t get_max_count();
private:
	std::vector<uint16_t> tasks;
	std::vector<uint64_t> task_count;
	PartitionPolicy* policy;
	uint64_t max_task_count;
	uint64_t min_task_count;
};

PkgPartitioner::PkgPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), task_count(tasks.size(), uint64_t(0))
{
	policy = new CountAwarePolicy();
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
}

PkgPartitioner::~PkgPartitioner()
{
	delete policy;
}

uint16_t PkgPartitioner::partition_next(const void* key, const size_t key_len)
{
	uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
	uint32_t first_choice, second_choice;
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	hash_one = long_hash_one[0] ^ long_hash_one[1];
	hash_two = long_hash_two[0] ^ long_hash_two[1];
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint16_t selected_choice = policy->get_score(first_choice, task_count[first_choice], 0,
		second_choice, task_count[second_choice], 0, min_task_count, max_task_count, 0, 0);
	task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	min_task_count = *std::min_element(task_count.begin(), task_count.end());
	return selected_choice;
}

uint64_t PkgPartitioner::get_min_count()
{
	return min_task_count;
}

uint64_t PkgPartitioner::get_max_count()
{
	return max_task_count;
}


#endif // !PKG_PARTITIONER_H_