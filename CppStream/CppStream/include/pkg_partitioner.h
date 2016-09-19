#include <vector>
#include <cinttypes>

#include "MurmurHash3.h"
#include "BitTrickBox.h"
#include "partition_policy.h"

#ifndef PKG_PARTITIONER_H_
#define PKG_PARTITIONER_H_
class PkgPartitioner
{
public:
	PkgPartitioner(const std::vector<uint16_t>& tasks);
	~PkgPartitioner();
	uint16_t partition_next(const std::string& key, size_t key_len);
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

uint16_t PkgPartitioner::partition_next(const std::string& key, size_t key_len)
{
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &first_choice);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &second_choice);
	first_choice = first_choice % tasks.size();
	second_choice = second_choice % tasks.size();
	uint16_t selected_choice = policy->get_score(first_choice, task_count[first_choice], 0,
		second_choice, task_count[second_choice], 0, min_task_count, max_task_count, 0, 0);
	task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, task_count[selected_choice]);
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