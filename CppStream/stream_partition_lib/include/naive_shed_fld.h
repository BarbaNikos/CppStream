#pragma once
#include <vector>
#include <cinttypes>
#include <algorithm>

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif // !IMBALANCE_SCORE_AGGR_H_

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef MURMURHASH_3_H_
#include "murmurhash_3.h"
#endif // !MURMURHASH_3_H_

#ifndef NAIVE_SHED_FLD_PARTITIONER_H_
#define NAIVE_SHED_FLD_PARTITIONER_H_
class NaiveShedFieldPartitioner : public Partitioner
{
public:
	NaiveShedFieldPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), tuple_count(tasks.size(), 0), max_task_count(0), 
	min_task_count(0) {}
	NaiveShedFieldPartitioner(const NaiveShedFieldPartitioner& p) : tasks(p.tasks), tuple_count(p.tuple_count), max_task_count(p.max_task_count), 
	min_task_count(p.min_task_count) {}
	~NaiveShedFieldPartitioner()
	{
		tasks.clear();
		tuple_count.clear();
	}
	void init() override
	{
		max_task_count = 0;
		min_task_count = 0;
		for (size_t i = 0; i < tasks.size(); ++i)
		{
			tuple_count[i] = 0;
		}
	}

	unsigned int partition_next(const void* key, const size_t key_len) override
	{
		uint64_t hash_code, long_hash_code[2];
		unsigned int selected_task;
		MurmurHash3_x64_128(key, key_len, 13, &long_hash_code);
		hash_code = long_hash_code[0] ^ long_hash_code[1];
		selected_task = static_cast<unsigned int>(hash_code) % tasks.size();
		float mean_tuple_count = std::accumulate(tuple_count.begin(), tuple_count.end(), 0.0) / tuple_count.size();
		bool shed = tuple_count[selected_task] >= min_task_count;
		// decide whether the tuple should be dropped
		if (tuple_count[selected_task] >= max_task_count && tuple_count[selected_task] > 0 && shed)
		{
			return tasks.size() + 1;
		}
		// update statistics
		tuple_count[selected_task]++;
		max_task_count = BitWizard::max_uint64(max_task_count, tuple_count[selected_task]);
		min_task_count = *std::min_element(tuple_count.begin(), tuple_count.end());
		return selected_task;
	}

	unsigned long long get_min_task_count() override { return min_task_count; }

	unsigned long long get_max_task_count() override { return max_task_count; }
private:
	std::vector<uint16_t> tasks;
	std::vector<unsigned long long> tuple_count;
	unsigned long long max_task_count;
	unsigned long long min_task_count;
};
#endif // !NAIVE_SHED_FLD_PARTITIONER_H_