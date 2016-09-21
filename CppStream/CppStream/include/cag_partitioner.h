#include <iostream>
#include <vector>
#include <unordered_set>
#include <cinttypes>
#include <limits>

#include "partitioner.h"
#include "MurmurHash3.h"
#include "CardinalityEstimator.h"
#include "partition_policy.h"

#ifndef CAG_PARTITIONER_H_
#define CAG_PARTITIONER_H_
namespace CagPartionLib
{
	class CagNaivePartitioner : public Partitioner
	{
	public:
		CagNaivePartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy);
		~CagNaivePartitioner();
		uint16_t partition_next(const std::string& key, const size_t key_len);
		void  get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		std::vector<uint64_t> task_count;
		std::vector<std::unordered_set<uint32_t>> task_cardinality;
		PartitionPolicy& policy;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint32_t max_task_cardinality;
		uint32_t min_task_cardinality;
	};

	class CagPcPartitioner : public Partitioner
	{
	public:
		CagPcPartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy);
		~CagPcPartitioner();
		uint16_t partition_next(const std::string& key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		PartitionPolicy& policy;
		uint64_t* _task_count;
		CardinalityEstimator::ProbCount** _task_cardinality;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint32_t max_task_cardinality;
		uint32_t min_task_cardinality;
	};

	class CagHllPartitioner : public Partitioner
	{
	public:
		CagHllPartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy, uint8_t k);
		~CagHllPartitioner();
		uint16_t partition_next(const std::string& key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		PartitionPolicy& policy;
		uint64_t* _task_count;
		CardinalityEstimator::HyperLoglog** _task_cardinality;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint32_t max_task_cardinality;
		uint32_t min_task_cardinality;
	};
}
#endif // !CAG_PARTITIONER_H_
