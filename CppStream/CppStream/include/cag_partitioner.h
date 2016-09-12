#pragma once
#include <vector>
#include <unordered_set>
#include <cinttypes>

#include "MurmurHash3.h"
#include "CardinalityEstimator.h"

#ifndef CAG_PARTITIONER_H_
#define CAG_PARTITIONER_H_
namespace CagPartionLib
{
	class CagNaivePartitioner
	{
	public:
		CagNaivePartitioner(const std::vector<uint16_t>& tasks);
		~CagNaivePartitioner();
		uint16_t partition_next(const std::string& key, size_t key_len);
		void  get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		std::vector<uint64_t> task_count;
		std::vector<std::unordered_set<uint32_t>> task_cardinality;
	};

	class CagPcPartitioner
	{
	public:
		CagPcPartitioner(const std::vector<uint16_t>& tasks);
		~CagPcPartitioner();
		uint16_t partition_next(const std::string& key, size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		uint64_t* _task_count;
		CardinalityEstimator::ProbCount** _task_cardinality;
	};

	class CagHllPartitioner
	{
	public:
		CagHllPartitioner(const std::vector<uint16_t>& tasks, uint8_t k);
		~CagHllPartitioner();
		uint16_t partition_next(const std::string& key, size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		uint64_t* _task_count;
		CardinalityEstimator::HyperLoglog** _task_cardinality;
	};
}
#endif // !CAG_PARTITIONER_H_
