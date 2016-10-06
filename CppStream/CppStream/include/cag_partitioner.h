#pragma once
#include <iostream>
#include <cstdlib>
#include <cmath>
#include <vector>
#include <unordered_set>
#include <cinttypes>
#include <limits>

#ifndef PARTITION_POLICY_H_
#include "partition_policy.h"
#endif // !PARTITION_POLICY_H_

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef MURMURHASH_3_H_
#include "murmurhash_3.h"
#endif // !MURMURHASH_3_H_

#ifndef CARDINALITY_ESTIMATION_UTILS_H_
#include "cardinality_estimation_utils.h"
#endif // !CARDINALITY_ESTIMATION_UTILS_H_

#ifndef C_HLL_H_
#include "c_hll.h"
#endif // !C_HLL_H_


#ifndef CAG_PARTITIONER_H_
#define CAG_PARTITIONER_H_
namespace CagPartitionLib
{
	class CagNaivePartitioner : public Partitioner
	{
	public:
		CagNaivePartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy);
		~CagNaivePartitioner();
		uint16_t partition_next(const void* key, const size_t key_len);
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
		uint16_t partition_next(const void* key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
		uint64_t get_max_cardinality();
		double get_average_cardinality();
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
		uint16_t partition_next(const void* key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		hll_8** task_cardinality;
		std::vector<uint16_t> tasks;
		PartitionPolicy& policy;
		uint64_t* _task_count;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint32_t max_task_cardinality;
		uint32_t min_task_cardinality;
	};

	class CagHllEstPartitioner : public Partitioner
	{
	public:
		CagHllEstPartitioner(const std::vector<uint16_t>& tasks, uint8_t k);
		~CagHllEstPartitioner();
		uint16_t partition_next(const void* key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
	private:
		std::vector<uint16_t> tasks;
		uint64_t* _task_count;
		hll_8** _task_cardinality;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint32_t max_task_cardinality;
		uint32_t min_task_cardinality;
	};
}

CagPartitionLib::CagNaivePartitioner::CagNaivePartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) :
	tasks(tasks), task_count(tasks.size(), uint64_t(0)),
	task_cardinality(tasks.size(), std::unordered_set<uint32_t>()),
	policy(policy)
{
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
	max_task_cardinality = uint32_t(0);
	min_task_cardinality = std::numeric_limits<uint32_t>::max();
}

CagPartitionLib::CagNaivePartitioner::~CagNaivePartitioner()
{
}

inline uint16_t CagPartitionLib::CagNaivePartitioner::partition_next(const void* key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key, key_len, 13, &hash_one);
	MurmurHash3_x86_32(key, key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_cardinality = uint32_t(task_cardinality[first_choice].size());
	uint32_t second_cardinality = uint32_t(task_cardinality[second_choice].size());
	uint16_t selected_choice = policy.get_score(first_choice, task_count[first_choice], first_cardinality,
		second_choice, task_count[second_choice], second_cardinality, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	task_cardinality[selected_choice].insert(hash_one);
	uint32_t selected_cardinality = uint32_t(task_cardinality[selected_choice].size());
	task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	min_task_cardinality = BitWizard::min_uint32(min_task_cardinality, selected_cardinality);
	return selected_choice;
}

inline void CagPartitionLib::CagNaivePartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	{
		v.push_back((uint32_t)task_cardinality[i].size());
	}
	v.shrink_to_fit();
}

CagPartitionLib::CagPcPartitioner::CagPcPartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) :
	tasks(tasks), policy(policy)
{
	_task_count = new uint64_t[tasks.size()];
	_task_cardinality = new CardinalityEstimator::ProbCount*[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		_task_cardinality[i] = new CardinalityEstimator::ProbCount();
		_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
	max_task_cardinality = uint32_t(0);
	min_task_cardinality = std::numeric_limits<uint32_t>::max();
}

CagPartitionLib::CagPcPartitioner::~CagPcPartitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete _task_cardinality[i];
	}
	delete[] _task_cardinality;
	delete[] _task_count;
}

// Vanilla version of CAG-PC (without the pessimistic increase in cardinality)
//uint16_t CagPartionLib::CagPcPartitioner::partition_next(const std::string & key, const size_t key_len)
//{
//	uint32_t hash_one, hash_two;
//	uint32_t first_choice, second_choice;
//	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
//	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
//	first_choice = hash_one % tasks.size();
//	second_choice = hash_two % tasks.size();
//	uint32_t first_cardinality = _task_cardinality[first_choice]->cardinality_estimation();
//	uint32_t second_cardinality = _task_cardinality[second_choice]->cardinality_estimation();
//	//uint32_t choice_cardinality = BitWizard::min_uint32(first_cardinality, second_cardinality);
//	// decision
//	uint16_t selected_choice = policy.get_score(first_choice, _task_count[first_choice], first_cardinality,
//		second_choice, _task_count[second_choice], second_cardinality, min_task_count, max_task_count,
//		min_task_cardinality, max_task_cardinality);
//	//update metrics
//	_task_cardinality[selected_choice]->update_bitmap_with_hashed_value(hash_one);
//	uint32_t selected_cardinality = _task_cardinality[selected_choice]->cardinality_estimation();
//	// way to overcome getting stuck at 661
//	//_task_cardinality[selected_choice]->set_bitmap(BitWizard::max_uint32(choice_cardinality + 1, selected_cardinality));
//	//selected_cardinality = BitWizard::max_uint32(choice_cardinality + 1, selected_cardinality);
//	_task_count[selected_choice] += 1;
//	max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
//	min_task_count = BitWizard::min_uint64(min_task_count, _task_count[selected_choice]);
//	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
//	min_task_cardinality = BitWizard::min_uint64(min_task_cardinality, selected_cardinality);
//	return selected_choice;
//}

/**
 * Pessimistic version with the blind increase in cardinality. (CAG-PC*)
 */
inline uint16_t CagPartitionLib::CagPcPartitioner::partition_next(const void* key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key, key_len, 13, &hash_one);
	MurmurHash3_x86_32(key, key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_cardinality = _task_cardinality[first_choice]->cardinality_estimation();
	uint32_t second_cardinality = _task_cardinality[second_choice]->cardinality_estimation();
	uint32_t choice_cardinality = BitWizard::min_uint32(first_cardinality, second_cardinality);
	// decision
	uint16_t selected_choice = policy.get_score(first_choice, _task_count[first_choice], first_cardinality,
		second_choice, _task_count[second_choice], second_cardinality, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	//update metrics
	_task_cardinality[selected_choice]->update_bitmap_with_hashed_value(hash_one);
	uint32_t selected_cardinality = _task_cardinality[selected_choice]->cardinality_estimation();
	// way to overcome getting stuck
	selected_cardinality = BitWizard::max_uint32(choice_cardinality + 1, selected_cardinality);
	_task_cardinality[selected_choice]->set_bitmap(selected_cardinality);
	_task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, _task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	min_task_cardinality = BitWizard::min_uint32(min_task_cardinality, selected_cardinality);
	return selected_choice;
}

void CagPartitionLib::CagPcPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(_task_cardinality[i]->cardinality_estimation());
	}
	v.shrink_to_fit();
}

inline uint64_t CagPartitionLib::CagPcPartitioner::get_max_cardinality()
{
	return max_task_cardinality;
}

inline double CagPartitionLib::CagPcPartitioner::get_average_cardinality()
{
	double avg_cardinality = 0;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		avg_cardinality += _task_cardinality[i]->cardinality_estimation();
	}
	return avg_cardinality / tasks.size();
}

CagPartitionLib::CagHllPartitioner::CagHllPartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy, uint8_t k) :
	tasks(tasks), policy(policy)
{
	this->task_cardinality = (hll_8**) malloc(tasks.size() * sizeof(hll_8*));
	this->_task_count = new uint64_t[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		// _task_cardinality[i] = new CardinalityEstimator::HyperLoglog(k);
		this->task_cardinality[i] = (hll_8*) malloc(tasks.size() * sizeof(hll_8));
		init_8(this->task_cardinality[i], k);
		this->_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
	max_task_cardinality = uint32_t(0);
	min_task_cardinality = std::numeric_limits<uint32_t>::max();
}

CagPartitionLib::CagHllPartitioner::~CagHllPartitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		destroy_8(this->task_cardinality[i]);
		free(this->task_cardinality[i]);
	}
	free(this->task_cardinality);
	delete[] _task_count;
}

inline uint16_t CagPartitionLib::CagHllPartitioner::partition_next(const void* key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key, key_len, 13, &hash_one);
	MurmurHash3_x86_32(key, key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_card = opt_cardinality_estimation_8(this->task_cardinality[first_choice]);
	uint32_t second_card = opt_cardinality_estimation_8(this->task_cardinality[second_choice]);
	// decision
	uint16_t selected_choice = policy.get_score(first_choice, _task_count[first_choice], first_card,
		second_choice, _task_count[second_choice], second_card, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	// update metrics
	opt_update_8(this->task_cardinality[selected_choice], hash_one);
	uint32_t selected_cardinality = opt_cardinality_estimation_8(this->task_cardinality[selected_choice]);
	_task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, _task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	min_task_cardinality = BitWizard::min_uint32(min_task_cardinality, selected_cardinality);
	return selected_choice;
}

void CagPartitionLib::CagHllPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(opt_cardinality_estimation_8(this->task_cardinality[i]));
	}
	v.shrink_to_fit();
}

CagPartitionLib::CagHllEstPartitioner::CagHllEstPartitioner(const std::vector<uint16_t>& tasks, uint8_t k) :
	tasks(tasks)
{
	this->_task_cardinality = (hll_8**) malloc(sizeof(hll_8*) * tasks.size());
	_task_count = new uint64_t[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		this->_task_cardinality[i] = (hll_8*) malloc(sizeof(hll_8));
		init_8(this->_task_cardinality[i], k);
		_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
	max_task_cardinality = uint32_t(0);
	min_task_cardinality = std::numeric_limits<uint32_t>::max();
}

CagPartitionLib::CagHllEstPartitioner::~CagHllEstPartitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		destroy_8(this->_task_cardinality[i]);
		free(this->_task_cardinality[i]);
	}
	free(this->_task_cardinality);
	delete[] _task_count;
}

inline uint16_t CagPartitionLib::CagHllEstPartitioner::partition_next(const void* key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key, key_len, 13, &hash_one);
	MurmurHash3_x86_32(key, key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_card = opt_cardinality_estimation_8(this->_task_cardinality[first_choice]);
	uint32_t second_card = opt_cardinality_estimation_8(this->_task_cardinality[second_choice]);
	// calculate new cardinality estimates (EXTRA COST)
	uint32_t first_card_est = opt_new_cardinality_estimate_8(this->_task_cardinality[first_choice], hash_one);
	uint32_t second_card_est = opt_new_cardinality_estimate_8(this->_task_cardinality[second_choice], hash_one);
	// decision
	if (first_card_est - first_card == 0)
	{
		opt_update_8(this->_task_cardinality[first_choice], hash_one);
		_task_count[first_choice] += 1;
		max_task_count = BitWizard::max_uint64(max_task_count, _task_count[first_choice]);
		min_task_count = BitWizard::min_uint64(min_task_count, _task_count[first_choice]);
		// do not have to update cardinalities because they will remain the same
		return first_choice;
	}
	else if (second_card_est - second_card == 0)
	{
		opt_update_8(this->_task_cardinality[second_choice], hash_one);
		_task_count[second_choice] += 1;
		max_task_count = BitWizard::max_uint64(max_task_count, _task_count[second_choice]);
		min_task_count = BitWizard::min_uint64(min_task_count, _task_count[second_choice]);
		// do not have to update cardinalities because they will remain the same
		return second_choice;
	}
	else
	{
		// behave like CAG: send the tuple to the worker with the smallest 
		// cardinality at that point
		uint16_t selected_choice = first_card < second_card ? first_choice : second_choice;
		// update metrics
		opt_update_8(this->_task_cardinality[selected_choice], hash_one);
		uint32_t selected_cardinality = opt_cardinality_estimation_8(this->_task_cardinality[selected_choice]);
		_task_count[selected_choice] += 1;
		max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
		min_task_count = BitWizard::min_uint64(min_task_count, _task_count[selected_choice]);
		max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
		min_task_cardinality = BitWizard::min_uint32(min_task_cardinality, selected_cardinality);
		return selected_choice;
	}
}

inline void CagPartitionLib::CagHllEstPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(opt_cardinality_estimation_8(this->_task_cardinality[i]));
	}
	v.shrink_to_fit();
}
#endif // !CAG_PARTITIONER_H_
