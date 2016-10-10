#pragma once
#include <iostream>
#include <cstdlib>
#include <cmath>
#include <vector>
#include <unordered_set>
#include <cinttypes>
#include <limits>
#include <algorithm>

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


#ifndef CA_PARTITION_LIB_H_
#define CA_PARTITION_LIB_H_
namespace CaPartitionLib
{
	class CA_Exact_Partitioner : public Partitioner
	{
	public:
		CA_Exact_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy);
		~CA_Exact_Partitioner();
		uint16_t partition_next(const void* key, const size_t key_len);
		void  get_cardinality_vector(std::vector<uint64_t>& v);
	private:
		std::vector<uint16_t> tasks;
		std::vector<uint64_t> task_count;
		std::vector<std::unordered_set<uint64_t>> task_cardinality;
		PartitionPolicy& policy;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint64_t max_task_cardinality;
		uint64_t min_task_cardinality;
	};

	class CA_PC_Partitioner : public Partitioner
	{
	public:
		CA_PC_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy);
		~CA_PC_Partitioner();
		uint16_t partition_next(const void* key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint32_t>& v);
		uint64_t get_max_cardinality();
		double get_average_cardinality();
	private:
		std::vector<uint16_t> tasks;
		PartitionPolicy& policy;
		uint64_t* _task_count;
		Cardinality_Estimation_Utils::ProbCount** _task_cardinality;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint32_t max_task_cardinality;
		uint32_t min_task_cardinality;
	};

	class CA_HLL_Partitioner : public Partitioner
	{
	public:
		CA_HLL_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy, uint8_t k);
		~CA_HLL_Partitioner();
		uint16_t partition_next(const void* key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint64_t>& v);
	private:
		hll_8** task_cardinality;
		std::vector<uint16_t> tasks;
		PartitionPolicy& policy;
		uint64_t* _task_count;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint64_t max_task_cardinality;
		uint64_t min_task_cardinality;
	};

	class CA_HLL_Aff_Partitioner : public Partitioner
	{
	public:
		CA_HLL_Aff_Partitioner(const std::vector<uint16_t>& tasks, uint8_t k);
		~CA_HLL_Aff_Partitioner();
		uint16_t partition_next(const void* key, const size_t key_len);
		void get_cardinality_vector(std::vector<uint64_t>& v);
	private:
		std::vector<uint16_t> tasks;
		uint64_t* _task_count;
		hll_8** _task_cardinality;
		uint64_t max_task_count;
		uint64_t min_task_count;
		uint64_t max_task_cardinality;
		uint64_t min_task_cardinality;
	};
}

CaPartitionLib::CA_Exact_Partitioner::CA_Exact_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) :
	tasks(tasks), task_count(tasks.size(), uint64_t(0)),
	task_cardinality(tasks.size(), std::unordered_set<uint64_t>()),
	policy(policy)
{
	max_task_count = uint64_t(0);
	min_task_count = uint64_t(0);
	max_task_cardinality = uint64_t(0);
	min_task_cardinality = uint64_t(0);
}

CaPartitionLib::CA_Exact_Partitioner::~CA_Exact_Partitioner()
{
}

inline uint16_t CaPartitionLib::CA_Exact_Partitioner::partition_next(const void* key, const size_t key_len)
{
	uint64_t hash_one, hash_two;
	uint64_t long_hash_one[2], long_hash_two[2];
	uint32_t first_choice, second_choice;
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	hash_one = long_hash_one[0] ^ long_hash_one[1];
	MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	hash_two = long_hash_two[0] ^ long_hash_two[1];
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint64_t first_cardinality = uint64_t(task_cardinality[first_choice].size());
	uint64_t second_cardinality = uint64_t(task_cardinality[second_choice].size());
	uint16_t selected_choice = policy.get_score(first_choice, task_count[first_choice], first_cardinality,
		second_choice, task_count[second_choice], second_cardinality, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	task_cardinality[selected_choice].insert(hash_one);
	uint64_t selected_cardinality = uint64_t(task_cardinality[selected_choice].size());
	task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint64(max_task_cardinality, selected_cardinality);
	// need to make the following faster
	min_task_count = *std::min_element(task_count.begin(), task_count.end());
	std::vector<uint64_t> v;
	get_cardinality_vector(v);
	min_task_cardinality = *std::min_element(v.begin(), v.end());
	return tasks[selected_choice];
}

inline void CaPartitionLib::CA_Exact_Partitioner::get_cardinality_vector(std::vector<uint64_t>& v)
{
	for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	{
		v.push_back(uint64_t(task_cardinality[i].size()));
	}
}

CaPartitionLib::CA_PC_Partitioner::CA_PC_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) :
	tasks(tasks), policy(policy)
{
	_task_count = new uint64_t[tasks.size()];
	_task_cardinality = new Cardinality_Estimation_Utils::ProbCount*[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		_task_cardinality[i] = new Cardinality_Estimation_Utils::ProbCount(64);
		_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = uint64_t(0);
	max_task_cardinality = uint64_t(0);
	min_task_cardinality = uint64_t(0);
}

CaPartitionLib::CA_PC_Partitioner::~CA_PC_Partitioner()
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
inline uint16_t CaPartitionLib::CA_PC_Partitioner::partition_next(const void* key, const size_t key_len)
{
	uint32_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
	uint32_t first_choice, second_choice;
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	hash_one = long_hash_one[0] ^ long_hash_one[1];
	hash_two = long_hash_two[0] ^ long_hash_two[1];
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_cardinality = _task_cardinality[first_choice]->cardinality_estimation_64();
	uint32_t second_cardinality = _task_cardinality[second_choice]->cardinality_estimation_64();
	uint32_t choice_cardinality = BitWizard::min_uint32(first_cardinality, second_cardinality);
	// decision
	uint16_t selected_choice = policy.get_score(first_choice, _task_count[first_choice], first_cardinality,
		second_choice, _task_count[second_choice], second_cardinality, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	//update metrics
	_task_cardinality[selected_choice]->update_bitmap_with_hashed_value_64(hash_one);
	uint32_t selected_cardinality = _task_cardinality[selected_choice]->cardinality_estimation_64();
	// way to overcome getting stuck
	selected_cardinality = BitWizard::max_uint32(choice_cardinality + 1, selected_cardinality);
	_task_cardinality[selected_choice]->set_bitmap_64(selected_cardinality);
	_task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
	// really slow - need to re-think the following
	uint64_t min_task_count = std::numeric_limits<uint64_t>::max();
	uint64_t min_task_card = std::numeric_limits<uint64_t>::max();
	for (size_t i = 0; i < tasks.size(); i++)
	{
		if (_task_count[i] < min_task_count)
		{
			min_task_count = _task_count[i];
		}
		uint64_t est_card = _task_cardinality[i]->cardinality_estimation_64();
		min_task_card = min_task_card > est_card ? est_card : min_task_card;
	}
	this->min_task_count = min_task_count;
	this->min_task_cardinality = min_task_card;
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	return tasks[selected_choice];
}

void CaPartitionLib::CA_PC_Partitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(_task_cardinality[i]->cardinality_estimation_64());
	}
	v.shrink_to_fit();
}

inline uint64_t CaPartitionLib::CA_PC_Partitioner::get_max_cardinality()
{
	return max_task_cardinality;
}

inline double CaPartitionLib::CA_PC_Partitioner::get_average_cardinality()
{
	double avg_cardinality = 0;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		avg_cardinality += _task_cardinality[i]->cardinality_estimation_64();
	}
	return avg_cardinality / tasks.size();
}

CaPartitionLib::CA_HLL_Partitioner::CA_HLL_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy, uint8_t k) :
	tasks(tasks), policy(policy)
{
	this->task_cardinality = (hll_8**) malloc(tasks.size() * sizeof(hll_8*));
	this->_task_count = new uint64_t[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		this->task_cardinality[i] = (hll_8*) malloc(tasks.size() * sizeof(hll_8));
		init_8(this->task_cardinality[i], k, sizeof(uint64_t));
		this->_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = uint64_t(0);
	max_task_cardinality = uint64_t(0);
	min_task_cardinality = uint64_t(0);
}

CaPartitionLib::CA_HLL_Partitioner::~CA_HLL_Partitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		destroy_8(this->task_cardinality[i]);
		free(this->task_cardinality[i]);
	}
	free(this->task_cardinality);
	delete[] _task_count;
}

inline uint16_t CaPartitionLib::CA_HLL_Partitioner::partition_next(const void* key, const size_t key_len)
{
	uint64_t hash_one, hash_two;
	uint64_t long_hash_one[2], long_hash_two[2];
	uint32_t first_choice, second_choice;
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	hash_one = long_hash_one[0] & long_hash_one[1];
	hash_two = long_hash_two[0] & long_hash_two[1];
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
	// really slow
	uint64_t min_task_count = std::numeric_limits<uint64_t>::max();
	uint64_t min_task_card = std::numeric_limits<uint64_t>::max();
	for (size_t i = 0; i < tasks.size(); i++)
	{
		if (_task_count[i] < min_task_count)
		{
			min_task_count = _task_count[i];
		}
		uint64_t est_card = opt_cardinality_estimation_8(this->task_cardinality[i]);
		min_task_card = min_task_card > est_card ? est_card : min_task_card;
	}
	this->min_task_count = min_task_count;
	this->min_task_cardinality = min_task_card;
	max_task_cardinality = BitWizard::max_uint64(max_task_cardinality, selected_cardinality);
	return tasks[selected_choice];
}

void CaPartitionLib::CA_HLL_Partitioner::get_cardinality_vector(std::vector<uint64_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(opt_cardinality_estimation_8(this->task_cardinality[i]));
	}
}

CaPartitionLib::CA_HLL_Aff_Partitioner::CA_HLL_Aff_Partitioner(const std::vector<uint16_t>& tasks, uint8_t k) :
	tasks(tasks)
{
	this->_task_cardinality = (hll_8**) malloc(sizeof(hll_8*) * tasks.size());
	_task_count = new uint64_t[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		this->_task_cardinality[i] = (hll_8*) malloc(sizeof(hll_8));
		init_8(this->_task_cardinality[i], k, sizeof(uint64_t));
		_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = uint64_t(0);
	max_task_cardinality = uint64_t(0);
	min_task_cardinality = uint64_t(0);
}

CaPartitionLib::CA_HLL_Aff_Partitioner::~CA_HLL_Aff_Partitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		destroy_8(this->_task_cardinality[i]);
		free(this->_task_cardinality[i]);
	}
	free(this->_task_cardinality);
	delete[] _task_count;
}

inline uint16_t CaPartitionLib::CA_HLL_Aff_Partitioner::partition_next(const void* key, const size_t key_len)
{
	uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
	uint32_t first_choice, second_choice;
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	hash_one = long_hash_one[0] ^ long_hash_one[1];
	MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	hash_two = long_hash_two[0] ^ long_hash_two[1];
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
		// really slow
		uint64_t min_task_count = std::numeric_limits<uint64_t>::max();
		uint64_t min_task_card = std::numeric_limits<uint64_t>::max();
		for (size_t i = 0; i < tasks.size(); i++)
		{
			if (_task_count[i] < min_task_count)
			{
				min_task_count = _task_count[i];
			}
			uint64_t est_card = opt_cardinality_estimation_8(this->_task_cardinality[i]);
			min_task_card = min_task_card > est_card ? est_card : min_task_card;
		}
		this->min_task_count = min_task_count;
		this->min_task_cardinality = min_task_card;
		return tasks[first_choice];
	}
	else if (second_card_est - second_card == 0)
	{
		opt_update_8(this->_task_cardinality[second_choice], hash_one);
		_task_count[second_choice] += 1;
		max_task_count = BitWizard::max_uint64(max_task_count, _task_count[second_choice]);
		// really slow
		uint64_t min_task_count = std::numeric_limits<uint64_t>::max();
		uint64_t min_task_card = std::numeric_limits<uint64_t>::max();
		for (size_t i = 0; i < tasks.size(); i++)
		{
			if (_task_count[i] < min_task_count)
			{
				min_task_count = _task_count[i];
			}
			uint64_t est_card = opt_cardinality_estimation_8(this->_task_cardinality[i]);
			min_task_card = min_task_card > est_card ? est_card : min_task_card;
		}
		this->min_task_count = min_task_count;
		this->min_task_cardinality = min_task_card;
		return tasks[second_choice];
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
		// really slow
		uint64_t min_task_count = std::numeric_limits<uint64_t>::max();
		uint64_t min_task_card = std::numeric_limits<uint64_t>::max();
		for (size_t i = 0; i < tasks.size(); i++)
		{
			if (_task_count[i] < min_task_count)
			{
				min_task_count = _task_count[i];
			}
			uint64_t est_card = opt_cardinality_estimation_8(this->_task_cardinality[i]);
			min_task_card = min_task_card > est_card ? est_card : min_task_card;
		}
		this->min_task_count = min_task_count;
		this->min_task_cardinality = min_task_card;
		max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
		max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
		return tasks[selected_choice];
	}
}

inline void CaPartitionLib::CA_HLL_Aff_Partitioner::get_cardinality_vector(std::vector<uint64_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(opt_cardinality_estimation_8(this->_task_cardinality[i]));
	}
}
#endif // !CA_PARTITION_LIB_H_
