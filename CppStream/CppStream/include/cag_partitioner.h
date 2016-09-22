#include <iostream>
#include <cmath>
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

	class PcPartionerBeta : public Partitioner
	{
	public:
		PcPartionerBeta(const std::vector<uint16_t>& tasks, PartitionPolicy& policy);
		~PcPartionerBeta();
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

CagPartionLib::CagNaivePartitioner::CagNaivePartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) :
	tasks(tasks), task_count(tasks.size(), uint64_t(0)),
	task_cardinality(tasks.size(), std::unordered_set<uint32_t>()),
	policy(policy)
{
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
	max_task_cardinality = uint32_t(0);
	min_task_cardinality = std::numeric_limits<uint32_t>::max();
}

CagPartionLib::CagNaivePartitioner::~CagNaivePartitioner()
{
}

inline uint16_t CagPartionLib::CagNaivePartitioner::partition_next(const std::string & key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_cardinality = task_cardinality[first_choice].size();
	uint32_t second_cardinality = task_cardinality[second_choice].size();
	uint16_t selected_choice = policy.get_score(first_choice, task_count[first_choice], first_cardinality,
		second_choice, task_count[second_choice], second_cardinality, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	task_cardinality[selected_choice].insert(hash_one);
	uint32_t selected_cardinality = task_cardinality[selected_choice].size();
	/*std::cout << "NAIVE: first: " << first_choice << ", first-card: " << first_cardinality <<
	", second: " << second_choice << ", second-card: " << second_cardinality <<
	", selected: " << selected_choice << ", selected-card: " << selected_cardinality << "\n";*/
	task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	min_task_cardinality = BitWizard::min_uint32(min_task_cardinality, selected_cardinality);
	return selected_choice;
}

inline void CagPartionLib::CagNaivePartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	{
		v.push_back((uint32_t)task_cardinality[i].size());
	}
}

CagPartionLib::CagPcPartitioner::CagPcPartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) :
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

CagPartionLib::CagPcPartitioner::~CagPcPartitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete _task_cardinality[i];
	}
	delete[] _task_cardinality;
	delete[] _task_count;
}

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
 * Pessimistic version with the blind increase in cardinality.
 */
inline uint16_t CagPartionLib::CagPcPartitioner::partition_next(const std::string & key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
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
	min_task_cardinality = BitWizard::min_uint64(min_task_cardinality, selected_cardinality);
	return selected_choice;
}

void CagPartionLib::CagPcPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(_task_cardinality[i]->cardinality_estimation());
	}
}

inline uint64_t CagPartionLib::CagPcPartitioner::get_max_cardinality()
{
	return max_task_cardinality;
}

inline double CagPartionLib::CagPcPartitioner::get_average_cardinality()
{
	double avg_cardinality;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		avg_cardinality += _task_cardinality[i]->cardinality_estimation();
	}
	return avg_cardinality / tasks.size();
}

CagPartionLib::PcPartionerBeta::PcPartionerBeta(const std::vector<uint16_t>& tasks, PartitionPolicy& policy) : 
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

CagPartionLib::PcPartionerBeta::~PcPartionerBeta()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete _task_cardinality[i];
	}
	delete[] _task_cardinality;
	delete[] _task_count;
}

uint16_t CagPartionLib::PcPartionerBeta::partition_next(const std::string& key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice, selected_choice;
	uint32_t selected_cardinality;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_cardinality = _task_cardinality[first_choice]->cardinality_estimation();
	uint32_t second_cardinality = _task_cardinality[second_choice]->cardinality_estimation();
	_task_cardinality[first_choice]->update_bitmap_with_hashed_value(hash_one);
	_task_cardinality[second_choice]->update_bitmap_with_hashed_value(hash_two);
	uint32_t first_new_cardinality = _task_cardinality[first_choice]->cardinality_estimation();
	uint32_t second_new_cardinality = _task_cardinality[second_choice]->cardinality_estimation();
	/*uint32_t first_diff = abs(first_cardinality - first_new_cardinality);
	uint32_t second_diff = abs(second_cardinality - second_new_cardinality);*/
	// decision
	/*if (first_diff == 0)
	{
		selected_cardinality = first_new_cardinality;
		selected_choice = first_choice;
		_task_cardinality[second_choice]->set_bitmap(second_cardinality);
	} else if (second_diff == 0)
	{
		selected_cardinality = second_new_cardinality;
		selected_choice = second_choice;
		_task_cardinality[first_choice]->set_bitmap(first_cardinality);
	} else 
	{
		if (first_cardinality > second_cardinality)
		{

		} else
		{
			
		}
	}*/
	_task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, _task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	min_task_cardinality = BitWizard::min_uint64(min_task_cardinality, selected_cardinality);
	return selected_choice;
}
		
void CagPartionLib::PcPartionerBeta::get_cardinality_vector(std::vector<uint32_t>& v)
{

}

CagPartionLib::CagHllPartitioner::CagHllPartitioner(const std::vector<uint16_t>& tasks, PartitionPolicy& policy, uint8_t k) :
	tasks(tasks), policy(policy)
{
	_task_cardinality = new CardinalityEstimator::HyperLoglog*[tasks.size()];
	_task_count = new uint64_t[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		_task_cardinality[i] = new CardinalityEstimator::HyperLoglog(k);
		_task_count[i] = uint64_t(0);
	}
	max_task_count = uint64_t(0);
	min_task_count = std::numeric_limits<uint64_t>::max();
	max_task_cardinality = uint32_t(0);
	min_task_cardinality = std::numeric_limits<uint32_t>::max();
}

CagPartionLib::CagHllPartitioner::~CagHllPartitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete _task_cardinality[i];
	}
	delete[] _task_cardinality;
	delete[] _task_count;
}

inline uint16_t CagPartionLib::CagHllPartitioner::partition_next(const std::string & key, const size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_card = _task_cardinality[first_choice]->cardinality_estimation();
	uint32_t second_card = _task_cardinality[second_choice]->cardinality_estimation();
	// decision
	uint16_t selected_choice = policy.get_score(first_choice, _task_count[first_choice], first_card,
		second_choice, _task_count[second_choice], second_card, min_task_count, max_task_count,
		min_task_cardinality, max_task_cardinality);
	// update metrics
	_task_cardinality[selected_choice]->update_bitmap_with_hashed_value(hash_one);
	uint32_t selected_cardinality = _task_cardinality[selected_choice]->cardinality_estimation();
	_task_count[selected_choice] += 1;
	max_task_count = BitWizard::max_uint64(max_task_count, _task_count[selected_choice]);
	min_task_count = BitWizard::min_uint64(min_task_count, _task_count[selected_choice]);
	max_task_cardinality = BitWizard::max_uint32(max_task_cardinality, selected_cardinality);
	min_task_cardinality = BitWizard::min_uint32(min_task_cardinality, selected_cardinality);
	return selected_choice;
}

void CagPartionLib::CagHllPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		v.push_back(_task_cardinality[i]->cardinality_estimation());
	}
}
#endif // !CAG_PARTITIONER_H_
