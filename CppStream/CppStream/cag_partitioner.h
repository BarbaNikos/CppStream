#pragma once
#include <vector>
#include <unordered_set>

#include "MurmurHash3.h"
#include "CardinalityEstimator.h"

#ifndef CAG_PARTITIONER_H_
#define CAG_PARTITIONER_H_
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
	std::vector<uint64_t> task_count;
	std::vector<CardinalityEstimator::ProbCount> task_cardinality;
};

class CagHllPartitioner
{
public:
	CagHllPartitioner(const std::vector<uint16_t>& tasks, uint16_t k);
	~CagHllPartitioner();
	uint16_t partition_next(const std::string& key, size_t key_len);
	void get_cardinality_vector(std::vector<uint32_t>& v);
private:
	std::vector<uint16_t> tasks;
	std::vector<uint64_t> task_count;
	std::vector<CardinalityEstimator::HyperLoglog> task_cardinality;
	CardinalityEstimator::HyperLoglog** _task_cardinality;
};

inline CagNaivePartitioner::CagNaivePartitioner(const std::vector<uint16_t>& tasks) :
	tasks(tasks), task_count(tasks.size(), uint64_t(0)),
	task_cardinality(tasks.size(), std::unordered_set<uint32_t>())
{

}

inline CagNaivePartitioner::~CagNaivePartitioner()
{
}

inline uint16_t CagNaivePartitioner::partition_next(const std::string & key, size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint16_t selected_choice = task_cardinality[first_choice].size() < task_cardinality[second_choice].size() ? first_choice : second_choice;
	task_cardinality[selected_choice].insert(hash_one);
	task_count[selected_choice] += 1;
	return selected_choice;
}

inline void CagNaivePartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	{
		v.push_back(this->task_cardinality[i].size());
	}
}

inline CagPcPartitioner::CagPcPartitioner(const std::vector<uint16_t>& tasks) :
	tasks(tasks), task_count(tasks.size(), uint64_t(0)), 
	task_cardinality(tasks.size(), CardinalityEstimator::ProbCount())
{
}

inline CagPcPartitioner::~CagPcPartitioner()
{
}

inline uint16_t CagPcPartitioner::partition_next(const std::string & key, size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint16_t selected_choice = task_cardinality[first_choice].cardinality_estimation() < task_cardinality[second_choice].cardinality_estimation() ? 
		first_choice : second_choice;
	task_cardinality[selected_choice].update_bitmap_with_hashed_value(hash_one);
	task_count[selected_choice] += 1;
	return selected_choice;
}

inline void CagPcPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	{
		v.push_back(this->task_cardinality[i].cardinality_estimation());
	}
}

inline CagHllPartitioner::CagHllPartitioner(const std::vector<uint16_t>& tasks, uint16_t k) :
	tasks(tasks), task_count(tasks.size(), uint64_t(0)), 
	task_cardinality(tasks.size(), CardinalityEstimator::HyperLoglog(k))
{
	_task_cardinality = new CardinalityEstimator::HyperLoglog*[tasks.size()];
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		_task_cardinality[i] = new CardinalityEstimator::HyperLoglog(k);
	}
}

inline CagHllPartitioner::~CagHllPartitioner()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete _task_cardinality[i];
	}
	delete[] _task_cardinality;
}

inline uint16_t CagHllPartitioner::partition_next(const std::string & key, size_t key_len)
{
	uint32_t hash_one, hash_two;
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &hash_one);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &hash_two);
	first_choice = hash_one % tasks.size();
	second_choice = hash_two % tasks.size();
	uint32_t first_card = _task_cardinality[first_choice]->cardinality_estimation();
	uint32_t second_card = _task_cardinality[second_choice]->cardinality_estimation();
	uint16_t selected_choice = first_card < second_card ? first_choice : second_choice;
	/*uint16_t selected_choice = task_cardinality[first_choice].cardinality_estimation() < task_cardinality[second_choice].cardinality_estimation() ?
		first_choice : second_choice;*/
	//task_cardinality[selected_choice].update_bitmap_with_hashed_value(hash_one);
	_task_cardinality[selected_choice]->update_bitmap_with_hashed_value(hash_one);
	task_count[selected_choice] += 1;
	return selected_choice;
}

inline void CagHllPartitioner::get_cardinality_vector(std::vector<uint32_t>& v)
{
	for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	{
		//v.push_back(this->task_cardinality[i].cardinality_estimation());
		v.push_back(_task_cardinality[i]->cardinality_estimation());
	}
}

#endif // !CAG_PARTITIONER_H_
