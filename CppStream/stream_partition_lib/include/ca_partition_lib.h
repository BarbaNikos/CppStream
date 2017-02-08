#pragma once
#include <vector>
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

#ifndef CARDINALITY_ESTIMATOR
#include "card_estimator.h"
#endif // !CARDINALITY_ESTIMATOR

#ifndef CA_PARTITION_LIB_H_
#define CA_PARTITION_LIB_H_
namespace CaPartitionLib
{
	template<typename Key>
	class CN : public Partitioner
	{
	public:
		CN(const std::vector<uint16_t>& tasks, PartitionPolicy* policy) 
		: tasks(tasks), task_count(tasks.size(), 0), 
			cardinality_estimator_(tasks.size(), CardinalityEstimator::naive_cardinality_estimator<Key>()), max_task_count(0), 
		    min_task_count(0), max_task_cardinality(0), min_task_cardinality(0) 
		{
			this->policy = policy->make_copy();
		}
		
		CN(const CN& p) 
		: tasks(p.tasks), task_count(p.task_count), max_task_count(p.max_task_count), 
		min_task_count(p.min_task_count), max_task_cardinality(p.max_task_cardinality), min_task_cardinality(p.min_task_cardinality) 
		{
			this->policy = p.policy->make_copy();
			cardinality_estimator_.clear();
			for (size_t i = 0; i < p.cardinality_estimator_.size(); ++i)
			{
				cardinality_estimator_.push_back(p.cardinality_estimator_[i]);
			}
		}
		
		~CN()
		{
			task_count.clear();
			delete policy;
			cardinality_estimator_.clear();
		}

		void init() override
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				task_count[i] = 0;
				cardinality_estimator_[i].init();
			}
			max_task_count = 0;
			min_task_count = 0;
			max_task_cardinality = 0;
			min_task_cardinality = 0;
		}

		unsigned int partition_next(const void* key, const size_t key_len) override
		{
			uint64_t hash_one, hash_two, long_hash_one[2], long_hash_two[2];
			size_t first, second, selection;
			MurmurHash3_x64_128(key, key_len, 313, &long_hash_one);
			MurmurHash3_x64_128(key, key_len, 317, &long_hash_two);
			hash_one = long_hash_one[0] ^ long_hash_one[1];
			hash_two = long_hash_two[0] ^ long_hash_two[1];
			first = hash_one % tasks.size();
			second = hash_two % tasks.size();
			size_t first_cardinality = cardinality_estimator_[first].count();
			size_t second_cardinality = cardinality_estimator_[second].count();
			selection = policy->get_score(first, task_count[first], first_cardinality,
				second, task_count[second], second_cardinality, min_task_count, max_task_count,
				min_task_cardinality, max_task_cardinality);
			cardinality_estimator_[selection].insert(hash_one);
			size_t new_cardinality = cardinality_estimator_[selection].count();
			task_count[selection] += 1;
			max_task_count = std::max(max_task_count, task_count[selection]);
			max_task_cardinality = std::max(max_task_cardinality, new_cardinality);
			// need to make the following faster
			min_task_count = *std::min_element(task_count.begin(), task_count.end());
			std::vector<unsigned long long> v;
			get_cardinality_vector(v);
			min_task_cardinality = *std::min_element(v.begin(), v.end());
			return tasks[selection];
		}
		
		unsigned long long get_min_task_count() override { return min_task_count; }
		
		unsigned long long get_max_task_count() override { return max_task_count; }
		
		unsigned long long get_max_cardinality() const { return max_task_cardinality; }
		
		unsigned long long get_min_cardinality() const { return min_task_cardinality; }
		
		void  get_cardinality_vector(std::vector<unsigned long long>& v)
		{
			for (size_t i = 0; i < cardinality_estimator_.size(); ++i)
			{
				v.push_back(uint64_t(cardinality_estimator_[i].count()));
			}
		}
	private:
		std::vector<uint16_t> tasks;
		
		std::vector<unsigned long long> task_count;
		
		std::vector<CardinalityEstimator::naive_cardinality_estimator<Key>> cardinality_estimator_;
		
		PartitionPolicy* policy;
		
		uint64_t max_task_count;
		
		uint64_t min_task_count;
		
		size_t max_task_cardinality;
		
		size_t min_task_cardinality;
	};

	template<typename Key>
	class CHIP : public Partitioner
	{
	public:
		CHIP(const std::vector<uint16_t>& tasks, PartitionPolicy* policy)
			: tasks(tasks), task_count(tasks.size(), 0),
			cardinality_estimator_(tasks.size(), CardinalityEstimator::hip_cardinality_estimator<Key>()), max_task_count(0),
			min_task_count(0), max_task_cardinality(0), min_task_cardinality(0)
		{
			this->policy = policy->make_copy();
		}

		CHIP(const CHIP& p)
			: tasks(p.tasks), task_count(p.task_count), max_task_count(p.max_task_count),
			min_task_count(p.min_task_count), max_task_cardinality(p.max_task_cardinality), min_task_cardinality(p.min_task_cardinality)
		{
			this->policy = p.policy->make_copy();
			cardinality_estimator_.clear();
			for (size_t i = 0; i < p.cardinality_estimator_.size(); ++i)
			{
				cardinality_estimator_.push_back(p.cardinality_estimator_[i]);
			}
		}

		~CHIP()
		{
			task_count.clear();
			delete policy;
			cardinality_estimator_.clear();
		}

		void init() override
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				task_count[i] = 0;
				cardinality_estimator_[i].init();
			}
			max_task_count = 0;
			min_task_count = 0;
			max_task_cardinality = 0;
			min_task_cardinality = 0;
		}

		unsigned int partition_next(const void* key, const size_t key_len) override
		{
			uint64_t hash_one, hash_two, long_hash_one[2], long_hash_two[2];
			size_t first, second, selection;
			MurmurHash3_x64_128(key, key_len, 313, &long_hash_one);
			MurmurHash3_x64_128(key, key_len, 317, &long_hash_two);
			hash_one = long_hash_one[0] ^ long_hash_one[1];
			hash_two = long_hash_two[0] ^ long_hash_two[1];
			first = hash_one % tasks.size();
			second = hash_two % tasks.size();
			size_t first_cardinality = cardinality_estimator_[first].count();
			size_t second_cardinality = cardinality_estimator_[second].count();
			selection = policy->get_score(first, task_count[first], first_cardinality,
				second, task_count[second], second_cardinality, min_task_count, max_task_count,
				min_task_cardinality, max_task_cardinality);
			cardinality_estimator_[selection].insert(hash_one);
			size_t new_cardinality = cardinality_estimator_[selection].count();
			task_count[selection] += 1;
			max_task_count = std::max(max_task_count, task_count[selection]);
			max_task_cardinality = std::max(max_task_cardinality, new_cardinality);
			// need to make the following faster
			min_task_count = *std::min_element(task_count.begin(), task_count.end());
			std::vector<unsigned long long> v;
			get_cardinality_vector(v);
			min_task_cardinality = *std::min_element(v.begin(), v.end());
			return tasks[selection];
		}

		unsigned long long get_min_task_count() override { return min_task_count; }

		unsigned long long get_max_task_count() override { return max_task_count; }

		unsigned long long get_max_cardinality() const { return max_task_cardinality; }

		unsigned long long get_min_cardinality() const { return min_task_cardinality; }

		void  get_cardinality_vector(std::vector<unsigned long long>& v)
		{
			for (size_t i = 0; i < cardinality_estimator_.size(); ++i)
			{
				v.push_back(uint64_t(cardinality_estimator_[i].count()));
			}
		}
	private:
		std::vector<uint16_t> tasks;

		std::vector<unsigned long long> task_count;

		std::vector<CardinalityEstimator::hip_cardinality_estimator<Key>> cardinality_estimator_;

		PartitionPolicy* policy;

		uint64_t max_task_count;

		uint64_t min_task_count;

		size_t max_task_cardinality;

		size_t min_task_cardinality;
	};

	//class AN_Partitioner : public Partitioner
	//{
	//public:
	//	AN_Partitioner() {}
	//	AN_Partitioner(const std::vector<uint16_t>& tasks) :
	//		tasks(tasks), task_count(tasks.size(), uint64_t(0)), task_cardinality(tasks.size(), std::unordered_set<uint64_t>()), 
	//		max_task_count(0), min_task_count(0), max_task_cardinality(0), min_task_cardinality(0) {}
	//	AN_Partitioner(const AN_Partitioner& p) : tasks(p.tasks), task_count(p.task_count),
	//		task_cardinality(p.task_cardinality), max_task_count(p.max_task_count), min_task_count(p.min_task_count),
	//		max_task_cardinality(p.max_task_cardinality), min_task_cardinality(p.min_task_cardinality) {}
	//	~AN_Partitioner()
	//	{
	//		for (auto i = task_cardinality.begin(); i != task_cardinality.end(); ++i)
	//		{
	//			i->clear();
	//		}
	//		task_count.clear();
	//		task_cardinality.clear();
	//	}
	//	void init() override
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			task_cardinality[i].clear();
	//			task_count[i] = 0;
	//		}
	//		max_task_count = 0;
	//		min_task_count = 0;
	//		max_task_cardinality = 0;
	//		min_task_cardinality = 0;
	//	}
	//	unsigned int partition_next(const void* key, const size_t key_len) override
	//	{
	//		uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
	//		unsigned int first_choice, second_choice;
	//		MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	//		MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	//		hash_one = long_hash_one[0] ^ long_hash_one[1];
	//		hash_two = long_hash_two[0] ^ long_hash_two[1];
	//		first_choice = static_cast<unsigned int>(hash_one) % tasks.size();
	//		second_choice = static_cast<unsigned int>(hash_two) % tasks.size();
	//		unsigned long first_card = task_cardinality[first_choice].size();
	//		unsigned long second_card = task_cardinality[second_choice].size();
	//		auto first_it_find = task_cardinality[first_choice].find(hash_one);
	//		auto second_it_find = task_cardinality[second_choice].find(hash_one);
	//		if (first_it_find != task_cardinality[first_choice].end())
	//		{
	//			task_count[first_choice] += 1;
	//			max_task_count = BitWizard::max_uint64(max_task_count, task_count[first_choice]);
	//			return tasks[first_choice];
	//		}
	//		else if (second_it_find != task_cardinality[second_choice].end())
	//		{
	//			task_count[second_choice] += 1;
	//			max_task_count = BitWizard::max_uint64(max_task_count, task_count[second_choice]);
	//			return tasks[second_choice];
	//		}
	//		else
	//		{
	//			unsigned int selected_choice = policy.get_score(first_choice, task_count[first_choice], first_card,
	//				second_choice, task_count[second_choice], second_card, min_task_count, max_task_count,
	//				min_task_cardinality, max_task_cardinality);
	//			task_cardinality[selected_choice].insert(hash_one);
	//			unsigned long long selected_cardinality = task_cardinality[selected_choice].size();
	//			task_count[selected_choice] += 1;
	//			max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	//			max_task_cardinality = BitWizard::max_uint64(max_task_cardinality, selected_cardinality);
	//			// need to make the following faster
	//			min_task_count = *std::min_element(task_count.begin(), task_count.end());
	//			std::vector<unsigned long long> v;
	//			get_cardinality_vector(v);
	//			min_task_cardinality = *std::min_element(v.begin(), v.end());
	//			return tasks[selected_choice];
	//		}
	//	}
	//	unsigned long long get_min_task_count() override { return min_task_count; }
	//	unsigned long long get_max_task_count() override { return max_task_count; }
	//	unsigned long long get_max_cardinality() const { return max_task_cardinality; }
	//	unsigned long long get_min_cardinality() const { return min_task_cardinality; }
	//	void get_cardinality_vector(std::vector<unsigned long long>& v) const
	//	{
	//		for (size_t i = 0; i < this->task_cardinality.size(); ++i)
	//		{
	//			v.push_back(this->task_cardinality[i].size());
	//		}
	//	}
	//private:
	//	std::vector<uint16_t> tasks;
	//	std::vector<unsigned long long> task_count;
	//	std::vector<std::unordered_set<uint64_t>> task_cardinality;
	//	const CardinalityAwarePolicy policy;
	//	uint64_t max_task_count;
	//	uint64_t min_task_count;
	//	uint64_t max_task_cardinality;
	//	uint64_t min_task_cardinality;
	//};

	//class CHLL_Partitioner : public Partitioner
	//{
	//public:
	//	CHLL_Partitioner() {}
	//	CHLL_Partitioner(const std::vector<uint16_t>& tasks, PartitionPolicy* policy, uint8_t k) : tasks(tasks),
	//		task_count(tasks.size(), uint64_t(0)), max_task_count(0), min_task_count(0), 
	//		max_task_cardinality(0), min_task_cardinality(0), k(k)
	//	{
	//		this->policy = policy->make_copy();
	//		this->task_cardinality = static_cast<Hll::hll_8**>(malloc(tasks.size() * sizeof(Hll::hll_8*)));
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			this->task_cardinality[i] = static_cast<Hll::hll_8*>(malloc(sizeof(Hll::hll_8)));
	//			Hll::init_8(this->task_cardinality[i], k, sizeof(uint64_t));
	//		}
	//	}
	//	CHLL_Partitioner(const CHLL_Partitioner& p) : tasks(p.tasks), task_count(p.task_count), policy(p.policy), 
	//		max_task_count(p.max_task_count), min_task_count(p.min_task_count), max_task_cardinality(p.max_task_cardinality), 
	//		min_task_cardinality(p.min_task_cardinality), k(p.k)
	//	{
	//		this->policy = p.policy->make_copy();
	//		// first initialize HLLs
	//		this->task_cardinality = static_cast<Hll::hll_8**>(malloc(tasks.size() * sizeof(Hll::hll_8*)));
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			this->task_cardinality[i] = static_cast<Hll::hll_8*>(malloc(sizeof(Hll::hll_8)));
	//			Hll::init_8(this->task_cardinality[i], this->k, sizeof(uint64_t));
	//		}
	//		// then copy HLL values
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			Hll::copy_8(this->task_cardinality[i], p.task_cardinality[i]);
	//		}
	//	}
	//	~CHLL_Partitioner()
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			Hll::destroy_8(this->task_cardinality[i]);
	//			free(this->task_cardinality[i]);
	//		}
	//		free(this->task_cardinality);
	//		delete policy;
	//	}
	//	void init() override
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			Hll::destroy_8(this->task_cardinality[i]);
	//			Hll::init_8(this->task_cardinality[i], k, sizeof(uint64_t));
	//		}
	//		max_task_count = 0;
	//		min_task_count = 0;
	//		max_task_cardinality = 0;
	//		min_task_cardinality = 0;
	//	}
	//	unsigned int partition_next(const void* key, const size_t key_len) override
	//	{
	//		uint64_t hash_one, hash_two;
	//		uint64_t long_hash_one[2], long_hash_two[2];
	//		unsigned int first_choice, second_choice;
	//		MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	//		MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	//		hash_one = long_hash_one[0] ^ long_hash_one[1];
	//		hash_two = long_hash_two[0] ^ long_hash_two[1];
	//		first_choice = static_cast<unsigned int>(hash_one) % tasks.size();
	//		second_choice = static_cast<unsigned int>(hash_two) % tasks.size();
	//		unsigned long first_card = static_cast<unsigned long>(Hll::opt_cardinality_estimation_8(this->task_cardinality[first_choice]));
	//		unsigned long second_card = static_cast<unsigned long>(Hll::opt_cardinality_estimation_8(this->task_cardinality[second_choice]));
	//		// decision
	//		unsigned int selected_choice = policy->get_score(first_choice, task_count[first_choice], first_card,
	//			second_choice, task_count[second_choice], second_card, min_task_count, max_task_count,
	//			min_task_cardinality, max_task_cardinality);
	//		// update metrics
	//		Hll::opt_update_8(this->task_cardinality[selected_choice], hash_one);
	//		unsigned long selected_cardinality = static_cast<unsigned long>(Hll::opt_cardinality_estimation_8(this->task_cardinality[selected_choice]));
	//		task_count[selected_choice] += 1;
	//		max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	//		max_task_cardinality = BitWizard::max_uint64(max_task_cardinality, selected_cardinality);
	//		// really slow
	//		std::vector<unsigned long long> v;
	//		get_cardinality_vector(v);
	//		min_task_cardinality = *std::min_element(v.begin(), v.end());
	//		min_task_count = *std::min_element(task_count.begin(), task_count.end());
	//		return tasks[selected_choice];
	//	}
	//	unsigned long long get_min_task_count() override { return min_task_count; }
	//	unsigned long long get_max_task_count() override { return max_task_count; }
	//	unsigned long get_max_cardinality() const { return max_task_cardinality; }
	//	unsigned long get_min_cardinality() const { return min_task_cardinality; }
	//	void get_cardinality_vector(std::vector<unsigned long long>& v) const
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			v.push_back(Hll::opt_cardinality_estimation_8(this->task_cardinality[i]));
	//		}
	//	}
	//private:
	//	Hll::hll_8** task_cardinality;
	//	std::vector<uint16_t> tasks;
	//	std::vector<unsigned long long> task_count;
	//	PartitionPolicy* policy;
	//	uint64_t max_task_count;
	//	uint64_t min_task_count;
	//	uint64_t max_task_cardinality;
	//	uint64_t min_task_cardinality;
	//	unsigned int k;
	//};

	//class AHLL_Partitioner : public Partitioner
	//{
	//public:
	//	AHLL_Partitioner() {}
	//	AHLL_Partitioner(const std::vector<uint16_t>& tasks, uint8_t k) : tasks(tasks), task_count(tasks.size(), 0), 
	//		max_task_count(0), min_task_count(0), max_task_cardinality(0), min_task_cardinality(0), k(k)
	//	{
	//		this->_task_cardinality = static_cast<Hll::hll_8**>(malloc(tasks.size() * sizeof(Hll::hll_8*)));
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			this->_task_cardinality[i] = static_cast<Hll::hll_8*>(malloc(sizeof(Hll::hll_8)));
	//			Hll::init_8(this->_task_cardinality[i], k, sizeof(uint64_t));
	//		}
	//	}
	//	AHLL_Partitioner(const AHLL_Partitioner& p) : tasks(p.tasks), task_count(p.task_count),
	//		max_task_count(p.max_task_count), min_task_count(p.min_task_count), max_task_cardinality(p.max_task_cardinality),
	//		min_task_cardinality(p.min_task_cardinality), k(p.k)
	//	{
	//		// first initialize HLLs
	//		this->_task_cardinality = static_cast<Hll::hll_8**>(malloc(tasks.size() * sizeof(Hll::hll_8*)));
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			this->_task_cardinality[i] = static_cast<Hll::hll_8*>(malloc(sizeof(Hll::hll_8)));
	//			Hll::init_8(this->_task_cardinality[i], k, sizeof(uint64_t));
	//		}
	//		// then copy HLL values
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			Hll::copy_8(this->_task_cardinality[i], p._task_cardinality[i]);
	//		}
	//	}
	//	~AHLL_Partitioner()
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			Hll::destroy_8(this->_task_cardinality[i]);
	//			free(this->_task_cardinality[i]);
	//		}
	//		free(this->_task_cardinality);
	//	}
	//	void init() override
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			Hll::destroy_8(this->_task_cardinality[i]);
	//			Hll::init_8(this->_task_cardinality[i], k, sizeof(uint64_t));
	//		}
	//		max_task_count = 0;
	//		min_task_count = 0;
	//		max_task_cardinality = 0;
	//		min_task_cardinality = 0;
	//	}
	//	unsigned int partition_next(const void* key, const size_t key_len) override
	//	{
	//		uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
	//		unsigned int first_choice, second_choice;
	//		MurmurHash3_x64_128(key, key_len, 13, &long_hash_one);
	//		MurmurHash3_x64_128(key, key_len, 17, &long_hash_two);
	//		hash_one = long_hash_one[0] ^ long_hash_one[1];
	//		hash_two = long_hash_two[0] ^ long_hash_two[1];
	//		first_choice = static_cast<unsigned int>(hash_one) % tasks.size();
	//		second_choice = static_cast<unsigned int>(hash_two) % tasks.size();
	//		unsigned long first_card = static_cast<unsigned long>(Hll::opt_cardinality_estimation_8(this->_task_cardinality[first_choice]));
	//		unsigned long second_card = static_cast<unsigned long>(Hll::opt_cardinality_estimation_8(this->_task_cardinality[second_choice]));
	//		// calculate new cardinality estimates (EXTRA COST)
	//		unsigned long first_card_est = static_cast<unsigned long>(Hll::opt_new_cardinality_estimate_8(this->_task_cardinality[first_choice], hash_one));
	//		unsigned long second_card_est = static_cast<unsigned long>(Hll::opt_new_cardinality_estimate_8(this->_task_cardinality[second_choice], hash_one));
	//		// decision
	//		if (first_card_est - first_card == 0)
	//		{
	//			Hll::opt_update_8(this->_task_cardinality[first_choice], hash_one);
	//			task_count[first_choice] += 1;
	//			max_task_count = BitWizard::max_uint64(max_task_count, task_count[first_choice]);
	//			// really slow
	//			std::vector<unsigned long long> v;
	//			get_cardinality_vector(v);
	//			min_task_cardinality = *std::min_element(v.begin(), v.end());
	//			min_task_count = *std::min_element(task_count.begin(), task_count.end());
	//			return tasks[first_choice];
	//		}
	//		else if (second_card_est - second_card == 0)
	//		{
	//			Hll::opt_update_8(this->_task_cardinality[second_choice], hash_one);
	//			task_count[second_choice] += 1;
	//			max_task_count = BitWizard::max_uint64(max_task_count, task_count[second_choice]);
	//			// really slow
	//			std::vector<unsigned long long> v;
	//			get_cardinality_vector(v);
	//			min_task_cardinality = *std::min_element(v.begin(), v.end());
	//			min_task_count = *std::min_element(task_count.begin(), task_count.end());
	//			return tasks[second_choice];
	//		}
	//		else
	//		{
	//			// behave like CHLL: send the tuple to the worker with the smallest 
	//			// cardinality at that point
	//			unsigned int selected_choice = first_card < second_card ? first_choice : second_choice;
	//			// update metrics
	//			Hll::opt_update_8(this->_task_cardinality[selected_choice], hash_one);
	//			unsigned long selected_cardinality = static_cast<unsigned long>(Hll::opt_cardinality_estimation_8(this->_task_cardinality[selected_choice]));
	//			task_count[selected_choice] += 1;
	//			// really slow
	//			std::vector<unsigned long long> v;
	//			get_cardinality_vector(v);
	//			min_task_cardinality = *std::min_element(v.begin(), v.end());
	//			min_task_count = *std::min_element(task_count.begin(), task_count.end());
	//			max_task_count = BitWizard::max_uint64(max_task_count, task_count[selected_choice]);
	//			max_task_cardinality = BitWizard::max_uint64(max_task_cardinality, selected_cardinality);
	//			return tasks[selected_choice];
	//		}
	//	}
	//	unsigned long long get_min_task_count() override { return min_task_count; }
	//	unsigned long long get_max_task_count() override { return max_task_count; }
	//	unsigned long get_max_cardinality() const { return max_task_cardinality; }
	//	unsigned long get_min_cardinality() const { return min_task_cardinality; }
	//	void get_cardinality_vector(std::vector<unsigned long long>& v) const
	//	{
	//		for (size_t i = 0; i < tasks.size(); ++i)
	//		{
	//			v.push_back(Hll::opt_cardinality_estimation_8(this->_task_cardinality[i]));
	//		}
	//	}
	//private:
	//	std::vector<uint16_t> tasks;
	//	std::vector<unsigned long long> task_count;
	//	Hll::hll_8** _task_cardinality;
	//	uint64_t max_task_count;
	//	uint64_t min_task_count;
	//	uint64_t max_task_cardinality;
	//	uint64_t min_task_cardinality;
	//	unsigned int k;
	//};
}
#endif // !CA_PARTITION_LIB_H_