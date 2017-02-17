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
	class CA : public Partitioner
	{
	public:
		CA(const std::vector<uint16_t>& tasks, const PartitionPolicy& policy, const CardinalityEstimator::cardinality_estimator<Key>& c) 
		: tasks(tasks), task_count(tasks.size(), 0), policy(policy.make_copy()), max_task_count(0), 
		    min_task_count(0), max_task_cardinality(0), min_task_cardinality(0) 
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				std::unique_ptr<CardinalityEstimator::cardinality_estimator<Key>> ptr(c.clone());
				card_e_.push_back(std::move(ptr));
			}
		}
		
		CA(const CA& p) 
		: tasks(p.tasks), task_count(p.task_count), policy(p.policy->make_copy()), max_task_count(p.max_task_count), 
		min_task_count(p.min_task_count), max_task_cardinality(p.max_task_cardinality), min_task_cardinality(p.min_task_cardinality) 
		{
			for (size_t i = 0; i < p.card_e_.size(); ++i)
			{
				std::unique_ptr<CardinalityEstimator::cardinality_estimator<Key>> ptr(p.card_e_[i]->clone());
				card_e_.push_back(std::move(ptr));
			}
		}

		void init() override
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				task_count[i] = 0;
				card_e_[i]->init();
			}
			max_task_count = 0;
			min_task_count = 0;
			max_task_cardinality = 0;
			min_task_cardinality = 0;
		}

		uint16_t partition_next(const void* key, const size_t key_len) override
		{
			uint64_t hash_one, hash_two, long_hash_one[2], long_hash_two[2];
			size_t first, second, selection;
			MurmurHash3_x64_128(key, key_len, 313, &long_hash_one);
			MurmurHash3_x64_128(key, key_len, 317, &long_hash_two);
			hash_one = long_hash_one[0] ^ long_hash_one[1];
			hash_two = long_hash_two[0] ^ long_hash_two[1];
			first = hash_one % tasks.size();
			second = hash_two % tasks.size();
			size_t first_cardinality = card_e_[first]->count();
			size_t second_cardinality = card_e_[second]->count();
			selection = policy->choose(first, task_count[first], first_cardinality,
				second, task_count[second], second_cardinality, min_task_count, max_task_count,
				min_task_cardinality, max_task_cardinality);
			card_e_[selection]->insert(hash_one);
			size_t new_cardinality = card_e_[selection]->count();
			task_count[selection] += 1;
			max_task_count = std::max(max_task_count, task_count[selection]);
			max_task_cardinality = std::max(max_task_cardinality, new_cardinality);
			// need to make the following faster
			min_task_count = *std::min_element(task_count.begin(), task_count.end());
			std::vector<size_t> v;
			get_cardinality_vector(v);
			min_task_cardinality = *std::min_element(v.begin(), v.end());
			return tasks[selection];
		}
		
		size_t get_min_task_count() const override { return min_task_count; }
		
		size_t get_max_task_count() const override { return max_task_count; }
		
		size_t get_max_cardinality() const { return max_task_cardinality; }
		
		size_t get_min_cardinality() const { return min_task_cardinality; }
		
		void  get_cardinality_vector(std::vector<size_t>& v)
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				v.push_back(card_e_[i]->count());
			}
		}
	private:
		std::vector<uint16_t> tasks;
		
		std::vector<size_t> task_count;

		std::vector<std::unique_ptr<CardinalityEstimator::cardinality_estimator<Key>>> card_e_;
		
		std::unique_ptr<PartitionPolicy> policy;
		
		size_t max_task_count;
		
		size_t min_task_count;
		
		size_t max_task_cardinality;
		
		size_t min_task_cardinality;
	};

	template<typename Key>
	class AN : public Partitioner
	{
	public:
		AN(const std::vector<uint16_t>& tasks, const CardinalityEstimator::cardinality_estimator<Key>& c) :
			tasks(tasks), task_count(tasks.size(), uint64_t(0)), max_task_count(0), min_task_count(0), 
			max_task_cardinality(0), min_task_cardinality(0)
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				std::unique_ptr<CardinalityEstimator::cardinality_estimator<Key>> ptr(c.clone());
				card_e_.push_back(std::move(ptr));
			}
		}
		AN(const AN& p) : tasks(p.tasks), task_count(p.task_count), max_task_count(p.max_task_count), min_task_count(p.min_task_count),
			max_task_cardinality(p.max_task_cardinality), min_task_cardinality(p.min_task_cardinality)
		{
			for (size_t i = 0; i < p.card_e_.size(); ++i)
			{
				std::unique_ptr<CardinalityEstimator::cardinality_estimator<Key>> ptr(p.card_e_[i]->clone());
				card_e_.push_back(std::move(ptr));
			}
		}

		void init() override
		{
			for (size_t i = 0; i < tasks.size(); ++i)
			{
				task_count[i] = 0;
				card_e_[i]->init();
			}
			max_task_count = 0;
			min_task_count = 0;
			max_task_cardinality = 0;
			min_task_cardinality = 0;
		}

		uint16_t partition_next(const void* key, const size_t key_len) override
		{
			uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
			size_t first_choice, second_choice;
			MurmurHash3_x64_128(key, key_len, 313, &long_hash_one);
			MurmurHash3_x64_128(key, key_len, 317, &long_hash_two);
			hash_one = long_hash_one[0] ^ long_hash_one[1];
			hash_two = long_hash_two[0] ^ long_hash_two[1];
			first_choice = hash_one % tasks.size();
			second_choice = hash_two % tasks.size();
			size_t first_card = card_e_[first_choice]->count();
			size_t second_card = card_e_[second_choice]->count();
			size_t first_new_card = card_e_[first_choice]->imitate_insert(hash_one);
			size_t second_new_card = card_e_[second_choice]->imitate_insert(hash_one);
			if (first_card == first_new_card)
			{
				task_count[first_choice] += 1;
				max_task_count = std::max(max_task_count, task_count[first_choice]);
				return tasks[first_choice];
			}
			else if (second_card == second_new_card)
			{
				task_count[second_choice] += 1;
				max_task_count = std::max(max_task_count, task_count[second_choice]);
				return tasks[second_choice];
			}
			else
			{
				size_t selected_choice = policy.choose(first_choice, task_count[first_choice], first_card,
					second_choice, task_count[second_choice], second_card, min_task_count, max_task_count,
					min_task_cardinality, max_task_cardinality);
				card_e_[selected_choice]->insert(hash_one);
				size_t selected_cardinality = card_e_[selected_choice]->count();
				task_count[selected_choice] += 1;
				max_task_count = std::max(max_task_count, task_count[selected_choice]);
				max_task_cardinality = std::max(max_task_cardinality, selected_cardinality);
				// need to make the following faster
				min_task_count = *std::min_element(task_count.begin(), task_count.end());
				std::vector<size_t> v;
				get_cardinality_vector(v);
				min_task_cardinality = *std::min_element(v.begin(), v.end());
				return tasks[selected_choice];
			}
		}
		
		size_t get_min_task_count() const override { return min_task_count; }
		
		size_t get_max_task_count() const override { return max_task_count; }
		
		size_t get_max_cardinality() const { return max_task_cardinality; }
		
		size_t get_min_cardinality() const { return min_task_cardinality; }
		
		void get_cardinality_vector(std::vector<size_t>& v) const
		{
			for (size_t i = 0; i < card_e_.size(); ++i)
			{
				v.push_back(card_e_[i]->count());
			}
		}

protected:
		
		std::vector<uint16_t> tasks;
		
		std::vector<size_t> task_count;
		
		std::vector<std::unique_ptr<CardinalityEstimator::cardinality_estimator<Key>>> card_e_;

		CardinalityAwarePolicy policy;
		
		size_t max_task_count;
		
		size_t min_task_count;
		
		size_t max_task_cardinality;
		
		size_t min_task_cardinality;
	};

	template<typename Key>
	class NewAN : public AN<Key>
	{
	public:
		NewAN(const std::vector<uint16_t>& tasks, const CardinalityEstimator::cardinality_estimator<Key>& c) : AN<Key>(tasks, c) {}
		uint16_t partition_next(const void* key, const size_t key_len) override
		{
			uint64_t hash_one, long_hash_one[2], hash_two, long_hash_two[2];
			size_t first_choice, second_choice;
			MurmurHash3_x64_128(key, key_len, 313, &long_hash_one);
			MurmurHash3_x64_128(key, key_len, 317, &long_hash_two);
			hash_one = long_hash_one[0] ^ long_hash_one[1];
			hash_two = long_hash_two[0] ^ long_hash_two[1];
			first_choice = hash_one % AN<Key>::tasks.size();
			second_choice = hash_two % AN<Key>::tasks.size();
			size_t first_card = AN<Key>::card_e_[first_choice]->count();
			size_t second_card = AN<Key>::card_e_[second_choice]->count();
			size_t first_new_card = AN<Key>::card_e_[first_choice]->imitate_insert(hash_one);
			size_t second_new_card = AN<Key>::card_e_[second_choice]->imitate_insert(hash_one);
			if (first_card == first_new_card)
			{
				AN<Key>::task_count[first_choice] += 1;
				AN<Key>::max_task_count = std::max(AN<Key>::max_task_count, AN<Key>::task_count[first_choice]);
				return AN<Key>::tasks[first_choice];
			}
			else if (second_card == second_new_card)
			{
				AN<Key>::task_count[second_choice] += 1;
				AN<Key>::max_task_count = std::max(AN<Key>::max_task_count, AN<Key>::task_count[second_choice]);
				return AN<Key>::tasks[second_choice];
			}
			else
			{
				size_t selected_choice = policy_.choose(first_choice, AN<Key>::task_count[first_choice], first_card,
					second_choice, AN<Key>::task_count[second_choice], second_card, AN<Key>::min_task_count, AN<Key>::max_task_count,
					AN<Key>::min_task_cardinality, AN<Key>::max_task_cardinality);
				AN<Key>::card_e_[selected_choice]->insert(hash_one);
				size_t selected_cardinality = AN<Key>::card_e_[selected_choice]->count();
				AN<Key>::task_count[selected_choice] += 1;
				AN<Key>::max_task_count = std::max(AN<Key>::max_task_count, AN<Key>::task_count[selected_choice]);
				AN<Key>::max_task_cardinality = std::max(AN<Key>::max_task_cardinality, selected_cardinality);
				// need to make the following faster
				AN<Key>::min_task_count = *std::min_element(AN<Key>::task_count.begin(), AN<Key>::task_count.end());
				std::vector<size_t> v;
				AN<Key>::get_cardinality_vector(v);
				AN<Key>::min_task_cardinality = *std::min_element(v.begin(), v.end());
				return AN<Key>::tasks[selected_choice];
			}
		}
	private:
		CountAwarePolicy policy_;
	};

}
#endif // !CA_PARTITION_LIB_H_