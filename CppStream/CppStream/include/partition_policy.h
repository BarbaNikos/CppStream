#pragma once
#include <iostream>
#include <cinttypes>

#ifndef PARTITION_POLICY_H_
#define PARTITION_POLICY_H_
class PartitionPolicy
{
public:
	virtual ~PartitionPolicy() {}
	virtual uint16_t get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
		uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
		unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const { return 0; }
	virtual PartitionPolicy* make_copy() { return new PartitionPolicy(); }
};

class CountAwarePolicy : public PartitionPolicy
{
public:
	uint16_t get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
		uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
		unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const override;
	CountAwarePolicy* make_copy() override { return new CountAwarePolicy(); }
};

class CardinalityAwarePolicy : public PartitionPolicy
{
public:
	uint16_t get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
		uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
		unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const override;
	CardinalityAwarePolicy* make_copy() override { return new CardinalityAwarePolicy(); }
};

class LoadAwarePolicy : public PartitionPolicy
{
public:
	uint16_t get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
		uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
		unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const override;
	LoadAwarePolicy* make_copy() override { return new LoadAwarePolicy(); }
};
#endif // !PARTITION_POLICY_H_

inline uint16_t CountAwarePolicy::get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
	uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
	unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const
{
	return first_count < second_count ? first_choice : second_choice;
}

inline uint16_t CardinalityAwarePolicy::get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
	uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
	unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const
{
	return first_cardinality < second_cardinality ? first_choice : second_choice;
}

inline uint16_t LoadAwarePolicy::get_score(uint16_t first_choice, unsigned long long first_count, unsigned long first_cardinality,
	uint16_t second_choice, unsigned long long second_count, unsigned long second_cardinality,
	unsigned long long min_count, unsigned long long max_count, unsigned long min_cardinality, unsigned long max_cardinality) const
{
	uint32_t cardinality_range = max_cardinality - min_cardinality + 1;
	uint64_t count_range = max_count - min_count + 1;
	uint32_t first_norm_cardinality = (first_cardinality - min_cardinality) / cardinality_range;
	uint32_t second_norm_cardinality = (second_cardinality - min_cardinality) / cardinality_range;
	uint64_t first_norm_count = (first_count - min_count) / count_range;
	uint64_t second_norm_count = (second_count - min_count) / count_range;
	uint64_t first_score = first_norm_cardinality + first_norm_count;
	uint64_t second_score = second_norm_cardinality + second_norm_count;
	return first_score < second_score ? first_choice : second_choice;
}
