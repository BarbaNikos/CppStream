#pragma once
#include <iostream>
#include <cinttypes>

#ifndef PARTITION_POLICY_H_
#define PARTITION_POLICY_H_
class PartitionPolicy
{
public:
	virtual ~PartitionPolicy() {}
	virtual uint16_t get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality, 
		uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality, 
		uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality) = 0;
};

class CountAwarePolicy : public PartitionPolicy
{
public:
	uint16_t get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality,
		uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality,
		uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality);
};

class CardinalityAwarePolicy : public PartitionPolicy
{
public:
	uint16_t get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality,
		uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality,
		uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality);
};

class LoadAwarePolicy : public PartitionPolicy
{
public:
	uint16_t get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality,
		uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality,
		uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality);
};

inline uint16_t CountAwarePolicy::get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality,
	uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality,
	uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality)
{
	return first_count < second_count ? first_choice : second_choice;
}

inline uint16_t CardinalityAwarePolicy::get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality,
	uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality,
	uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality)
{
	return first_cardinality < second_cardinality ? first_choice : second_choice;
}

inline uint16_t LoadAwarePolicy::get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality,
	uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality,
	uint64_t min_count, uint64_t max_count, uint32_t min_cardinality, uint32_t max_cardinality)
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
#endif // !PARTITION_POLICY_H_
