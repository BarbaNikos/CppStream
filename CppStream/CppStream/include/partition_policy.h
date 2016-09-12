#pragma once
#include <iostream>
#include <cinttypes>

#ifndef PARTITION_POLICY_H_
#define PARTITION_POLICY_H_
class PartitionPolicy
{
public:
	virtual double get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality, uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality) = 0;
};

class CardinalityAwarePolicy : PartitionPolicy
{
public:
	static double get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality, uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality);
};

class LoadAwarePolicy : PartitionPolicy
{
public:
	double get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality, uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality);
};

double CardinalityAwarePolicy::get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality, uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality)
{
	return first_cardinality < second_cardinality ? first_choice : second_choice;
}

double LoadAwarePolicy::get_score(uint16_t first_choice, uint64_t first_count, uint32_t first_cardinality, uint16_t second_choice, uint64_t second_count, uint32_t second_cardinality)
{
	return double(0);
}
#endif // !PARTITION_POLICY_H_
