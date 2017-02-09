#pragma once
#include <iostream>

#ifndef PARTITION_POLICY_H_
#define PARTITION_POLICY_H_
class PartitionPolicy
{
public:
	virtual ~PartitionPolicy() {}
	virtual size_t choose(size_t first_choice, size_t first_count, size_t first_cardinality,
		size_t second_choice, size_t second_count, size_t second_cardinality,
		size_t min_count, size_t max_count, size_t min_cardinality, size_t max_cardinality) const { return 0; }
	virtual PartitionPolicy* make_copy() const { return new PartitionPolicy(); }
};

class CountAwarePolicy : public PartitionPolicy
{
public:
	size_t choose(size_t first_choice, size_t first_count, size_t first_cardinality,
		size_t second_choice, size_t second_count, size_t second_cardinality,
		size_t min_count, size_t max_count, size_t min_cardinality, size_t max_cardinality) const override 
	{
		return first_count < second_count ? first_choice : second_choice;
	}
	CountAwarePolicy* make_copy() const override { return new CountAwarePolicy(); }
};

class CardinalityAwarePolicy : public PartitionPolicy
{
public:
	size_t choose(size_t first_choice, size_t first_count, size_t first_cardinality,
		size_t second_choice, size_t second_count, size_t second_cardinality,
		size_t min_count, size_t max_count, size_t min_cardinality, size_t max_cardinality) const override
	{
		return first_cardinality < second_cardinality ? first_choice : second_choice;
	}
	CardinalityAwarePolicy* make_copy() const override { return new CardinalityAwarePolicy(); }
};

class LoadAwarePolicy : public PartitionPolicy
{
public:
	size_t choose(size_t first_choice, size_t first_count, size_t first_cardinality,
		size_t second_choice, size_t second_count, size_t second_cardinality,
		size_t min_count, size_t max_count, size_t min_cardinality, size_t max_cardinality) const override
	{
		size_t cardinality_range = max_cardinality - min_cardinality + 1;
		size_t count_range = max_count - min_count + 1;
		double first_norm_cardinality = double(first_cardinality - min_cardinality) / double(cardinality_range);
		double second_norm_cardinality = double(second_cardinality - min_cardinality) / double(cardinality_range);
		double first_norm_count = double(first_count - min_count) / double(count_range);
		double second_norm_count = double(second_count - min_count) / double(count_range);
		double first_score = first_norm_cardinality + first_norm_count;
		double second_score = second_norm_cardinality + second_norm_count;
		return first_score < second_score ? first_choice : second_choice;
	}
	LoadAwarePolicy* make_copy() const override { return new LoadAwarePolicy(); }
};
#endif // !PARTITION_POLICY_H_