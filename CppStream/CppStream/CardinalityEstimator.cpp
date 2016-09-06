#pragma once
#include "CardinalityEstimator.h"
#include "BitTrickBox.h"

CardinalityEstimator::ProbCount::ProbCount()
{
	bitmap = uint64_t(0);
}

CardinalityEstimator::ProbCount::~ProbCount()
{
}

inline void CardinalityEstimator::ProbCount::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	uint32_t leftmost_bit = BitWizard::lowest_order_bit_index(hash_result);
	bitmap = bitmap | (1 << leftmost_bit);
}

inline uint32_t CardinalityEstimator::ProbCount::cardinality_estimation()
{
	uint32_t negated_bitmap = bitmap ^ int32_t(-1);
	uint32_t rightmost_zero = BitWizard::highest_order_bit_index(negated_bitmap);
	uint32_t leftmost_zero = BitWizard::swap_bits_32(rightmost_zero);
	return leftmost_zero;
}