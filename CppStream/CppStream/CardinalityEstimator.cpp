#include "CardinalityEstimator.h"

CardinalityEstimator::ProbCount::ProbCount()
{
	bitmap = uint64_t(0);
}

CardinalityEstimator::ProbCount::~ProbCount()
{	
}

void CardinalityEstimator::ProbCount::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	// first reverse the hash_result
	uint32_t swapped_value = swap_bits_32(hash_result);
	// extract the rightmost-bit
	uint32_t rightmost_bit = swapped_value & -((int32_t)swapped_value);
	uint32_t leftmost_bit = swap_bits_32(rightmost_bit);
	bitmap = bitmap | (1 << leftmost_bit);
}

uint64_t CardinalityEstimator::ProbCount::cardinality_estimation()
{
	uint32_t swap_bitmap = swap_bits_32(bitmap);
	// take the negation
	int32_t negated_swap_bitmap = swap_bitmap ^ int32_t(-1);
	uint32_t rightmost_zero = negated_swap_bitmap & -negated_swap_bitmap;
	uint32_t leftmost_zero = swap_bits_32(rightmost_zero);
	return uint64_t(leftmost_zero);
}

inline uint32_t CardinalityEstimator::ProbCount::swap_bits_32(uint32_t v)
{
	v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
	v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
	v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
	v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
	v = (v >> 16) | (v << 16);
	return v;
}
