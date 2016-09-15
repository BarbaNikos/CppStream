#include "../include/CardinalityEstimator.h"

const double CardinalityEstimator::ProbCount::phi = 0.77351;

CardinalityEstimator::ProbCount::ProbCount()
{
	bitmap = uint64_t(0);
}

CardinalityEstimator::ProbCount::~ProbCount()
{
}

void CardinalityEstimator::ProbCount::update_bitmap_with_hashed_value(uint32_t hashed_value)
{
	uint32_t leftmost_bit = hashed_value == 0 ? uint32_t(0x80000000) : BitWizard::lowest_order_bit_index(hashed_value);
	bitmap |= leftmost_bit;
}

void CardinalityEstimator::ProbCount::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	this->update_bitmap_with_hashed_value(hash_result);
}

uint32_t CardinalityEstimator::ProbCount::cardinality_estimation()
{
	uint32_t negated_bitmap = bitmap ^ int32_t(-1);
	uint32_t leftmost_zero = BitWizard::lowest_order_bit_index(negated_bitmap);
	uint32_t R = BitWizard::log_base_2_of_power_of_2_uint(leftmost_zero);
	return uint32_t(std::pow(2, R) / CardinalityEstimator::ProbCount::phi);
}

const double CardinalityEstimator::HyperLoglog::a_16 = 0.673;

const double CardinalityEstimator::HyperLoglog::a_32 = 0.697;

const double CardinalityEstimator::HyperLoglog::a_64 = 0.709;

CardinalityEstimator::HyperLoglog::HyperLoglog(uint8_t k)
{
	_current_sum = double(0);
	m = (uint16_t)std::pow(2, k);
	this->k = k;
	buckets = new uint32_t[m];
	for (size_t i = 0; i < m; ++i)
	{
		buckets[i] = uint32_t(0);
		_current_sum += (double(1) / std::pow(2, uint32_t(0)));
	}
	a_m = 0.7213 / (1 + 1.079 / m);
	_multiplier = a_32 * m * m;
}

CardinalityEstimator::HyperLoglog::~HyperLoglog()
{
	delete[] buckets;
}

void CardinalityEstimator::HyperLoglog::update_bitmap_with_hashed_value(uint32_t hashed_value)
{
	uint32_t j = BitWizard::isolate_bits_32(32 - k, k, hashed_value) >> (32 - k);	// isolate k highest order bits
	uint32_t w = BitWizard::isolate_bits_32(0, 32 - k, hashed_value);				// isolate 32-k lowest order bits
	uint32_t lob = BitWizard::lowest_order_bit_index(w);
	uint32_t leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	uint32_t current = buckets[j];
	//_current_sum -= (double(1) / std::pow(2, buckets[j]));
	_current_sum -= (double(1) / double(1 << buckets[j]));
	// buckets[j] = current < leftmost_bit ? leftmost_bit : current;
	buckets[j] = BitWizard::max_uint32(current, leftmost_bit);
	//_current_sum += (double(1) / std::pow(2, buckets[j]));
	_current_sum += (double(1) / double(1 << buckets[j]));
}

void CardinalityEstimator::HyperLoglog::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	this->update_bitmap_with_hashed_value(hash_result);
}

uint32_t CardinalityEstimator::HyperLoglog::cardinality_estimation()
{
	double E = _multiplier / _current_sum;
	return (uint32_t)E;
}
