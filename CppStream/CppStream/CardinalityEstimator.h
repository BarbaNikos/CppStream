#pragma once
#include <iostream>
#include <cmath>
#include <cstdlib>

#include "MurmurHash3.h"
#include "BitTrickBox.h"

#ifndef CARDINALITY_ESTIMATOR_
#define CARDINALITY_ESTIMATOR_
namespace CardinalityEstimator
{
	class ProbCount
	{
	public:
		ProbCount();
		~ProbCount();
		void update_bitmap_with_hashed_value(uint32_t hashed_value);
		void update_bitmap(uint32_t value);
		uint32_t cardinality_estimation();
	private:
		const static double phi;
		uint32_t bitmap;
	};

	class HyperLoglog
	{
	public:
		HyperLoglog(uint8_t k);
		~HyperLoglog();
		void update_bitmap_with_hashed_value(uint32_t hashed_value);
		void update_bitmap(uint32_t value);
		uint32_t cardinality_estimation();
	private:
		const static double a_16;
		const static double a_32;
		const static double a_64;
		double a_m;
		uint16_t k;
		uint16_t m;
		uint32_t* buckets;
	};
}

const double CardinalityEstimator::ProbCount::phi = 0.77351;

CardinalityEstimator::ProbCount::ProbCount()
{
	bitmap = uint64_t(0);
}

CardinalityEstimator::ProbCount::~ProbCount()
{
}

inline void CardinalityEstimator::ProbCount::update_bitmap_with_hashed_value(uint32_t hashed_value)
{
	uint32_t leftmost_bit = hashed_value == 0 ? uint32_t(0x80000000) : BitWizard::lowest_order_bit_index(hashed_value);
	bitmap |= leftmost_bit;
}

inline void CardinalityEstimator::ProbCount::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	uint32_t leftmost_bit = hash_result == 0 ? uint32_t(0x80000000) : BitWizard::lowest_order_bit_index(hash_result);
	bitmap |= leftmost_bit;
}

inline uint32_t CardinalityEstimator::ProbCount::cardinality_estimation()
{
	uint32_t negated_bitmap = bitmap ^ int32_t(-1);
	uint32_t leftmost_zero = BitWizard::lowest_order_bit_index(negated_bitmap);
	uint32_t R = BitWizard::log_base_2_of_power_of_2_uint(leftmost_zero);
	return std::pow(2, R) / CardinalityEstimator::ProbCount::phi;
}

const double CardinalityEstimator::HyperLoglog::a_16 = 0.673;

const double CardinalityEstimator::HyperLoglog::a_32 = 0.697;

const double CardinalityEstimator::HyperLoglog::a_64 = 0.709;

CardinalityEstimator::HyperLoglog::HyperLoglog(uint8_t k)
{
	m = std::pow(2, k);
	this->k = k;
	buckets = new uint32_t[m];
	for (size_t i = 0; i < m; i++)
	{
		buckets[i] = uint32_t(0);
	}
	a_m = 0.7213 / (1 + 1.079 / m);
}

CardinalityEstimator::HyperLoglog::~HyperLoglog()
{
	delete[] buckets;
	buckets = nullptr;
}

inline void CardinalityEstimator::HyperLoglog::update_bitmap_with_hashed_value(uint32_t hashed_value)
{
	uint32_t j = BitWizard::isolate_bits_32(32 - k, k, hashed_value);
	uint32_t s_j = j >> (32 - k);	// isolate k highest order bits
	uint32_t w = BitWizard::isolate_bits_32(0, 32 - k, hashed_value);	// isolate 32-k lowest order bits
	uint32_t lob = BitWizard::lowest_order_bit_index(w);
	uint32_t leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	if (s_j >= m)
	{
		int st = s_j + j;
		std::cout << "here s_j points to " << s_j << " when m is: " << m << std::endl;
		exit(1);
	}
	uint32_t current = buckets[s_j];
	buckets[s_j] = current < leftmost_bit ? leftmost_bit : current;
	//buckets[j] = BitWizard::return_max_uint32(buckets[j], leftmost_bit);
}

inline void CardinalityEstimator::HyperLoglog::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	uint32_t j = BitWizard::isolate_bits_32(32 - k, k, hash_result) >> (32 - k);	// isolate k highest order bits
	uint32_t w = BitWizard::isolate_bits_32(0, 32 - k, hash_result);	// isolate 32-k lowest order bits
	uint32_t leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(BitWizard::lowest_order_bit_index(w));
	buckets[j] = buckets[j] < leftmost_bit ? leftmost_bit : buckets[j];
	// buckets[j] = BitWizard::return_max_uint32(buckets[j], leftmost_bit);
}

inline uint32_t CardinalityEstimator::HyperLoglog::cardinality_estimation()
{
	double sum = 0;
	for (size_t i = 0; i < m; ++i)
	{
		if (buckets[i] != 0)
		{
			sum += (double(1) / std::pow(2, buckets[i]));
		}
	}
	double E = a_32 * m * m * double(1) / sum;
	return E;
}

#endif // !CARDINALITY_ESTIMATOR_