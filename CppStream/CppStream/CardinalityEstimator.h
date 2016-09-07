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
		void update_bitmap(uint32_t value);
		uint32_t cardinality_estimation();
	private:
		uint32_t bitmap;
	};

	class HyperLoglog
	{
	public:
		HyperLoglog(uint8_t k);
		~HyperLoglog();
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
	uint32_t leftmost_bit = hash_result == 0 ? uint32_t(31) : BitWizard::lowest_order_bit_index(hash_result);
	bitmap |= leftmost_bit;
}

inline uint32_t CardinalityEstimator::ProbCount::cardinality_estimation()
{
	uint32_t negated_bitmap = bitmap ^ int32_t(-1);
	uint32_t leftmost_zero = BitWizard::lowest_order_bit_index(negated_bitmap);
	uint32_t R = log2(leftmost_zero);
	return pow(2, R) / 0.77531;
}

const double CardinalityEstimator::HyperLoglog::a_16 = 0.673;

const double CardinalityEstimator::HyperLoglog::a_32 = 0.697;

const double CardinalityEstimator::HyperLoglog::a_64 = 0.709;

CardinalityEstimator::HyperLoglog::HyperLoglog(uint8_t k)
{
	this->m = pow(2, k);
	this->k = k;
	buckets = (uint32_t*)std::calloc(this->m, sizeof(uint32_t));
	a_m = 0.7213 / (1 + 1.079 / m);
}

CardinalityEstimator::HyperLoglog::~HyperLoglog()
{
	free(buckets);
	buckets = nullptr;
}

inline void CardinalityEstimator::HyperLoglog::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	uint32_t j = BitWizard::isolate_bits_32(32 - k, k, hash_result) >> (32 - k);	// isolate k highest order bits
	uint32_t w = BitWizard::isolate_bits_32(0, 32 - k, hash_result);	// isolate 32-k lowest order bits
	uint32_t leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(BitWizard::lowest_order_bit_index(w));
	buckets[j] = buckets[j] < leftmost_bit ? leftmost_bit : buckets[j];
}

inline uint32_t CardinalityEstimator::HyperLoglog::cardinality_estimation()
{
	double sum = 0;
	for (size_t i = 0; i < m; ++i)
	{
		sum += (double(1) / pow(2, buckets[i]));
	}
	double Z = double(1) / sum;
	double E = a_32 * m * m * Z;
	return E;
	// Corrections proposed by Heule et al. from Hyperloglog in Practice: Algorithmic Engineering of a State of the 
	// art cardinality estimation algorithm
	/*double E_star;
	if (E <= double(5/2) * double(m))
	{
		size_t V = 0;
		for (size_t i = 0; i < m; i++)
		{
			if (buckets[i] == 0)
			{
				V++;
			}
		}
		E_star = m * log(m / V);
	}
	else if (E <= double(1/30 * pow(2, 32)))
	{
		E_star = E;
	}
	else
	{
		E_star = -1 * pow(2, 32) * log(1 - E / pow(2, 32));
	}
	return E_star;*/
}

#endif // !CARDINALITY_ESTIMATOR_