#pragma once
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <cinttypes>

#ifndef MURMURHASH_3_H_
#include "murmurhash_3.h"
#endif // !MURMURHASH_3_H_


#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_


#ifndef CARDINALITY_ESTIMATION_UTILS_H_
#define CARDINALITY_ESTIMATION_UTILS_H_

namespace Cardinality_Estimation_Utils
{
	class ProbCount
	{
	public:
		ProbCount(size_t bitmap_length_in_bits);
		~ProbCount();
		void update_bitmap_with_hashed_value_32(uint32_t hash_value);
		void update_bitmap_with_hashed_value_64(uint64_t hash_value);
		void update_bitmap_32(uint32_t value);
		void update_bitmap_64(uint64_t value);
		void set_bitmap_32(uint32_t value);
		void set_bitmap_64(uint64_t value);
		size_t cardinality_estimation_32();
		size_t cardinality_estimation_64();
	private:
		const static double phi;
		uint32_t bitmap_32;
		uint32_t negated_bitmap_32;
		uint64_t bitmap_64;
		uint64_t negated_bitmap_64;
	};

	class HyperLoglog
	{
	public:
		HyperLoglog(uint8_t k);
		~HyperLoglog();
		void update_bitmap_with_hashed_value(uint32_t hashed_value);
		uint32_t new_cardinality_estimate(uint32_t hashed_value);
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
		double _current_sum;
		double _multiplier;
	};
}
#endif // !CARDINALITY_ESTIMATION_UTILS_H_

inline Cardinality_Estimation_Utils::ProbCount::ProbCount(size_t bitmap_length_in_bits)
{
	switch (bitmap_length_in_bits)
	{
	case 32:
		bitmap_32 = uint32_t(0);
		break;
	case 64:
		bitmap_64 = uint64_t(0);
		break;
	default:
		std::cout << "ProbCount: available bitmap lengths are either 32 or 64 bits.\n";
		exit(1);
	}
}

inline Cardinality_Estimation_Utils::ProbCount::~ProbCount()
{
}

inline void Cardinality_Estimation_Utils::ProbCount::update_bitmap_with_hashed_value_32(uint32_t hashed_value)
{
	uint32_t least_significant_bit = hashed_value == 0 ? uint32_t(0x80000000) : BitWizard::lowest_order_bit_index(hashed_value);
	bitmap_32 = bitmap_32 | least_significant_bit;
}

inline void Cardinality_Estimation_Utils::ProbCount::update_bitmap_with_hashed_value_64(uint64_t hash_value)
{
	uint64_t least_significant_bit = hash_value == 0 ? uint64_t(0x8000000000000000) : BitWizard::lowest_order_bit_index(hash_value);
	bitmap_64 = bitmap_64 | least_significant_bit;
}

inline void Cardinality_Estimation_Utils::ProbCount::update_bitmap_32(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	update_bitmap_with_hashed_value_32(hash_result);
}

inline void Cardinality_Estimation_Utils::ProbCount::update_bitmap_64(uint64_t value)
{
	uint64_t long_hash_code[2], hash_code;
	MurmurHash3_x64_128(&value, sizeof(value), 13, &long_hash_code);
	hash_code = long_hash_code[0] ^ long_hash_code[1];
	update_bitmap_with_hashed_value_64(hash_code);
}

inline void Cardinality_Estimation_Utils::ProbCount::set_bitmap_32(uint32_t value)
{
	bitmap_32 = value;
}

inline void Cardinality_Estimation_Utils::ProbCount::set_bitmap_64(uint64_t value)
{
	bitmap_64 = value;
}

inline size_t Cardinality_Estimation_Utils::ProbCount::cardinality_estimation_32()
{
	uint32_t negated_bitmap_32 = bitmap_32 ^ int32_t(-1);
	uint32_t leftmost_zero = BitWizard::lowest_order_bit_index(negated_bitmap_32);
	// uint32_t R = log2(leftmost_zero);
	// uint32_t two_to_R = std::pow(2, R);
	// return size_t(two_to_R / Cardinality_Estimation_Utils::ProbCount::phi);
	return size_t(leftmost_zero / Cardinality_Estimation_Utils::ProbCount::phi);
}

inline size_t Cardinality_Estimation_Utils::ProbCount::cardinality_estimation_64()
{
	uint64_t negated_bitmap_64 = bitmap_64 ^ int64_t(-1);
	uint64_t leftmost_zero = BitWizard::lowest_order_bit_index(negated_bitmap_64); // 2^x
	// uint32_t R = log2(leftmost_zero); // x
	// uint32_t two_to_R = std::pow(2, R); // 2^x
	// return size_t(two_to_R / Cardinality_Estimation_Utils::ProbCount::phi);
	return size_t(leftmost_zero / Cardinality_Estimation_Utils::ProbCount::phi);
}

inline Cardinality_Estimation_Utils::HyperLoglog::HyperLoglog(uint8_t k)
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

inline Cardinality_Estimation_Utils::HyperLoglog::~HyperLoglog()
{
	delete[] buckets;
}

inline void Cardinality_Estimation_Utils::HyperLoglog::update_bitmap_with_hashed_value(uint32_t hashed_value)
{
	//uint32_t j = BitWizard::isolate_bits_32(32 - k, k, hashed_value) >> (32 - k);	// isolate k highest order bits
	uint32_t j = hashed_value >> (32 - k);	// isolate k highest order bits
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

inline uint32_t Cardinality_Estimation_Utils::HyperLoglog::new_cardinality_estimate(uint32_t hashed_value)
{
	//uint32_t j = BitWizard::isolate_bits_32(32 - k, k, hashed_value) >> (32 - k);	// isolate k highest order bits
	uint32_t j = hashed_value >> (32 - k);	// isolate k highest order bits
	uint32_t w = BitWizard::isolate_bits_32(0, 32 - k, hashed_value);				// isolate 32-k lowest order bits
	uint32_t lob = BitWizard::lowest_order_bit_index(w);
	uint32_t leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	uint32_t current = buckets[j];
	double _estimate_sum = _current_sum - (double(1) / double(1 << buckets[j]));
	uint32_t new_bucket_value = BitWizard::max_uint32(current, leftmost_bit);
	_estimate_sum += (double(1) / double(1 << new_bucket_value));
	double E = _multiplier / _estimate_sum;
	return (uint32_t)E;
}

inline void Cardinality_Estimation_Utils::HyperLoglog::update_bitmap(uint32_t value)
{
	uint32_t hash_result;
	MurmurHash3_x86_32(&value, sizeof(value), 13, &hash_result);
	this->update_bitmap_with_hashed_value(hash_result);
}

inline uint32_t Cardinality_Estimation_Utils::HyperLoglog::cardinality_estimation()
{
	double E = _multiplier / _current_sum;
	return (uint32_t)E;
}