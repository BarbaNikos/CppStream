#pragma once
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <climits>
#include <cstring>

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_

#if defined(_MSC_VER)

#include <intrin.h>
#define popcnt64(x)	__popcnt64(x)

#else	// defined(_MSC_VER)

#define popcnt64(x)	__builtin_popcountll(x)

#endif	// !defined(_MSC_VER)

#ifndef C_HLL_H_
#define C_HLL_H_

const double a_16 = 0.673;

const double a_32 = 0.697;

const double a_64 = 0.709;

typedef struct
{
	double current_sum;
	unsigned int p;
	unsigned int m;
	unsigned int bitmap_size;
	size_t bitmap_offset;
	unsigned int mem_size_in_bits;
	double _multiplier;
	char* bucket;
	double condition_value_test_1;
	double condition_value_test_2;
	double pow_2_to_32;
	double m_times_log_m;
	size_t v_size;
	uint64_t* v_bitmap;
	unsigned int v;
	size_t hash_code_len_in_bits;
}hll_8;

typedef struct
{
	double current_sum;
	unsigned int p;
	unsigned int m;
	unsigned int bitmap_size;
	size_t bitmap_offset;
	unsigned int mem_size_in_bits;
	double _multiplier;
	unsigned int* bucket;
	double condition_value_test_1;
	double condition_value_test_2;
	double pow_2_to_32;
	double m_times_log_m;
	size_t v_size;
	uint64_t* v_bitmap;
	unsigned int v;
	size_t hash_code_len_in_bits;
}hll_32;

void init_8(hll_8* _hll, unsigned int p, size_t hash_code_len_in_bytes);
void init_32(hll_32* _hll, unsigned int p, size_t hash_code_len_in_bytes);
void update_8(hll_8* _hll, uint32_t hash_value);
void update_8(hll_8* _hll, uint64_t hash_value);
void opt_update_8(hll_8* _hll, uint32_t hash_value);
void opt_update_8(hll_8* _hll, uint64_t hash_value);
void update_32(hll_32* _hll, uint32_t hash_value);
void update_32(hll_32* _hll, uint64_t hash_value);
void opt_update_32(hll_32* _hll, uint32_t hash_value);
void opt_update_32(hll_32* _hll, uint64_t hash_value);
uint64_t cardinality_estimation_8(hll_8* _hll);
uint64_t opt_cardinality_estimation_8(hll_8* _hll);
uint64_t new_cardinality_estimate_8(hll_8* _hll, uint32_t hash_value);
uint64_t new_cardinality_estimate_8(hll_8* _hll, uint64_t hash_value);
uint64_t opt_new_cardinality_estimate_8(hll_8* _hll, uint32_t hash_value);
uint64_t opt_new_cardinality_estimate_8(hll_8* _hll, uint64_t hash_value);
uint64_t cardinality_estimation_32(hll_32* _hll);
uint64_t opt_cardinality_estimation_32(hll_32* _hll);
void destroy_32(hll_32* _hll);
void destroy_8(hll_8* _hll);

#endif // C_HLL_H_