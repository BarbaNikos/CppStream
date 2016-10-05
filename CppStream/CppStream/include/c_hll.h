#pragma once
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <climits>

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_


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
	unsigned int mem_size;
	double _multiplier;
	char* bucket;
}hll_8;

typedef struct
{
	double current_sum;
	unsigned int p;
	unsigned int m;
	unsigned int bitmap_size;
	unsigned int mem_size;
	double _multiplier;
	unsigned int* bucket;
}hll_32;

void init_8(hll_8* _hll, unsigned int p)
{
	unsigned int i;
	if (_hll != NULL)
	{
		_hll->current_sum = 0;
		_hll->p = p;
		_hll->m = pow(2, p);
		if (_hll->m == 0)
		{
			printf("number overflow for m with a given k value of %d.\n", p);
			exit(1);
		}
		_hll->bitmap_size = CHAR_BIT;
		_hll->mem_size = (unsigned int)_hll->bitmap_size * _hll->m;
		_hll->bucket = (char*)calloc(_hll->m, sizeof(char));
		for (i = 0; i < _hll->m; ++i)
		{
			_hll->bucket[i] = 0x00;
			_hll->current_sum += (1 / (pow(2, 0)));
		}
		_hll->_multiplier = a_16 * _hll->m * _hll->m;
		_hll->bitmap_offset = 32 - _hll->p - _hll->bitmap_size;
	}
	else
	{
		printf("empty hll structure provided.\n");
		exit(1);
	}
}

void init_32(hll_32* _hll, unsigned int p)
{
	unsigned int i;
	if (_hll != NULL)
	{
		_hll->current_sum = 0;
		_hll->p = p;
		_hll->m = pow(2, p);
		if (_hll->m == 0)
		{
			printf("number overflow for m with a given k value of %d.\n", p);
			exit(1);
		}
		_hll->bitmap_size = 32;
		_hll->mem_size = (unsigned int)_hll->bitmap_size * _hll->m;
		_hll->bucket = (unsigned int*)calloc(_hll->m, sizeof(unsigned int));
		for (i = 0; i < _hll->m; ++i)
		{
			_hll->bucket[i] = (unsigned int)0x00000000;
			_hll->current_sum += (1 / (pow(2, 0)));
		}
		_hll->_multiplier = a_32 * _hll->m * _hll->m;
	}
	else
	{
		printf("empty hll structure provided.\n");
		exit(1);
	}
}

void update_8(hll_8* _hll, unsigned int hash_value)
{
	unsigned int j;
	unsigned char w, current;
	// unsigned char lob, leftmost_bit;
	unsigned char rob, righmost_bit;
	j = hash_value >> (32 - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	// lob = BitWizard::lowest_order_bit_index(w);
	rob = BitWizard::highest_order_bit_index_arch(w);
	// leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	righmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	// leftmost_bit = 1 + (unsigned char)log2(lob);
	current = _hll->bucket[j];
	_hll->current_sum -= (double)((double)1 / (double)(1 << _hll->bucket[j]));
	// _hll->bucket[j] = max_uint8(current, leftmost_bit);
	_hll->bucket[j] = BitWizard::max_uint8(current, righmost_bit);
	_hll->current_sum += (double)((double)1 / (double)(1 << _hll->bucket[j]));
}

// Still looks at leading zeros as leftmost bits
void update_32(hll_32* _hll, unsigned int hash_value)
{
	unsigned int j, w, current;
	// unsigned int lob, leftmost_bit;
	unsigned int rob, rightmost_bit;
	j = hash_value >> (32 - _hll->p);
	w = BitWizard::isolate_bits_32(0, 32 - _hll->p, hash_value);
	// lob = BitWizard::lowest_order_bit_index(w);
	rob = BitWizard::highest_order_bit_index_arch(w);
	// leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	rightmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	current = _hll->bucket[j];
	_hll->current_sum -= (double)((double)1 / (double)(1 << _hll->bucket[j]));
	// _hll->bucket[j] = BitWizard::max_uint32(current, leftmost_bit);
	_hll->bucket[j] = BitWizard::max_uint32(current, rightmost_bit);
	_hll->current_sum += (double)((double)1 / (double)(1 << _hll->bucket[j]));
}

unsigned int cardinality_estimation_8(hll_8* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	return (unsigned int)E;
}

unsigned int new_cardinality_estimate_8(hll_8* _hll, unsigned int hash_value)
{
	unsigned int j;
	unsigned char w, current;
	double current_sum, E;
	uint8_t new_value;
	// unsigned char lob, leftmost_bit;
	unsigned char rob, righmost_bit;
	j = hash_value >> (32 - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	// lob = BitWizard::lowest_order_bit_index(w);
	rob = BitWizard::highest_order_bit_index_arch(w);
	// leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	righmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	// leftmost_bit = 1 + (unsigned char)log2(lob);
	current = _hll->bucket[j];
	current_sum = _hll->current_sum - (double)((double)1 / (double)(1 << _hll->bucket[j]));
	// _hll->bucket[j] = max_uint8(current, leftmost_bit);
	new_value = BitWizard::max_uint8(current, righmost_bit);
	current_sum += (double)((double)1 / (double)(1 << new_value));
	E = _hll->_multiplier / current_sum;
	return (unsigned int)E;
}

unsigned int cardinality_estimation_32(hll_32* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	return (unsigned int)E;
}

void destroy_8(hll_8* _hll)
{
	_hll->p = 0;
	_hll->m = 0;
	_hll->bitmap_size = 0;
	_hll->mem_size = 0;
	free(_hll->bucket);
}

void destroy_32(hll_32* _hll)
{
	_hll->p = 0;
	_hll->m = 0;
	_hll->bitmap_size = 0;
	_hll->mem_size = 0;
	free(_hll->bucket);
}

#endif // !C_HLL_H_