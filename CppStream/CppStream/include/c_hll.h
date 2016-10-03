#pragma once
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <climits>

#ifndef BIT_TRICK_UTILS_H_
#include "bit_trick_utils.h"
#endif // !BIT_TRICK_UTILS_H_


const double a_16 = 0.673;

const double a_32 = 0.697;

const double a_64 = 0.709;

typedef struct
{
	double current_sum;
	unsigned int k;
	unsigned int m;
	unsigned int bitmap_size;
	size_t bitmap_offset;
	unsigned int mem_size;
	double _multiplier;
	char* bucket;
}hll;

typedef struct
{
	double current_sum;
	unsigned int k;
	unsigned int m;
	unsigned int bitmap_size;
	unsigned int mem_size;
	double _multiplier;
	unsigned int* bucket;
}hll_32;

unsigned int lowest_order_bit_32(unsigned int x)
{
	return x == 0 ? 0x80000000 : x & (~x + 1);
}

unsigned short lowest_order_bit_16(unsigned short x)
{
	return x == 0 ? 0x8000 : x & (~x + 1);
}

unsigned char lowest_order_bit_8(unsigned char x)
{
	return x == 0 ? 0x80 : x & (~x + 1);
}

unsigned int isolate_bits_32(size_t offset, size_t length, unsigned int value)
{
	return value & (((((unsigned int)1) << length) - ((unsigned int)1)) << offset);
}

unsigned short isolate_bits_16(size_t offset, size_t length, unsigned short value)
{
	return value & (((((unsigned short)1) << length) - (unsigned short)1) << offset);
}

unsigned char isolate_bits_8(size_t offset, size_t length, unsigned int value)
{
	return (unsigned char)(value & (((((unsigned int)1) << length) - ((unsigned int)1)) << offset)) >> (32 - offset);
}

unsigned char max_uint8(unsigned char x, unsigned char y)
{
	return x ^ ((x ^ y) & -(x < y)); // max(x, y)
}

unsigned int max_uint32(unsigned int x, unsigned int y)
{
	return x ^ ((x ^ y) & -(x < y)); // max(x, y)
}

void init(hll* _hll, unsigned int expected_cardinality, unsigned int k)
{
	unsigned int i;
	if (_hll != NULL)
	{
		_hll->current_sum = 0;
		_hll->k = k;
		_hll->m = pow(2, k);
		if (_hll->m == 0)
		{
			printf("number overflow for m with a given k value of %d.\n", k);
			exit(1);
		}
		_hll->bitmap_size = CHAR_BIT;
		_hll->mem_size = (unsigned int)_hll->bitmap_size * _hll->m;
		_hll->bucket = (char*)calloc(_hll->m, sizeof(char));
		for (i = 0; i < _hll->m; ++i)
		{
			_hll->bucket[i] = (unsigned char)0x00;
			_hll->current_sum += (1 / (pow(2, 0)));
		}
		_hll->_multiplier = a_16 * _hll->m * _hll->m;
		_hll->bitmap_offset = 32 - _hll->k - _hll->bitmap_size;
	}
	else
	{
		printf("empty hll structure provided.\n");
		exit(1);
	}
}

void init_32(hll_32* _hll, unsigned int expected_cardinality, unsigned int k)
{
	unsigned int i;
	if (_hll != NULL)
	{
		_hll->current_sum = 0;
		_hll->k = k;
		_hll->m = pow(2, k);
		if (_hll->m == 0)
		{
			printf("number overflow for m with a given k value of %d.\n", k);
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

void update(hll* _hll, unsigned int hash_value)
{
	unsigned int j;
	unsigned char w, lob, leftmost_bit, current;
	j = hash_value >> (32 - _hll->k);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	//w = isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	lob = lowest_order_bit_8(w);
	leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	// leftmost_bit = 1 + (unsigned char)log2(lob);
	// the current value will be at bucket
	current = _hll->bucket[j];
	_hll->current_sum -= (double)((double)1 / (double)(1 << _hll->bucket[j]));
	_hll->bucket[j] = max_uint8(current, leftmost_bit);
	_hll->current_sum += (double)((double)1 / (double)(1 << _hll->bucket[j]));
}

void update_32(hll_32* _hll, unsigned int hash_value)
{
	unsigned int j, w, lob, leftmost_bit, current;
	j = hash_value >> (32 - _hll->k);
	w = BitWizard::isolate_bits_32(0, 32 - _hll->k, hash_value);
	//w = isolate_bits_32(0, 32 - _hll->k, hash_value);
	lob = BitWizard::lowest_order_bit_index(w);
	//lob = lowest_order_bit_32(w);
	leftmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(lob);
	//leftmost_bit = 1 + (unsigned int)log2(lob);
	current = _hll->bucket[j];
	_hll->current_sum -= (double)((double)1 / (double)(1 << _hll->bucket[j]));
	_hll->bucket[j] = max_uint32(current, leftmost_bit);
	_hll->current_sum += (double)((double)1 / (double)(1 << _hll->bucket[j]));
}

unsigned int cardinality_estimation(hll* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	return (unsigned int)E;
}

unsigned int cardinality_estimation_32(hll_32* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	return (unsigned int)E;
}

void destroy(hll* _hll)
{
	_hll->k = 0;
	_hll->m = 0;
	_hll->bitmap_size = 0;
	_hll->mem_size = 0;
	free(_hll->bucket);
}