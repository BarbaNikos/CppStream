#ifndef C_HLL_H_
#include "../include/c_hll.h"
#endif // C_HLL_H_

int Hll::init_8(Hll::hll_8* _hll, unsigned int p, size_t hash_code_len_in_bytes)
{
	size_t i;
	if (p > (hash_code_len_in_bytes * CHAR_BIT))
	{
		printf("init_32 error: p value given cannot exceed length of hash_code.\n");
		return -1;
	}
	if (_hll != nullptr)
	{
		_hll->current_sum = 0;
		_hll->p = p;
		_hll->m = static_cast<unsigned int>(pow(2, p));
		if (_hll->m == 0)
		{
			printf("number overflow for m with a given k value of %d.\n", p);
			exit(1);
		}
		_hll->bitmap_size = CHAR_BIT;
		_hll->mem_size_in_bits = static_cast<unsigned int>(_hll->bitmap_size) * _hll->m;
		_hll->bucket = static_cast<char*>(calloc(_hll->m, sizeof(char)));
		for (i = 0; i < _hll->m; ++i)
		{
			_hll->bucket[i] = 0x00;
			_hll->current_sum += (1 / (pow(2, 0)));
		}
		_hll->_multiplier = a_16 * _hll->m * _hll->m;
		_hll->hash_code_len_in_bits = hash_code_len_in_bytes * CHAR_BIT;
		_hll->bitmap_offset = _hll->hash_code_len_in_bits - _hll->p - _hll->bitmap_size;
		_hll->v = _hll->m;
		_hll->v_size = static_cast<size_t>(ceil(_hll->m / 64));
		_hll->v_bitmap = static_cast<uint64_t*>(calloc(_hll->v_size, sizeof(uint64_t)));
		_hll->condition_value_test_1 = (5 / 2) * _hll->m;
		_hll->pow_2_to_32 = pow(2, 32);
		_hll->condition_value_test_2 = (1 / 30) * _hll->pow_2_to_32;
		_hll->m_times_log_m = _hll->m * log(_hll->m);
		return 0;
	}
	else
	{
		printf("empty hll structure provided.\n");
		return -1;
	}
}

void Hll::copy_8(Hll::hll_8 * _new_hll, const Hll::hll_8* _src_hll)
{
	_new_hll->current_sum = _src_hll->current_sum;
	_new_hll->p = _src_hll->p;
	_new_hll->m = _src_hll->m;
	_new_hll->bitmap_size = _src_hll->bitmap_size;
	_new_hll->bitmap_offset = _src_hll->bitmap_offset;
	_new_hll->mem_size_in_bits = _src_hll->mem_size_in_bits;
	_new_hll->_multiplier = _src_hll->_multiplier;
	free(_new_hll->bucket);
	_new_hll->bucket = static_cast<char*>(calloc(_new_hll->m, sizeof(char)));
	memcpy(_new_hll->bucket, _src_hll->bucket, _new_hll->m * sizeof(char));
	_new_hll->condition_value_test_1 = _src_hll->condition_value_test_1;
	_new_hll->condition_value_test_2 = _src_hll->condition_value_test_2;
	_new_hll->pow_2_to_32 = _src_hll->pow_2_to_32;
	_new_hll->m_times_log_m = _src_hll->m_times_log_m;
	_new_hll->v_size = _src_hll->v_size;
	free(_new_hll->v_bitmap);
	_new_hll->v_bitmap = static_cast<uint64_t*>(calloc(_new_hll->v_size, sizeof(uint64_t)));
	memcpy(_new_hll->v_bitmap, _src_hll->v_bitmap, _new_hll->v_size * sizeof(uint64_t));
	_new_hll->v = _src_hll->v;
	_new_hll->hash_code_len_in_bits = _src_hll->hash_code_len_in_bits;
}

int Hll::init_32(Hll::hll_32* _hll, unsigned int p, size_t hash_code_len_in_bytes)
{
	size_t i;
	if (p > (hash_code_len_in_bytes * CHAR_BIT))
	{
		printf("init_32 error: p value given cannot exceed length of hash_code.\n");
		return -1;
	}
	if (_hll != nullptr)
	{
		_hll->current_sum = 0;
		_hll->p = p;
		_hll->m = static_cast<unsigned int>(pow(2, p));
		if (_hll->m == 0)
		{
			printf("number overflow for m with a given k value of %d.\n", p);
			exit(1);
		}
		_hll->bitmap_size = 32;
		_hll->mem_size_in_bits = static_cast<unsigned int>(_hll->bitmap_size) * _hll->m;
		_hll->bucket = static_cast<unsigned int*>(calloc(_hll->m, sizeof(unsigned int)));
		for (i = 0; i < _hll->m; ++i)
		{
			_hll->bucket[i] = static_cast<unsigned int>(0x00000000);
			_hll->current_sum += (1 / (pow(2, 0)));
		}
		_hll->_multiplier = a_32 * _hll->m * _hll->m;
		_hll->hash_code_len_in_bits = hash_code_len_in_bytes * CHAR_BIT;
		_hll->bitmap_offset = (_hll->hash_code_len_in_bits - _hll->p - _hll->bitmap_size) < 0 ? 0 : 
			(_hll->hash_code_len_in_bits - _hll->p - _hll->bitmap_size);
		_hll->v = _hll->m;
		_hll->v_size = static_cast<size_t>(ceil(_hll->m / 64));
		_hll->v_bitmap = static_cast<uint64_t*>(calloc(_hll->v_size, sizeof(uint64_t)));
		_hll->condition_value_test_1 = (5 / 2) * _hll->m;
		_hll->pow_2_to_32 = pow(2, 32);
		_hll->condition_value_test_2 = (1 / 30) * _hll->pow_2_to_32;
		_hll->m_times_log_m = _hll->m * log(_hll->m);
		return 0;
	}
	else
	{
		printf("empty hll structure provided.\n");
		return -1;
	}
}

void Hll::copy_32(Hll::hll_32 * _new_hll, const Hll::hll_32* _src_hll)
{
	_new_hll->current_sum = _src_hll->current_sum;
	_new_hll->p = _src_hll->p;
	_new_hll->m = _src_hll->m;
	_new_hll->bitmap_size = _src_hll->bitmap_size;
	_new_hll->bitmap_offset = _src_hll->bitmap_offset;
	_new_hll->mem_size_in_bits = _src_hll->mem_size_in_bits;
	_new_hll->_multiplier = _src_hll->_multiplier;
	free(_new_hll->bucket);
	_new_hll->bucket = static_cast<unsigned int*>(calloc(_new_hll->m, sizeof(unsigned int)));
	memcpy(_new_hll->bucket, _src_hll->bucket, _new_hll->m * sizeof(unsigned int));
	_new_hll->condition_value_test_1 = _src_hll->condition_value_test_1;
	_new_hll->condition_value_test_2 = _src_hll->condition_value_test_2;
	_new_hll->pow_2_to_32 = _src_hll->pow_2_to_32;
	_new_hll->m_times_log_m = _src_hll->m_times_log_m;
	_new_hll->v_size = _src_hll->v_size;
	free(_new_hll->v_bitmap);
	_new_hll->v_bitmap = static_cast<uint64_t*>(calloc(_new_hll->v_size, sizeof(uint64_t)));
	memcpy(_new_hll->v_bitmap, _src_hll->v_bitmap, _new_hll->v_size * sizeof(uint64_t));
	_new_hll->v = _src_hll->v;
	_new_hll->hash_code_len_in_bits = _src_hll->hash_code_len_in_bits;
}

void Hll::update_8(Hll::hll_8* _hll, uint32_t hash_value)
{
	uint32_t j;
	uint8_t w, current, rob, righmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	righmost_bit = 1 + static_cast<uint8_t>(log2(rob));
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint8(current, righmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
}

void Hll::update_8(Hll::hll_8* _hll, uint64_t hash_value)
{
	uint64_t j;
	uint8_t w, current, rob, righmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_64(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	righmost_bit = 1 + static_cast<uint8_t>(log2(rob));
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint8(current, righmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
}

void Hll::opt_update_8(Hll::hll_8* _hll, uint32_t hash_value)
{
	uint32_t j;
	uint8_t w, current, rob, righmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	righmost_bit = 1 + static_cast<uint8_t>(log2(rob));
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint8(current, righmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	if (_hll->bucket[j] != 0)
	{
		uint64_t or_operand = uint64_t(1 << (j % 64));
		uint32_t byte_offset = j / 64;
		uint64_t prev_count = popcnt64(_hll->v_bitmap[byte_offset]);
		uint64_t new_bitmap_value = _hll->v_bitmap[byte_offset] | or_operand;
		uint64_t current_count = popcnt64(new_bitmap_value);
		_hll->v_bitmap[byte_offset] = new_bitmap_value;
		_hll->v -= (current_count - prev_count);
	}
}

void Hll::opt_update_8(Hll::hll_8* _hll, uint64_t hash_value)
{
	uint64_t j;
	uint8_t w, current, rob, righmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits -_hll->p);
	w = BitWizard::isolate_bits_64(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	righmost_bit = 1 + static_cast<uint32_t>(log2(rob));
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint8(current, righmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	if (_hll->bucket[j] != 0)
	{
		uint64_t or_operand = 1 << (j % 64);
		uint32_t byte_offset = j / 64;
		uint32_t prev_count = popcnt64(_hll->v_bitmap[byte_offset]);
		uint64_t new_bitmap_value = _hll->v_bitmap[byte_offset] | or_operand;
		uint32_t current_count = popcnt64(new_bitmap_value);
		_hll->v_bitmap[byte_offset] = new_bitmap_value;
		_hll->v -= (current_count - prev_count);
	}
}

void Hll::update_32(Hll::hll_32* _hll, uint32_t hash_value)
{
	uint32_t j, w, current, rob, rightmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value);
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint32(current, rightmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
}

void Hll::update_32(Hll::hll_32* _hll, uint64_t hash_value)
{
	uint32_t j, w, current, rob, rightmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits -_hll->p);
	w = BitWizard::isolate_bits_64(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint32(current, rightmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
}

void Hll::opt_update_32(Hll::hll_32* _hll, uint32_t hash_value)
{
	uint32_t j, w, current, rob, rightmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value);
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint32(current, rightmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	if (_hll->bucket[j] != 0)
	{
		uint64_t or_operand = 1 << (j % 64);
		uint32_t byte_offset = j / 64;
		uint32_t prev_count = popcnt64(_hll->v_bitmap[byte_offset]);
		uint64_t new_bitmap_value = _hll->v_bitmap[byte_offset] | or_operand;
		uint32_t current_count = popcnt64(new_bitmap_value);
		_hll->v_bitmap[byte_offset] = new_bitmap_value;
		_hll->v -= (current_count - prev_count);
	}
}

void Hll::opt_update_32(Hll::hll_32* _hll, uint64_t hash_value)
{
	uint32_t j, w, current, rob, rightmost_bit;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_64(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + BitWizard::log_base_2_of_power_of_2_uint(rob);
	current = _hll->bucket[j];
	_hll->current_sum -= static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	_hll->bucket[j] = BitWizard::max_uint32(current, rightmost_bit);
	_hll->current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	if (_hll->bucket[j] != 0)
	{
		uint64_t or_operand = 1 << (j % 64);
		uint32_t byte_offset = j / 64;
		uint32_t prev_count = popcnt64(_hll->v_bitmap[byte_offset]);
		uint64_t new_bitmap_value = _hll->v_bitmap[byte_offset] | or_operand;
		uint32_t current_count = popcnt64(new_bitmap_value);
		_hll->v_bitmap[byte_offset] = new_bitmap_value;
		_hll->v -= (current_count - prev_count);
	}
}

uint64_t Hll::cardinality_estimation_8(Hll::hll_8* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	return static_cast<unsigned int>(E);
}

uint64_t Hll::opt_cardinality_estimation_8(Hll::hll_8* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	if (E <= _hll->condition_value_test_1)
	{
		switch (_hll->v)
		{
		case 0:
			return E;
		default:
			return _hll->m_times_log_m - _hll->m * log(_hll->v);
		}
	}
	else if (E <= _hll->condition_value_test_2)
	{
		return E;
	}
	else
	{
		return -_hll->pow_2_to_32 * log(1 - (E / _hll->pow_2_to_32));
	}
}

uint64_t Hll::new_cardinality_estimate_8(Hll::hll_8* _hll, uint32_t hash_value)
{
	uint32_t j;
	uint8_t w, current, rob, righmost_bit, new_value;
	double current_sum, E;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	righmost_bit = 1 + log2(rob);
	current = _hll->bucket[j];
	current_sum = _hll->current_sum - static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	new_value = BitWizard::max_uint8(current, righmost_bit);
	current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << new_value));
	E = _hll->_multiplier / current_sum;
	return static_cast<unsigned int>(E);
}

uint64_t Hll::new_cardinality_estimate_8(Hll::hll_8* _hll, uint64_t hash_value)
{
	uint32_t j;
	uint8_t w, current, new_value, rob, rightmost_bit;
	double current_sum, E;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_64(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + log2(rob);
	current = _hll->bucket[j];
	current_sum = _hll->current_sum - static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	new_value = BitWizard::max_uint8(current, rightmost_bit);
	current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << new_value));
	E = _hll->_multiplier / current_sum;
	return static_cast<unsigned int>(E);
}

uint64_t Hll::opt_new_cardinality_estimate_8(Hll::hll_8* _hll, uint32_t hash_value)
{
	uint32_t j, new_v = _hll->v;
	uint8_t w, current, new_value, rob, rightmost_bit;
	double current_sum, E;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_32(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + log2(rob);
	current = _hll->bucket[j];
	current_sum = _hll->current_sum - static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	new_value = BitWizard::max_uint8(current, rightmost_bit);
	current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << new_value));
	if (new_value != 0)
	{
		uint64_t or_operand = 1 << (j % 64);
		uint32_t byte_offset = j / 64;
		uint32_t prev_count = popcnt64(_hll->v_bitmap[byte_offset]);
		uint64_t new_bitmap_value = _hll->v_bitmap[byte_offset] | or_operand;
		uint32_t current_count = popcnt64(new_bitmap_value);
		new_v = _hll->v - (current_count - prev_count);
	}
	E = _hll->_multiplier / current_sum;
	if (E <= _hll->condition_value_test_1)
	{
		switch (new_v)
		{
		case 0:
			return E;
		default:
			return _hll->m_times_log_m - _hll->m * log(new_v);
		}
	}
	else if (E <= _hll->condition_value_test_2)
	{
		return E;
	}
	else
	{
		return -_hll->pow_2_to_32 * log(1 - (E / _hll->pow_2_to_32));
	}
}

uint64_t Hll::opt_new_cardinality_estimate_8(Hll::hll_8* _hll, uint64_t hash_value)
{
	uint32_t j, new_v = _hll->v;
	uint8_t w, current, new_value, rob, rightmost_bit;
	double current_sum, E;
	j = hash_value >> (_hll->hash_code_len_in_bits - _hll->p);
	w = BitWizard::isolate_bits_64(_hll->bitmap_offset, _hll->bitmap_size, hash_value) >> _hll->bitmap_offset;
	rob = BitWizard::highest_order_bit_index_arch(w);
	rightmost_bit = 1 + log2(rob);
	current = _hll->bucket[j];
	current_sum = _hll->current_sum - static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << _hll->bucket[j]));
	new_value = BitWizard::max_uint8(current, rightmost_bit);
	current_sum += static_cast<double>(static_cast<double>(1) / static_cast<double>(1 << new_value));
	if (new_value != 0)
	{
		uint64_t or_operand = 1 << (j % 64);
		uint32_t byte_offset = j / 64;
		uint32_t prev_count = popcnt64(_hll->v_bitmap[byte_offset]);
		uint64_t new_bitmap_value = _hll->v_bitmap[byte_offset] | or_operand;
		uint32_t current_count = popcnt64(new_bitmap_value);
		new_v = _hll->v - (current_count - prev_count);
	}
	E = _hll->_multiplier / current_sum;
	if (E <= _hll->condition_value_test_1)
	{
		switch (new_v)
		{
		case 0:
			return E;
		default:
			return _hll->m_times_log_m - _hll->m * log(new_v);
		}
	}
	else if (E <= _hll->condition_value_test_2)
	{
		return E;
	}
	else
	{
		return -_hll->pow_2_to_32 * log(1 - (E / _hll->pow_2_to_32));
	}
}

uint64_t Hll::cardinality_estimation_32(Hll::hll_32* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	return static_cast<unsigned int>(E);
}

uint64_t Hll::opt_cardinality_estimation_32(Hll::hll_32* _hll)
{
	double E = _hll->_multiplier / _hll->current_sum;
	if (E <= _hll->condition_value_test_1)
	{
		switch (_hll->v)
		{
		case 0:
			return E;
		default:
			return _hll->m_times_log_m - _hll->m * log(_hll->v);
		}
	}
	else if (E <= _hll->condition_value_test_2)
	{
		return E;
	}
	else
	{
		return -_hll->pow_2_to_32 * log(1 - (E / _hll->pow_2_to_32));
	}
}

void Hll::destroy_8(Hll::hll_8* _hll)
{
	_hll->p = 0;
	_hll->m = 0;
	_hll->bitmap_size = 0;
	_hll->mem_size_in_bits = 0;
	free(_hll->bucket);
	memset(_hll->v_bitmap, 0x0, sizeof(uint64_t) * _hll->v_size);
	free(_hll->v_bitmap);
}

void Hll::destroy_32(Hll::hll_32* _hll)
{
	_hll->p = 0;
	_hll->m = 0;
	_hll->bitmap_size = 0;
	_hll->mem_size_in_bits = 0;
	free(_hll->bucket);
}
