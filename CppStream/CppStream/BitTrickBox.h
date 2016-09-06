#pragma once

#include <iostream>

#ifndef BIT_TRICK_BOX
#define BIT_TRICK_BOX

#include <intrin.h>

#pragma intrinsic(_BitScanForward);

class BitWizard
{
public:
	static uint32_t swap_bits_32(uint32_t value);

	static uint32_t lowest_order_bit_index_slow(uint32_t value);
	static uint64_t lowest_order_bit_index_slow(uint64_t value);
	
	static uint32_t highest_order_bit_index_slow(uint32_t value);
	static uint64_t highest_order_bit_index_slow(uint64_t value);
	
	static uint32_t lowest_order_bit_index(uint32_t value);
	static uint64_t lowest_order_bit_index(uint64_t value);
	
	static uint32_t highest_order_bit_index(uint32_t value);
	static uint64_t highest_order_bit_index(uint64_t value);

	static uint32_t lowest_order_bit_index_arch(uint32_t value);
	static uint64_t lowest_order_bit_index_arch(uint64_t value);

	static uint32_t highest_order_bit_index_arch(uint32_t value);
	static uint64_t highest_order_bit_index_arch(uint64_t value);
};

inline uint32_t BitWizard::swap_bits_32(uint32_t v)
{
	v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
	v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
	v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
	v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
	v = (v >> 16) | (v << 16);
	return v;
}

inline uint32_t BitWizard::lowest_order_bit_index_slow(uint32_t value)
{
	uint32_t index = 0x00000001;
	if (value == uint32_t(0))
	{
		return uint32_t(0);
	}
	while ((value & index) == 0)
	{
		index = index << 1;
	}
	return index;
}

inline uint64_t BitWizard::lowest_order_bit_index_slow(uint64_t value)
{
	uint64_t index = uint64_t(1);
	if (value == uint64_t(0))
	{
		return uint64_t(0);
	}
	while ((value & index) == 0)
	{
		index = index << 1;
	}
	return index;
}

inline uint32_t BitWizard::highest_order_bit_index_slow(uint32_t value)
{
	uint32_t index = 0x80000000;
	if (value == uint32_t(0))
	{
		return uint32_t(0);
	}
	while ((value & index) == 0 && index != 0)
	{
		index = index >> 1;
	}
	return index;
}

inline uint64_t BitWizard::highest_order_bit_index_slow(uint64_t value)
{
	uint64_t index = 0x8000000000000000;
	if (value == uint64_t(0))
	{
		return uint64_t(0);
	}
	while ((value & index) == 0 && index != 0)
	{
		index = index >> 1;
	}
	return index;
}

inline uint32_t BitWizard::lowest_order_bit_index(uint32_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

inline uint64_t BitWizard::lowest_order_bit_index(uint64_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

inline uint32_t BitWizard::highest_order_bit_index(uint32_t value)
{
	// found from stackoverflow which references a source named Hacker's Delight
	value |= (value >> 1);
	value |= (value >> 2);
	value |= (value >> 4);
	value |= (value >> 8);
	value |= (value >> 16);
	return value - (value >> 1);
}

inline uint64_t BitWizard::highest_order_bit_index(uint64_t value)
{
	// have not tested it properly. Need to verify its correctness
	value |= (value >> 1);
	value |= (value >> 2);
	value |= (value >> 4);
	value |= (value >> 8);
	value |= (value >> 16);
	value |= (value >> 32);
	return value - (value >> 1);
}

inline uint32_t BitWizard::lowest_order_bit_index_arch(uint32_t value)
{
	unsigned long mask;
	_BitScanForward(&mask, uint64_t(value));
	return mask;
}

inline uint64_t BitWizard::lowest_order_bit_index_arch(uint64_t value)
{
	unsigned long mask;
	_BitScanForward(&mask, value);
	return mask;
}

inline uint32_t BitWizard::highest_order_bit_index_arch(uint32_t value)
{
	switch (value)
	{
	case uint32_t(0):
		return uint32_t(0);
	default:
		return 0x80000000 >> __lzcnt(value);
	}
}

inline uint64_t BitWizard::highest_order_bit_index_arch(uint64_t value)
{
	switch (value)
	{
	case uint64_t(0):
		return uint64_t(0);
	default:
		return 0x8000000000000000 >> __lzcnt64(value);
	}
}

#endif // !BIT_TRICK_BOX
