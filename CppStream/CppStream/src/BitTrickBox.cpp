#pragma once
#include "../include/BitTrickBox.h"

const uint8_t BitWizard::MultiplyDeBruijnBitPosition2[32] =
{
	0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
	31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
};

uint32_t BitWizard::return_max_uint32(uint32_t x, uint32_t y)
{
	return x ^ ((x ^ y) & -(x < y)); // max(x, y)
}

uint32_t BitWizard::log_base_2_of_power_of_2_uint(uint32_t value)
{
	return BitWizard::MultiplyDeBruijnBitPosition2[(uint32_t)(value * 0x077CB531U) >> 27];
}

uint32_t BitWizard::swap_bits_32(uint32_t v)
{
	v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
	v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
	v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
	v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
	v = (v >> 16) | (v << 16);
	return v;
}

uint16_t BitWizard::isolate_bits_16(size_t offset, size_t length, uint16_t value)
{
	return value & (((uint16_t(1) << length) - uint16_t(1)) << offset);
}

uint32_t BitWizard::isolate_bits_32(size_t offset, size_t length, uint32_t value)
{
	return value & (((uint32_t(1) << length) - uint32_t(1)) << offset);
}

uint64_t BitWizard::isolate_bits_64(size_t offset, size_t length, uint64_t value)
{
	return value & (((uint64_t(1) << length) - uint64_t(1)) << offset);
}

uint16_t BitWizard::lowest_order_bit_index_slow(uint16_t value)
{
	uint16_t index = 0x0001;
	if (value == uint16_t(0))
	{
		return uint16_t(0);
	}
	while ((value & index) == 0)
	{
		index = index << 1;
	}
	return index;
}

uint32_t BitWizard::lowest_order_bit_index_slow(uint32_t value)
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

uint64_t BitWizard::lowest_order_bit_index_slow(uint64_t value)
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

uint16_t BitWizard::highest_order_bit_index_slow(uint16_t value)
{
	uint16_t index = 0x8000;
	if (value == uint16_t(0))
	{
		return uint16_t(0);
	}
	while ((value & index) == 0 && index != 0)
	{
		index = index >> 1;
	}
	return index;
}

uint32_t BitWizard::highest_order_bit_index_slow(uint32_t value)
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

uint64_t BitWizard::highest_order_bit_index_slow(uint64_t value)
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

uint16_t BitWizard::lowest_order_bit_index(uint16_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

uint32_t BitWizard::lowest_order_bit_index(uint32_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

uint64_t BitWizard::lowest_order_bit_index(uint64_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

uint16_t BitWizard::highest_order_bit_index(uint16_t value)
{
	// found from stackoverflow which references a source named Hacker's Delight
	value |= (value >> 1);
	value |= (value >> 2);
	value |= (value >> 4);
	value |= (value >> 8);
	return value - (value >> 1);
}

uint32_t BitWizard::highest_order_bit_index(uint32_t value)
{
	// found from stackoverflow which references a source named Hacker's Delight
	value |= (value >> 1);
	value |= (value >> 2);
	value |= (value >> 4);
	value |= (value >> 8);
	value |= (value >> 16);
	return value - (value >> 1);
}

uint64_t BitWizard::highest_order_bit_index(uint64_t value)
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

uint16_t BitWizard::lowest_order_bit_index_arch(uint16_t value)
{
	/*unsigned long mask;
	return _BitScanForward(&mask, value) != 0 ? uint16_t(1) << mask : uint16_t(0);*/
	return tzcnt16(value);
}

uint32_t BitWizard::lowest_order_bit_index_arch(uint32_t value)
{
	/*unsigned long mask;
	return _BitScanForward(&mask, value) != 0 ? uint32_t(1) << mask : uint32_t(0);*/
	return tzcnt32(value);
}

uint64_t BitWizard::lowest_order_bit_index_arch(uint64_t value)
{
	/*unsigned long mask;
	return _BitScanForward64(&mask, value) != 0 ? uint64_t(1) << mask : uint64_t(0);*/
	return tzcnt64(value);
}

uint16_t BitWizard::highest_order_bit_index_arch(uint16_t value)
{
	return value != 0 ? 0x8000 >> lzcnt16(value) : uint16_t(0);
}

uint32_t BitWizard::highest_order_bit_index_arch(uint32_t value)
{
	return value != 0 ? 0x80000000 >> lzcnt32(value) : uint32_t(0);
}

uint64_t BitWizard::highest_order_bit_index_arch(uint64_t value)
{
	return value != 0 ? 0x8000000000000000 >> lzcnt64(value) : uint64_t(0);
}