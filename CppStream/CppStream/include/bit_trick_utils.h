#pragma once
#include <iostream>
#include <cinttypes>

#ifndef BIT_TRICK_UTILS_H_
#define BIT_TRICK_UTILS_H_

#if defined(_MSC_VER)

#include <intrin.h>
#pragma intrinsic(_BitScanForward)
#pragma intrinsic(_BitScanForward64)

#define tzcnt16(x)	__mvc_ctz(x)
#define	tzcnt32(x)	__mvc_ctz(x)
#define	tzcnt64(x)	__mvc_ctz_64(x)
#define lzcnt8(x)	__lzcnt16(x) - CHAR_BIT
#define lzcnt16(x)	__lzcnt16(x)
#define lzcnt32(x)	__lzcnt(x)
#define lzcnt64(x)	__lzcnt64(x)

inline uint32_t __mvc_ctz(uint32_t value)
{
	unsigned long mask;
	_BitScanForward(&mask, value);
	return mask;
}

inline uint64_t __mvc_ctz_64(uint64_t value)
{
	unsigned long mask;
	_BitScanForward64(&mask, value);
	return mask;
}
#else	// defined(_MSC_VER)
#define tzcnt16(x)	__builtin_ctz(x)
#define tzcnt32(x)	__builtin_ctz(x)
#define	tzcnt64(x)	__builtin_ctz(x)
#define	lzcnt8(x)	__builtin_clz(x) - 24
#define lzcnt16(x)	__builtin_clz(x) - 16
#define lzcnt32(x)	__builtin_clz(x)
#define lzcnt64(x)	__builtin_clzl(x)
#endif // !defined(_MSC_VER_)

class BitWizard
{
public:
	static uint8_t max_uint8(uint8_t x, uint8_t y) { return x ^ ((x ^ y) & -(x < y)); }
	static uint16_t max_uint16(uint16_t x, uint16_t y) { return x ^ ((x ^ y) & -(x < y)); }
	static uint32_t max_uint32(uint32_t x, uint32_t y) { return x ^ ((x ^ y) & -(x < y)); }
	static uint64_t max_uint64(uint64_t x, uint64_t y) { return x ^ ((x ^ y) & -(x < y)); }

	static uint8_t min_uint8(uint8_t x, uint8_t y) { return y ^ ((x ^ y) & -(x < y)); }
	static uint16_t min_uint16(uint16_t x, uint16_t y) { return y ^ ((x ^ y) & -(x < y)); }
	static uint32_t min_uint32(uint32_t x, uint32_t y) { return y ^ ((x ^ y) & -(x < y)); }
	static uint64_t min_uint64(uint64_t x, uint64_t y) { return y ^ ((x ^ y) & -(x < y)); }

	static uint32_t log_base_2_of_power_of_2_uint(uint32_t value);

	static uint32_t swap_bits_32(uint32_t value);

	static uint16_t isolate_bits_16(size_t offset, size_t length, uint16_t value);
	static uint32_t isolate_bits_32(size_t offset, size_t length, uint32_t value);
	static uint64_t isolate_bits_64(size_t offset, size_t length, uint64_t value);

	static uint16_t lowest_order_bit_index_slow(uint16_t value);
	static uint32_t lowest_order_bit_index_slow(uint32_t value);
	static uint64_t lowest_order_bit_index_slow(uint64_t value);

	static uint8_t highest_order_bit_index_slow(uint8_t value);
	static uint16_t highest_order_bit_index_slow(uint16_t value);
	static uint32_t highest_order_bit_index_slow(uint32_t value);
	static uint64_t highest_order_bit_index_slow(uint64_t value);

	static uint8_t lowest_order_bit_index(uint8_t value);
	static uint16_t lowest_order_bit_index(uint16_t value);
	static uint32_t lowest_order_bit_index(uint32_t value);
	static uint64_t lowest_order_bit_index(uint64_t value);

	static uint8_t highest_order_bit_index(uint8_t value);
	static uint16_t highest_order_bit_index(uint16_t value);
	static uint32_t highest_order_bit_index(uint32_t value);
	static uint64_t highest_order_bit_index(uint64_t value);

	static uint16_t lowest_order_bit_index_arch(uint16_t value);
	static uint32_t lowest_order_bit_index_arch(uint32_t value);
	static uint64_t lowest_order_bit_index_arch(uint64_t value);

	static uint8_t highest_order_bit_index_arch(uint8_t value);
	static uint16_t highest_order_bit_index_arch(uint16_t value);
	static uint32_t highest_order_bit_index_arch(uint32_t value);
	static uint64_t highest_order_bit_index_arch(uint64_t value);
};
#endif // !BIT_TRICK_UTILS_H_

inline uint32_t BitWizard::log_base_2_of_power_of_2_uint(uint32_t value)
{
	const uint8_t MultiplyDeBruijnBitPosition2[32] = {
		0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
		31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9 };
	return value == 0 ? 0 : MultiplyDeBruijnBitPosition2[(uint32_t)(value * 0x077CB531U) >> 27];
}

inline uint32_t BitWizard::swap_bits_32(uint32_t v)
{
	v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
	v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
	v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
	v = ((v >> 8) & 0x00FF00FF) | ((v & 0x00FF00FF) << 8);
	v = (v >> 16) | (v << 16);
	return v;
}

inline uint16_t BitWizard::isolate_bits_16(size_t offset, size_t length, uint16_t value)
{
	return value & (((uint16_t(1) << length) - uint16_t(1)) << offset);
}

inline uint32_t BitWizard::isolate_bits_32(size_t offset, size_t length, uint32_t value)
{
	return value & (((uint32_t(1) << length) - uint32_t(1)) << offset);
}

inline uint64_t BitWizard::isolate_bits_64(size_t offset, size_t length, uint64_t value)
{
	return value & (((uint64_t(1) << length) - uint64_t(1)) << offset);
}

inline uint16_t BitWizard::lowest_order_bit_index_slow(uint16_t value)
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

inline uint8_t BitWizard::highest_order_bit_index_slow(uint8_t value)
{
	uint8_t index = 0x01;
	if (value == 0x00)
	{
		return 0x00;
	}
	while ((value & index) == 0)
	{
		index <<= 1;
	}
	return index;
}

inline uint16_t BitWizard::highest_order_bit_index_slow(uint16_t value)
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

inline uint8_t BitWizard::lowest_order_bit_index(uint8_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

inline uint16_t BitWizard::lowest_order_bit_index(uint16_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

inline uint32_t BitWizard::lowest_order_bit_index(uint32_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

inline uint64_t BitWizard::lowest_order_bit_index(uint64_t value)
{
	return value & (~value + 1); // Knuth's book rules 37 (extract right-most-bit x & -x) and 16 (-x = ~x + 1)
}

inline uint8_t BitWizard::highest_order_bit_index(uint8_t value)
{
	value |= (value >> 1);
	value |= (value >> 2);
	value |= (value >> 4);
	return value - (value >> 1);
}

inline uint16_t BitWizard::highest_order_bit_index(uint16_t value)
{
	// found from stackoverflow which references a source named Hacker's Delight
	value |= (value >> 1);
	value |= (value >> 2);
	value |= (value >> 4);
	value |= (value >> 8);
	return value - (value >> 1);
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

inline uint16_t BitWizard::lowest_order_bit_index_arch(uint16_t value)
{
	uint16_t s = 0x0001;
	uint16_t tz = tzcnt16(value);
	return value != 0 ? s << tz : 0x0000;
}

inline uint32_t BitWizard::lowest_order_bit_index_arch(uint32_t value)
{
	uint32_t s = 0x00000001;
	uint32_t tz = tzcnt32(value);
	return value != 0 ? s << tz : 0x00000000;
}

inline uint64_t BitWizard::lowest_order_bit_index_arch(uint64_t value)
{
	uint64_t s = 0x0000000000000001;
	uint64_t tz = tzcnt64(value);
	return value != 0 ? s << tz : 0x0000000000000000;
}

inline uint8_t BitWizard::highest_order_bit_index_arch(uint8_t value)
{
	uint8_t s = 0x80;
	uint8_t lz = lzcnt8(value);
	return value != 0 ? s >> lz : 0x00;
}

inline uint16_t BitWizard::highest_order_bit_index_arch(uint16_t value)
{
	uint16_t s = 0x8000;
	uint16_t lz = lzcnt16(value);
	return value != 0 ? s >> lz : 0x0000;
}

inline uint32_t BitWizard::highest_order_bit_index_arch(uint32_t value)
{
	uint32_t s = 0x80000000;
	uint32_t lz = lzcnt32(value);
	return value != 0 ? s >> lz : 0x00000000;
}

inline uint64_t BitWizard::highest_order_bit_index_arch(uint64_t value)
{
	uint64_t s = 0x8000000000000000;
	uint64_t lz = lzcnt64(value);
	return value != 0 ? s >> lz : 0x0000000000000000;
}