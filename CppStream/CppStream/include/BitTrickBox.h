#pragma once
#include <iostream>
#include <cinttypes>

#ifndef BIT_TRICK_BOX
#define BIT_TRICK_BOX

#if defined(_MSC_VER)

#include <intrin.h>
#pragma intrinsic(_BitScanForward)
#pragma intrinsic(_BitScanForward64)

#define tzcnt16(x)	__mvc_ctz(x)
#define	tzcnt32(x)	__mvc_ctz(x)
#define	tzcnt64(x)	__mvc_ctz_64(x)
#define lzcnt16(x) __lzcnt16(x)
#define lzcnt32(x) __lzcnt(x)
#define lzcnt64(x) __lzcnt64(x)

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
#define lzcnt16(x)	__builtin_clz(x)
#define lzcnt32(x)	__builtin_clz(x)
#define lzcnt64(x)	__builtin_clzl(x)
#endif // !defined(_MSC_VER_)

class BitWizard
{
public:
	static const uint8_t MultiplyDeBruijnBitPosition2[32];

	static uint32_t return_max_uint32(uint32_t x, uint32_t y);

	static uint32_t log_base_2_of_power_of_2_uint(uint32_t value);

	static uint32_t swap_bits_32(uint32_t value);

	static uint16_t isolate_bits_16(size_t offset, size_t length, uint16_t value);
	static uint32_t isolate_bits_32(size_t offset, size_t length, uint32_t value);
	static uint64_t isolate_bits_64(size_t offset, size_t length, uint64_t value);

	static uint16_t lowest_order_bit_index_slow(uint16_t value);
	static uint32_t lowest_order_bit_index_slow(uint32_t value);
	static uint64_t lowest_order_bit_index_slow(uint64_t value);
	
	static uint16_t highest_order_bit_index_slow(uint16_t value);
	static uint32_t highest_order_bit_index_slow(uint32_t value);
	static uint64_t highest_order_bit_index_slow(uint64_t value);
	
	static uint16_t lowest_order_bit_index(uint16_t value);
	static uint32_t lowest_order_bit_index(uint32_t value);
	static uint64_t lowest_order_bit_index(uint64_t value);
	
	static uint16_t highest_order_bit_index(uint16_t value);
	static uint32_t highest_order_bit_index(uint32_t value);
	static uint64_t highest_order_bit_index(uint64_t value);

	static uint16_t lowest_order_bit_index_arch(uint16_t value);
	static uint32_t lowest_order_bit_index_arch(uint32_t value);
	static uint64_t lowest_order_bit_index_arch(uint64_t value);

	static uint16_t highest_order_bit_index_arch(uint16_t value);
	static uint32_t highest_order_bit_index_arch(uint32_t value);
	static uint64_t highest_order_bit_index_arch(uint64_t value);
};
#endif // !BIT_TRICK_BOX
