#pragma once
#include <cinttypes>
#include <chrono>
#include "BitTrickBox.h"

void bit_tricks_scenario()
{
	uint32_t i = 8; // 0x00000008
	uint32_t l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000008
	uint32_t f_l = BitWizard::lowest_order_bit_index(i);
	uint32_t h = BitWizard::highest_order_bit_index_slow(i); // 0x00000008
	uint32_t f_h = BitWizard::highest_order_bit_index(i);
	uint64_t h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	uint64_t f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));

	//uint64_t f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//uint64_t f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));

	if (l != f_l)
	{
		std::cout << "first one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "first one failed on highest\n";
	}
	if (h_64 != f_h_64)
	{
		std::cout << "first one failed on highest (x64)\n";
	}
	i = 15; // 0x0000000f
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x00000008
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h = BitWizard::highest_order_bit_index(i);
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "second one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "second one failed on highest\n";
	}
	i = 123452; // 0x0001e23c
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000004
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x00010000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "third one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "third one failed on highest\n";
	}
	i = uint32_t(0xffffffff);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "fourth one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "fourth one failed on highest\n";
	}
	i = uint32_t(0x80000000);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x800000000
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "fifth one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "fifth one failed on highest\n";
	}
	i = uint32_t(0x80000001);
	l = BitWizard::lowest_order_bit_index_slow(i); // 0x00000001
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i); // 0x80000000
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
	if (l != f_l)
	{
		std::cout << "sixth one failed on lowest\n";
	}
	if (h != f_h)
	{
		std::cout << "sixth one failed on highest\n";
		return;
	}
	i = uint32_t(0x00000000);
	l = BitWizard::lowest_order_bit_index_slow(i);
	f_l = BitWizard::lowest_order_bit_index(i);
	h = BitWizard::highest_order_bit_index_slow(i);
	f_h = BitWizard::highest_order_bit_index(i);
	h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
	f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
	//f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
	//f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));
}

void bit_tricks_performance_16()
{
	const unsigned long upper_limit = ULONG_MAX / 32;
	size_t sum = 0, f_sum = 0, f_arch_sum = 0;
	std::chrono::system_clock::time_point normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t l_16 = BitWizard::lowest_order_bit_index_slow(uint16_t(i));
		sum += l_16;
	}
	std::chrono::system_clock::time_point normal_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16 = BitWizard::lowest_order_bit_index(uint16_t(i));
		f_sum += f_l_16;
	}
	std::chrono::system_clock::time_point fast_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16_arch = BitWizard::lowest_order_bit_index_arch(uint16_t(i));
		f_arch_sum += f_l_16_arch;
	}
	std::chrono::system_clock::time_point fast_arch_end = std::chrono::system_clock::now();

	std::chrono::duration<double, std::milli> normal_duration = normal_end - normal_start;
	std::chrono::duration<double, std::milli> fast_duration = fast_end - fast_start;
	std::chrono::duration<double, std::milli> fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-16-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16_arch = BitWizard::lowest_order_bit_index_arch(uint16_t(i));
		f_arch_sum += f_l_16_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-16-bit without branch for fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	// high-order
	sum = 0; f_sum = 0; f_arch_sum = 0;
	normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t l_16 = BitWizard::highest_order_bit_index_slow(uint16_t(i));
		sum += l_16;
	}
	normal_end = std::chrono::system_clock::now();
	fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16 = BitWizard::highest_order_bit_index(uint16_t(i));
		f_sum += f_l_16;
	}
	fast_end = std::chrono::system_clock::now();
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint16_t f_l_16_arch = BitWizard::highest_order_bit_index_arch(uint16_t(i));
		f_arch_sum += f_l_16_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	normal_duration = normal_end - normal_start;
	fast_duration = fast_end - fast_start;
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-16-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
}

void bit_tricks_performance_32()
{
	const unsigned long upper_limit = ULONG_MAX / 32;
	size_t sum = 0, f_sum = 0, f_arch_sum = 0;
	std::chrono::system_clock::time_point normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t l_32 = BitWizard::lowest_order_bit_index_slow(uint32_t(i));
		sum += l_32;
	}
	std::chrono::system_clock::time_point normal_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32 = BitWizard::lowest_order_bit_index(uint32_t(i));
		f_sum += f_l_32;
	}
	std::chrono::system_clock::time_point fast_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32_arch = BitWizard::lowest_order_bit_index_arch(uint32_t(i));
		f_arch_sum += f_l_32_arch;
	}
	std::chrono::system_clock::time_point fast_arch_end = std::chrono::system_clock::now();

	std::chrono::duration<double, std::milli> normal_duration = normal_end - normal_start;
	std::chrono::duration<double, std::milli> fast_duration = fast_end - fast_start;
	std::chrono::duration<double, std::milli> fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-32-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	// high-order
	sum = 0; f_sum = 0; f_arch_sum = 0;
	normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t l_32 = BitWizard::highest_order_bit_index_slow(uint32_t(i));
		sum += l_32;
	}
	normal_end = std::chrono::system_clock::now();
	fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32 = BitWizard::highest_order_bit_index(uint32_t(i));
		f_sum += f_l_32;
	}
	fast_end = std::chrono::system_clock::now();
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint32_t f_l_32_arch = BitWizard::highest_order_bit_index_arch(uint32_t(i));
		f_arch_sum += f_l_32_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	normal_duration = normal_end - normal_start;
	fast_duration = fast_end - fast_start;
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-32-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
}

void bit_tricks_performance_64()
{
	const unsigned long upper_limit = ULONG_MAX / 32;
	size_t sum = 0, f_sum = 0, f_arch_sum = 0;
	std::chrono::system_clock::time_point normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t l_64 = BitWizard::lowest_order_bit_index_slow(uint64_t(i));
		sum += l_64;
	}
	std::chrono::system_clock::time_point normal_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64 = BitWizard::lowest_order_bit_index(uint64_t(i));
		f_sum += f_l_64;
	}
	std::chrono::system_clock::time_point fast_end = std::chrono::system_clock::now();
	std::chrono::system_clock::time_point fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64_arch = BitWizard::lowest_order_bit_index_arch(uint64_t(i));
		f_arch_sum += f_l_64_arch;
	}
	std::chrono::system_clock::time_point fast_arch_end = std::chrono::system_clock::now();

	std::chrono::duration<double, std::milli> normal_duration = normal_end - normal_start;
	std::chrono::duration<double, std::milli> fast_duration = fast_end - fast_start;
	std::chrono::duration<double, std::milli> fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "low-order-64-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	// high-order
	sum = 0; f_sum = 0; f_arch_sum = 0;
	normal_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t l_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
		sum += l_64;
	}
	normal_end = std::chrono::system_clock::now();
	fast_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64 = BitWizard::highest_order_bit_index(uint64_t(i));
		f_sum += f_l_64;
	}
	fast_end = std::chrono::system_clock::now();
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		uint64_t f_l_64_arch = BitWizard::highest_order_bit_index_arch(uint64_t(i));
		f_arch_sum += f_l_64_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	normal_duration = normal_end - normal_start;
	fast_duration = fast_end - fast_start;
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-64-bit :: normal: " << normal_duration.count() << ", fast: " <<
		fast_duration.count() << ", fast-arch: " << fast_arch_duration.count() << " (msec).\n";
	fast_arch_start = std::chrono::system_clock::now();
	for (unsigned long i = 0; i < upper_limit; ++i)
	{
		//uint64_t f_l_64_arch = 0x8000000000000000 >> __lzcnt64(i);
		uint64_t f_l_64_arch = BitWizard::highest_order_bit_index_arch(uint64_t(i));
		f_arch_sum += f_l_64_arch;
	}
	fast_arch_end = std::chrono::system_clock::now();
	fast_arch_duration = fast_arch_end - fast_arch_start;
	std::cout << "high-order-64-bit without branch: " << fast_arch_duration.count() << " (msec).\n";
}

int bit_tricks_correctness_test()
{
	for (unsigned long i = 0; i < ULONG_MAX; ++i)
	{
		uint16_t l_16 = BitWizard::lowest_order_bit_index_slow(uint16_t(i));
		uint16_t f_l_16 = BitWizard::lowest_order_bit_index(uint16_t(i));
		uint16_t f_l_arch_16 = BitWizard::lowest_order_bit_index_arch(uint16_t(i));

		uint16_t h_16 = BitWizard::highest_order_bit_index_slow(uint16_t(i));
		uint16_t f_h_16 = BitWizard::highest_order_bit_index(uint16_t(i));
		uint16_t f_h_arch_16 = BitWizard::highest_order_bit_index_arch(uint16_t(i));

		if (l_16 != f_l_16 || l_16 != f_l_arch_16)
		{
			std::cout << "for input number: " << uint16_t(i) << ", l_16: " << l_16 << ", f_l_16: " << f_l_16 << ", f_l_arch_16: " << f_l_arch_16 << "\n";
			return 1;
		}
		if (h_16 != f_h_16 || h_16 != f_h_arch_16)
		{
			std::cout << "for input number: " << uint16_t(i) << ", h_16: " << h_16 << ", f_h_16: " << f_h_16 << ", f_h_arch_16: " << f_h_arch_16 << "\n";
			return 1;
		}
		uint32_t l_32 = BitWizard::lowest_order_bit_index_slow(uint32_t(i));
		uint32_t f_l_32 = BitWizard::lowest_order_bit_index(uint32_t(i));
		uint32_t f_l_arch_32 = BitWizard::lowest_order_bit_index_arch(uint32_t(i));

		uint32_t h_32 = BitWizard::highest_order_bit_index_slow(uint32_t(i));
		uint32_t f_h_32 = BitWizard::highest_order_bit_index(uint32_t(i));
		uint32_t f_h_arch_32 = BitWizard::highest_order_bit_index_arch(uint32_t(i));

		if (l_32 != f_l_32 || l_32 != f_l_arch_32)
		{
			std::cout << "for input number: " << uint32_t(i) << ", l_32: " << l_32 << ", f_l_32: " << f_l_32 << ", f_l_arch_32: " << f_l_arch_32 << "\n";
			return 1;
		}
		if (h_32 != f_h_32 || h_32 != f_h_arch_32)
		{
			std::cout << "for input number: " << uint32_t(i) << ", h_32: " << h_32 << ", f_h_32: " << f_h_32 << ", f_h_arch_32: " << f_h_arch_32 << "\n";
			return 1;
		}
		uint64_t l_64 = BitWizard::lowest_order_bit_index_slow(uint64_t(i));
		uint64_t f_l_64 = BitWizard::lowest_order_bit_index(uint64_t(i));
		uint64_t f_l_arch_64 = BitWizard::lowest_order_bit_index_arch(uint64_t(i));

		uint64_t h_64 = BitWizard::highest_order_bit_index_slow(uint64_t(i));
		uint64_t f_h_64 = BitWizard::highest_order_bit_index(uint64_t(i));
		uint64_t f_h_arch_64 = BitWizard::highest_order_bit_index_arch(uint64_t(i));

		if (l_64 != f_l_64 || l_64 != f_l_arch_64)
		{
			std::cout << "for input number: " << uint64_t(i) << ", l_64: " << l_64 << ", f_l_64: " << f_l_64 << ", f_l_arch_64: " << f_l_arch_64 << "\n";
			return 1;
		}
		if (h_64 != f_h_64 || h_64 != f_h_arch_64)
		{
			std::cout << "for input number: " << uint64_t(i) << ", h_64: " << h_64 << ", f_h_64: " << f_h_64 << ", f_h_arch_64: " << f_h_arch_64 << "\n";
			return 1;
		}
	}
	std::cout << "All numbers passed...\n";
	return 0;
}