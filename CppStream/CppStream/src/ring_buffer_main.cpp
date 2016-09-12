#pragma once
#include "../include/CircularBuffer.h"

int t_main(int argc, char** argv)
{
	/*std::vector<uint16_t> tasks = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
	const size_t buffer_size = (size_t) 1e+6;
	std::cout << "Current architecture size of size_t: " << sizeof(size_t) << " (max value for size_t: " << 
		std::numeric_limits<size_t>::max() << ")." << std::endl;
	CircularBuffer<uint16_t> ring_buffer(tasks, 10);
	std::vector<BasicWindow<uint16_t>>& buffer_ref = ring_buffer.get_buffer();
	std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
	for (size_t i = 0; i < buffer_size * 10; ++i)
	{
		if (ring_buffer.is_full())
		{
			ring_buffer.pop_tail();
		}
		ring_buffer.progress_head();
		uint16_t head = ring_buffer.get_head();
		buffer_ref[head].cardinality[0].insert(uint16_t(i));
	}
	std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> traverse_time = end - start;
	std::cout << "Total time: " << traverse_time.count() << " msec." << std::endl;

	char ch;
	std::cin >> ch;*/
	return 0;
}