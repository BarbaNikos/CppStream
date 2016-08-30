#include <iostream>
#include <chrono>

#include "CircularBuffer.h"

int main(int argc, char** argv)
{
	CircularBuffer<uint16_t, 3097> ring_buffer;
	std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
	for (size_t i = 0; i < 3097 * 10; ++i)
	{
		if (ring_buffer.is_full())
		{
			ring_buffer.pop_tail();
		}
		ring_buffer.put((uint16_t)i);
	}
	std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
	std::chrono::duration<double, std::milli> traverse_time = end - start;
	std::cout << "Total time: " << traverse_time.count() << " msec." << std::endl;
	char ch;
	std::cin >> ch;
	return 0;
}