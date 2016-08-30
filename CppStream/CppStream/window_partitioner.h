#pragma once

#include <array>

#include "MurmurHash3.h"
#include "WindowLoad.h"

#ifndef WINDOW_PARTITIONER_H_
#define WINDOW_PARTITIONER_H_
template <std::size_t N, std::size_t B>
class WindowPartitioner
{
	WindowPartitioner(const std::array<uint16_t, N>& tasks);
	~WindowPartitioner();
public:
private:
	std::array<uint16_t, N> tasks;
	WindowLoad windowLoad;
};
#endif // !WINDOW_PARTITIONER_H_
