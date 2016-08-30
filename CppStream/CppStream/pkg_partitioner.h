#pragma once

#include <array>

#include "MurmurHash3.h"

#ifndef PKG_PARTITIONER_H_
#define PKG_PARTITIONER_H_
template <std::size_t N>
class PkgPartitioner
{
public:
	PkgPartitioner(const std::array<uint16_t, N>& tasks);
	~PkgPartitioner();
	__int16 partition_next(const std::string& key, size_t key_len);
private:
	std::array<uint16_t, N> tasks;
	std::array<uint64_t, N> task_count;
};

#endif // !PKG_PARTITIONER_H_

template<std::size_t N>
inline PkgPartitioner<N>::PkgPartitioner(const std::array<uint16_t, N>& tasks)
{
	this->tasks = tasks;
	task_count = { 0 };
}

template<std::size_t N>
inline PkgPartitioner<N>::~PkgPartitioner()
{
}

template<std::size_t N>
inline __int16 PkgPartitioner<N>::partition_next(const std::string& key, size_t key_len)
{
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &first_choice);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &second_choice);
	first_choice = first_choice % tasks.size();
	second_choice = second_choice % tasks.size();
	uint16_t selected_choice = task_count[first_choice] < task_count[second_choice] ? first_choice : second_choice;
	task_count[selected_choice] += 1;
	return 0;
}