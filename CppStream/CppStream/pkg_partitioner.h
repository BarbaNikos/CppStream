#pragma once

#include <vector>

#include "MurmurHash3.h"

#ifndef PKG_PARTITIONER_H_
#define PKG_PARTITIONER_H_
class PkgPartitioner
{
public:
	PkgPartitioner(const std::vector<uint16_t>& tasks);
	~PkgPartitioner();
	__int16 partition_next(const std::string& key, size_t key_len);
private:
	std::vector<uint16_t> tasks;
	std::vector<uint64_t> task_count;
};

#endif // !PKG_PARTITIONER_H_

inline PkgPartitioner::PkgPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks), task_count(tasks.size(), uint64_t(0))
{
}

inline PkgPartitioner::~PkgPartitioner()
{
}

inline __int16 PkgPartitioner::partition_next(const std::string& key, size_t key_len)
{
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &first_choice);
	MurmurHash3_x86_32(key.c_str(), key_len, 17, &second_choice);
	first_choice = first_choice % tasks.size();
	second_choice = second_choice % tasks.size();
	uint16_t selected_choice = task_count[first_choice] < task_count[second_choice] ? first_choice : second_choice;
	task_count[selected_choice] += 1;
	return selected_choice;
}