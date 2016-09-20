#pragma once
#include <vector>
#include <cinttypes>

#include "MurmurHash3.h"

#ifndef HASH_FLD_PARTITIONER_H_
#define HASH_FLD_PARTITIONER_H_
class HashFieldPartitioner
{
public:
	HashFieldPartitioner(const std::vector<uint16_t>& tasks);
	~HashFieldPartitioner();
	uint16_t partition_next(const std::string& key, size_t key_len);
private:
	std::vector<uint16_t> tasks;
};

HashFieldPartitioner::HashFieldPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks)
{

}

HashFieldPartitioner::~HashFieldPartitioner()
{

}

uint16_t HashFieldPartitioner::partition_next(const std::string& key, size_t key_len)
{
	uint32_t choice;
	MurmurHash3_x86_32(key.c_str(), key_len, 13, &choice);
	return uint16_t(choice % tasks.size());
}
#endif // !HASH_FLD_PARTITIONER_H_
