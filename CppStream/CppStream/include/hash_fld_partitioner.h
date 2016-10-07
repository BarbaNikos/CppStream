#pragma once
#include <vector>
#include <cinttypes>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef MURMURHASH_3_H_
#include "murmurhash_3.h"
#endif // !MURMURHASH_3_H_


#ifndef HASH_FLD_PARTITIONER_H_
#define HASH_FLD_PARTITIONER_H_
class HashFieldPartitioner : public Partitioner
{
public:
	HashFieldPartitioner(const std::vector<uint16_t>& tasks);
	~HashFieldPartitioner();
	uint16_t partition_next(const void* key, const size_t key_len);
private:
	std::vector<uint16_t> tasks;
};

HashFieldPartitioner::HashFieldPartitioner(const std::vector<uint16_t>& tasks) : tasks(tasks)
{
}

HashFieldPartitioner::~HashFieldPartitioner()
{
}

uint16_t HashFieldPartitioner::partition_next(const void* key, const size_t key_len)
{
	uint64_t hash_code, long_hash_code[2];
	MurmurHash3_x64_128(key, key_len, 13, &long_hash_code);
	hash_code = long_hash_code[0] ^ long_hash_code[1];
	return uint16_t(hash_code % tasks.size());
}
#endif // !HASH_FLD_PARTITIONER_H_
