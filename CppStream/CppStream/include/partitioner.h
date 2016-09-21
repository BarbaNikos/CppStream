#pragma once
#include <cinttypes>
#include <string>

#ifndef _PARTITIONER_H_
#define _PARTITIONER_H_
class Partitioner
{
public:
	Partitioner();
	~Partitioner();
	virtual uint16_t partition_next(const std::string& key, const size_t key_len) = 0;
};

inline Partitioner::Partitioner()
{
}

inline Partitioner::~Partitioner()
{
}
#endif // !_PARTITIONER_H_
