#pragma once
#include <cinttypes>
#include <string>

#ifndef _PARTITIONER_H_
#define _PARTITIONER_H_
class Partitioner
{
public:
	Partitioner();
	virtual ~Partitioner();
	virtual unsigned int partition_next(const void* key, const size_t key_len) = 0;
	virtual unsigned long long get_min_task_count() = 0;
	virtual unsigned long long get_max_task_count() = 0;
};

inline Partitioner::Partitioner()
{
}

inline Partitioner::~Partitioner()
{
}
#endif // !_PARTITIONER_H_
