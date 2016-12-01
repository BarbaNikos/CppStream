#pragma once
#include <cinttypes>
#include <string>

#ifndef PARTITION_POLICY_H_
#include "partition_policy.h"
#endif // !PARTITION_POLICY_H_

#ifndef _PARTITIONER_H_
#define _PARTITIONER_H_
class Partitioner
{
public:
	virtual ~Partitioner() {}
	virtual void init() = 0;
	virtual unsigned int partition_next(const void* key, const size_t key_len) = 0;
	virtual unsigned long long get_min_task_count() = 0;
	virtual unsigned long long get_max_task_count() = 0;
};
#endif // !_PARTITIONER_H_