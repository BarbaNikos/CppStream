#pragma once
#include <iostream>

#include "MurmurHash3.h"

#ifndef CARDINALITY_ESTIMATOR_
#define CARDINALITY_ESTIMATOR_
namespace CardinalityEstimator
{
	class ProbCount
	{
	public:
		ProbCount();
		~ProbCount();
		void update_bitmap(uint32_t value);
		uint32_t cardinality_estimation();
	private:
		// L value is 64
		uint32_t bitmap;
	};

	class HyperLoglog
	{
	public:
		HyperLoglog(uint8_t k);
		~HyperLoglog();
	private:
		uint8_t k;
		void* buckets;
	};
}
#endif // !CARDINALITY_ESTIMATOR_