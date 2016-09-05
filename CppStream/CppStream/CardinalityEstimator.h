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
		uint64_t cardinality_estimation();
	private:
		uint32_t swap_bits_32(uint32_t v);
		// L value is 64
		uint32_t bitmap;
	};
}
#endif // !CARDINALITY_ESTIMATOR_