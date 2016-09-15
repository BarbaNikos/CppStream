#include <iostream>
#include <cmath>
#include <cstdlib>
#include <cinttypes>

#include "MurmurHash3.h"
#include "BitTrickBox.h"

#ifndef CARDINALITY_ESTIMATOR_
#define CARDINALITY_ESTIMATOR_
namespace CardinalityEstimator
{
	class ProbCount
	{
	public:
		ProbCount();
		~ProbCount();
		void update_bitmap_with_hashed_value(uint32_t hashed_value);
		void update_bitmap(uint32_t value);
		uint32_t cardinality_estimation();
	private:
		const static double phi;
		uint32_t bitmap;
	};

	class HyperLoglog
	{
	public:
		HyperLoglog(uint8_t k);
		~HyperLoglog();
		void update_bitmap_with_hashed_value(uint32_t hashed_value);
		void update_bitmap(uint32_t value);
		uint32_t cardinality_estimation();
	private:
		const static double a_16;
		const static double a_32;
		const static double a_64;
		double a_m;
		uint16_t k;
		uint16_t m;
		uint32_t* buckets;
		double _current_sum;
		double _multiplier;
	};
}
#endif // !CARDINALITY_ESTIMATOR_