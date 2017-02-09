#include "gtest/gtest.h"
#include "c_hll.h"
#include "murmurhash_3.h"
#include <unordered_set>
#include <cstdlib>
#include <ctime>
#include <sstream>
#include "dense_array.h"
#include "hip_estimator.h"

TEST(DummyTest, Zero)
{
	EXPECT_EQ(1, 1);
}

TEST(TestWillHLLOne, Zero)
{
	srand(time(nullptr));
	std::unordered_set<std::string> naive;
	hip_estimator<uint32_t> h;
	hip_estimator<std::string> h_str;
	for (int i = 0; i < 100; ++i)
	{
		std::stringstream ss;
		ss << std::to_string(rand() % 300) << "." << std::to_string(rand() % 300) << "-" << 
			std::to_string(rand() % 300) << "." << std::to_string(rand() % 300);
		std::string route = ss.str();
		uint32_t hash = 0;
		MurmurHash3_x86_32(route.c_str(), strlen(route.c_str()), 313, &hash);
		naive.insert(std::to_string(i));
		h.insert(hash);
		h_str.insert(route);
	}
	size_t real_cardinality = naive.size();
	size_t hipEstimation = h.count();
	double absolute_error = abs(long(real_cardinality - hipEstimation));
	double relError = absolute_error / (double)real_cardinality;
	size_t hipStrEstimation = h_str.count();
	double absolute_str_error = abs(long(real_cardinality - hipStrEstimation));
	double relStrError = absolute_str_error / (double)real_cardinality;
}