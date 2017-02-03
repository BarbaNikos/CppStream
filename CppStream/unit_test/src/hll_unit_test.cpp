#include "gtest/gtest.h"
#include "c_hll.h"
#include "murmurhash_3.h"
#include "../include/hyperloglog.hpp"
#include <unordered_set>
#include "../../gcm/include/google_cluster_monitor_query.h"
#include <cstdlib>
#include <ctime>
#include "../include/distinct_counter.h"
#include <sstream>

TEST(DummyTest, Zero)
{
	EXPECT_EQ(1, 1);
}

TEST(TestWillHLLOne, Zero)
{
	srand(time(nullptr));
	Hll::hll_8* myHll = (Hll::hll_8*) malloc(sizeof(Hll::hll_8));
	hll::HyperLogLog otherHll(4);
	Hll::init_8(myHll, 4, sizeof(uint32_t));
	std::unordered_set<std::string> naive;
	hyperloglog_hip::distinct_counter<std::string> h;
	for (int i = 0; i < 1e+5; ++i)
	{
		std::stringstream ss;
		ss << std::to_string(rand() % 300) << "." << std::to_string(rand() % 300) << "-" << 
			std::to_string(rand() % 300) << "." << std::to_string(rand() % 300);
		std::string route = ss.str();
		uint32_t hash = 0;
		MurmurHash3_x86_32(route.c_str(), strlen(route.c_str()), 313, &hash);
		Hll::update_8(myHll, hash);
		otherHll.add(route.c_str(), strlen(route.c_str()));
		naive.insert(std::to_string(i));
		h.insert(route);
	}
	size_t real_cardinality = naive.size();
	uint64_t myEstimation = Hll::opt_cardinality_estimation_8(myHll);
	double otherEstimation = otherHll.estimate();
	size_t otherOtherEstimation = h.count();
	destroy_8(myHll);
	free(myHll);
}