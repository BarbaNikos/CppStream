#include "gtest/gtest.h"
#include "c_hll.h"
#include "murmurhash_3.h"
#include "../include/hyperloglog.hpp"
#include <unordered_set>
#include "../../gcm/include/google_cluster_monitor_query.h"
#include <cstdlib>
#include <ctime>
#include "../include/distinct_counter.h"

TEST(DummyTest, Zero)
{
	EXPECT_EQ(1, 1);
}

TEST(TestWillHLLOne, Zero)
{
	srand(time(nullptr));
	Hll::hll_8* myHll = (Hll::hll_8*) malloc(sizeof(Hll::hll_8));
	hll::HyperLogLog otherHll(4);
	Hll::init_8(myHll, 12, sizeof(uint64_t));
	std::unordered_set<std::string> naive;
	hyperloglog_hip::distinct_counter<std::string> h;
	for (int i = 0; i < 10000000; ++i)
	{
		std::string pickup_c = std::to_string(rand() % 300) + "." + std::to_string(rand() % 300);
		std::string dropoff_c = std::to_string(rand() % 300) + "." + std::to_string(rand() % 300);
		std::string route = pickup_c + "-" + dropoff_c;
		/*uint32_t hash;
		MurmurHash3_x86_32(route.c_str(), strlen(route.c_str()), 313, (void*)&hash);
		uint64_t long_hash[2];
		MurmurHash3_x64_128(route.c_str(), strlen(route.c_str()), 313, (void*)&long_hash);
		uint64_t new_hash = long_hash[0] ^ long_hash[1];
		Hll::update_8(myHll, new_hash);
		otherHll.add(route.c_str(), strlen(route.c_str()));*/
		//naive.insert(std::to_string(i));
		h.insert(route);
	}
	size_t real_cardinality = naive.size();
	uint64_t myEstimation = Hll::opt_cardinality_estimation_8(myHll);
	double otherEstimation = otherHll.estimate();
	size_t otherOtherEstimation = h.count();
	destroy_8(myHll);
	free(myHll);
}