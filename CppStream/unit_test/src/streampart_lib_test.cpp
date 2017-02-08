#include "gtest/gtest.h"
#include "hip_estimator.h"
#include "card_estimator.h"
#include "ca_partition_lib.h"

TEST(DenseArrayTest, Init)
{
	dense_array<5, uint8_t> original(4096);
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint8_t random = rand() % 31;
		size_t index = rand() % 4095;
		original.set(index, random);
	}
	original.init();
	for (size_t i = 0; i < 4096; ++i)
	{
		EXPECT_EQ(original.get(i), 0);
	}
}

TEST(DenseArrayTest, Copy)
{
	dense_array<5, uint8_t> original(4096);
	srand(time(nullptr));
	for (size_t i = 0; i < 10000; ++i)
	{
		uint8_t random = rand() % 31;
		size_t index = rand() % 4095;
		original.set(index, random);
	}
	dense_array<5, uint8_t> replica(4096);
	replica.copy(original);
	for (size_t i = 0; i < 4096; ++i)
	{
		EXPECT_EQ(original.get(i), replica.get(i));
	}
}

TEST(DenseArrayTest, CopyConstructor)
{
	dense_array<5, uint8_t> original(4096);
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint8_t random = rand() % 31;
		size_t index = rand() % 4095;
		original.set(index, random);
	}
	dense_array<5, uint8_t> replica(original);
	for (size_t i = 0; i < 4096; ++i)
	{
		EXPECT_EQ(original.get(i), replica.get(i));
	}
}

TEST(HipEstimator, CopyConstructor)
{
	hip_estimator<uint32_t> hip;
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		hip.insert(random);
	}
	hip_estimator<uint32_t> replica(hip);
	EXPECT_EQ(hip.count(), replica.count());
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		hip.insert(random);
		replica.insert(random);
		EXPECT_EQ(hip.count(), replica.count());
	}
}

TEST(HipEstimator, Init)
{
	hip_estimator<uint32_t> hip;
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		hip.insert(random);
	}
	ASSERT_TRUE(hip.count() > 0);
	hip.init();
	ASSERT_TRUE(hip.count() == 0);
}

TEST(HipEstimator, Copy)
{
	hip_estimator<uint32_t> hip;
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		hip.insert(random);
	}
	hip_estimator<uint32_t> replica;
	replica.copy(hip);
	EXPECT_EQ(hip.count(), replica.count());
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		hip.insert(random);
		replica.insert(random);
		EXPECT_EQ(hip.count(), replica.count());
	}
}

TEST(NaiveCardEstimatorTest, Init)
{
	CardinalityEstimator::cardinality_estimator<uint32_t>* estimator = 
		new CardinalityEstimator::naive_cardinality_estimator<uint32_t>();
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		estimator->insert(random);
	}
	ASSERT_TRUE(estimator->count() > 0);
	estimator->init();
	ASSERT_TRUE(estimator->count() == 0);
	delete estimator;
}

TEST(NaiveCardEstimatorTest, Copy)
{
	CardinalityEstimator::cardinality_estimator<uint32_t>* estimator =
		new CardinalityEstimator::naive_cardinality_estimator<uint32_t>();
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		estimator->insert(random);
	}
	ASSERT_TRUE(estimator->count() > 0);
	CardinalityEstimator::cardinality_estimator<uint32_t>* replica =
		new CardinalityEstimator::naive_cardinality_estimator<uint32_t>();
	replica->copy(*estimator);
	EXPECT_EQ(estimator->count(), replica->count());
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		estimator->insert(random);
		replica->insert(random);
	}
	EXPECT_EQ(estimator->count(), replica->count());
	delete estimator;
	delete replica;
}

TEST(HipCardEstimatorTest, Init)
{
	CardinalityEstimator::cardinality_estimator<uint32_t>* estimator =
		new CardinalityEstimator::hip_cardinality_estimator<uint32_t>();
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		estimator->insert(random);
	}
	ASSERT_TRUE(estimator->count() > 0);
	estimator->init();
	ASSERT_TRUE(estimator->count() == 0);
	delete estimator;
}

TEST(HipCardEstimatorTest, Copy)
{
	CardinalityEstimator::cardinality_estimator<uint32_t>* estimator =
		new CardinalityEstimator::hip_cardinality_estimator<uint32_t>();
	srand(time(nullptr));
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		estimator->insert(random);
	}
	ASSERT_TRUE(estimator->count() > 0);
	CardinalityEstimator::cardinality_estimator<uint32_t>* replica =
		new CardinalityEstimator::hip_cardinality_estimator<uint32_t>();
	replica->copy(*estimator);
	EXPECT_EQ(estimator->count(), replica->count());
	for (size_t i = 0; i < 1e+5; ++i)
	{
		uint32_t random = rand();
		estimator->insert(random);
		replica->insert(random);
	}
	EXPECT_EQ(estimator->count(), replica->count());
	delete estimator;
	delete replica;
}

TEST(CaPartitionerTest, HipConstructor)
{
	std::vector<uint16_t> tasks;
	tasks.push_back(1);
	tasks.push_back(2);
	tasks.push_back(3);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> est;
	LoadAwarePolicy policy;
	CaPartitionLib::CN<uint64_t> partitioner(tasks, &policy);
}