#include "gtest/gtest.h"
#include "hip_estimator.h"
#include "card_estimator.h"
#include "ca_partition_lib.h"
#include "pkg_partitioner.h"

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
		size_t card = estimator->imitate_insert(random);
		estimator->insert(random);
		size_t new_card = estimator->count();
		EXPECT_EQ(card, new_card);
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
		size_t card = estimator->imitate_insert(random);
		estimator->insert(random);
		size_t new_card = estimator->count();
		EXPECT_EQ(card, new_card);
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
		size_t card = estimator->imitate_insert(random);
		estimator->insert(random);
		size_t new_card = estimator->count();
		EXPECT_EQ(card, new_card);
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
		size_t card = estimator->imitate_insert(random);
		estimator->insert(random);
		size_t new_card = estimator->count();
		EXPECT_EQ(card, new_card);
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

TEST(CaPartitionerTest, Constructor)
{
	std::vector<uint16_t> tasks;
	tasks.push_back(1);
	tasks.push_back(2);
	tasks.push_back(3);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> est;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_est;
	LoadAwarePolicy policy;
	CaPartitionLib::CA<uint64_t> partitioner(tasks, policy, est);
	std::vector<size_t> v;
	partitioner.get_cardinality_vector(v);
	CaPartitionLib::CA<uint64_t> hip_partitioner(tasks, policy, hip_est);
	std::vector<size_t> v_hip;
	hip_partitioner.get_cardinality_vector(v_hip);
	EXPECT_EQ(v.size(), v_hip.size());
	EXPECT_EQ(tasks.size(), v.size());
	for (size_t i = 0; i < v_hip.size(); ++i)
	{
		EXPECT_EQ(v[i], v_hip[i]);
	}
	EXPECT_EQ(partitioner.get_max_cardinality(), 0);
	EXPECT_EQ(partitioner.get_max_task_count(), 0);
	EXPECT_EQ(hip_partitioner.get_max_cardinality(), 0);
	EXPECT_EQ(hip_partitioner.get_max_task_count(), 0);
}

TEST(AnPartitionTest, Constructor)
{
	std::vector<uint16_t> tasks;
	tasks.push_back(1);
	tasks.push_back(2);
	tasks.push_back(3);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> est;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_est;
	CaPartitionLib::AN<uint64_t> partitioner(tasks, est);
	CaPartitionLib::AN<uint64_t> hip_partitioner(tasks, est);
	std::vector<size_t> v;
	partitioner.get_cardinality_vector(v);
	std::vector<size_t> v_hip;
	hip_partitioner.get_cardinality_vector(v_hip);
	EXPECT_EQ(v.size(), v_hip.size());
	EXPECT_EQ(tasks.size(), v.size());
	for (size_t i = 0; i < v_hip.size(); ++i)
	{
		EXPECT_EQ(v[i], v_hip[i]);
	}
	EXPECT_EQ(partitioner.get_max_cardinality(), 0);
	EXPECT_EQ(partitioner.get_max_task_count(), 0);
	EXPECT_EQ(hip_partitioner.get_max_cardinality(), 0);
	EXPECT_EQ(hip_partitioner.get_max_task_count(), 0);
}

TEST(CaPartitionerTest, CopyConstructor)
{
	std::vector<uint16_t> tasks;
	tasks.push_back(1);
	tasks.push_back(2);
	tasks.push_back(3);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> est;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_est;
	LoadAwarePolicy policy;
	CaPartitionLib::CA<uint64_t> partitioner(tasks, policy, est);
	CaPartitionLib::CA<uint64_t> hip_partitioner(tasks, policy, hip_est);
	srand(time(nullptr));
	for (size_t i = 0; i < 100; ++i)
	{
		uint64_t r = rand() % 1000;
		partitioner.partition_next(&r, sizeof(r));
		hip_partitioner.partition_next(&r, sizeof(r));
	}
	ASSERT_TRUE(partitioner.get_max_task_count() <= 100);
	ASSERT_TRUE(partitioner.get_max_cardinality() <= 100);
	ASSERT_TRUE(hip_partitioner.get_max_task_count() <= 100);
	ASSERT_TRUE(hip_partitioner.get_max_cardinality() <= 100);
	CaPartitionLib::CA<uint64_t> partitioner_replica(partitioner);
	CaPartitionLib::CA<uint64_t> hip_partitioner_replica(hip_partitioner);
	for (size_t i = 0; i < 10; ++i)
	{
		uint64_t r = rand() % 1000;
		size_t c1 = partitioner.partition_next(&r, sizeof(r));
		size_t c2 = partitioner_replica.partition_next(&r, sizeof(r));
		EXPECT_EQ(c1, c2);
		size_t hc1 = hip_partitioner.partition_next(&r, sizeof(r));
		size_t hc2 = hip_partitioner_replica.partition_next(&r, sizeof(r));
		EXPECT_EQ(hc1, hc2);
	}
}

TEST(AnPartitionTest, CopyConstructor)
{
	std::vector<uint16_t> tasks;
	tasks.push_back(1);
	tasks.push_back(2);
	tasks.push_back(3);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> est;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_est;
	CaPartitionLib::AN<uint64_t> partitioner(tasks, est);
	CaPartitionLib::AN<uint64_t> hip_partitioner(tasks, est);
	srand(time(nullptr));
	for (size_t i = 0; i < 100; ++i)
	{
		uint64_t r = rand() % 1000;
		partitioner.partition_next(&r, sizeof(r));
		hip_partitioner.partition_next(&r, sizeof(r));
	}
	ASSERT_TRUE(partitioner.get_max_task_count() <= 100);
	ASSERT_TRUE(partitioner.get_max_cardinality() <= 100);
	ASSERT_TRUE(hip_partitioner.get_max_task_count() <= 100);
	ASSERT_TRUE(hip_partitioner.get_max_cardinality() <= 100);
	CaPartitionLib::AN<uint64_t> partitioner_replica(partitioner);
	CaPartitionLib::AN<uint64_t> hip_partitioner_replica(hip_partitioner);
	for (size_t i = 0; i < 10; ++i)
	{
		uint64_t r = rand() % 1000;
		size_t c1 = partitioner.partition_next(&r, sizeof(r));
		size_t c2 = partitioner_replica.partition_next(&r, sizeof(r));
		EXPECT_EQ(c1, c2);
		size_t hc1 = hip_partitioner.partition_next(&r, sizeof(r));
		size_t hc2 = hip_partitioner_replica.partition_next(&r, sizeof(r));
		EXPECT_EQ(hc1, hc2);
	}
}

TEST(MultiPkgTest, UseCase)
{
	std::vector<uint16_t> tasks;
	for (size_t i = 0; i < 32; ++i)
		tasks.push_back(i);
	MultiPkPartitioner mPk(tasks);
	CardinalityEstimator::naive_cardinality_estimator<uint64_t> est;
	CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_est;
	CaPartitionLib::MultiAN<uint64_t> partitioner(tasks, est);
	CaPartitionLib::MultiAN<uint64_t> hip_partitioner(tasks, hip_est);
	srand(time(nullptr));
	for (size_t i = 0; i < 100; ++i)
	{
		uint64_t r = rand() % 1000;
		mPk.partition_next(&r, sizeof(r));
		partitioner.partition_next(&r, sizeof(r));
		hip_partitioner.partition_next(&r, sizeof(r));
	}
}