#pragma once
#include <unordered_set>
#include "hip_estimator.h"

#ifndef CARDINALITY_ESTIMATOR
#define CARDINALITY_ESTIMATOR
namespace CardinalityEstimator
{
	template<typename Key>
	class cardinality_estimator
	{
	public:
		virtual ~cardinality_estimator() {}
		virtual void init() = 0;
		virtual void insert(const Key &v) = 0;
		virtual size_t count() const = 0;
		virtual void copy(const cardinality_estimator<Key>& o) = 0;
	};

	template<typename Key>
	class naive_cardinality_estimator : public cardinality_estimator<Key>
	{
	public:
		naive_cardinality_estimator<Key>() {}
		naive_cardinality_estimator<Key>(const naive_cardinality_estimator<Key>& x)
			: S(x.S) {}

		void init() override
		{
			S.clear();
		}
		
		void insert(const Key &v) override
		{
			S.insert(v);
		}

		size_t count() const override
		{
			return S.size();
		}

		void copy(const cardinality_estimator<Key>& o) override
		{
			naive_cardinality_estimator<Key>* rename_ptr = (naive_cardinality_estimator<Key>*)(&o);
			S = rename_ptr->S;
		}
	private:
		std::unordered_set<Key> S;
	};

	template<typename Key>
	class hip_cardinality_estimator : public cardinality_estimator<Key>
	{
	public:
		hip_cardinality_estimator(size_t num_bucket_bits = 12)
			: hip(num_bucket_bits) 
		{
			num_bucket_bits_ = num_bucket_bits;
		}

		hip_cardinality_estimator(const hip_cardinality_estimator<Key>& x) 
			: num_bucket_bits_(x.num_bucket_bits_), hip(x.hip) {}

		void init() override
		{
			hip.init();
		}

		void insert(const Key &v) override
		{
			hip.insert(v);
		}

		size_t count() const override
		{
			return hip.count();
		}

		void copy(const cardinality_estimator<Key>& o) override
		{
			hip_cardinality_estimator<Key>* rename_ptr = (hip_cardinality_estimator<Key>*)(&o);
			hip.copy(rename_ptr->hip);
		}

	private:
		size_t num_bucket_bits_;
		hip_estimator<Key> hip;
	};
}
#endif // ! CARDINALITY_ESTIMATOR