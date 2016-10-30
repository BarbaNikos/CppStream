#pragma once
#include <iostream>
#include <vector>
#include <unordered_set>
#include <numeric>

#ifndef TPCH_STRUCT_LIB_H_
#include "tpch_struct_lib.h"
#endif // !TPCH_STRUCT_LIB_H_

#ifndef TPCH_UTIL_H_
#include "tpch_util.h"
#endif // !TPCH_UTIL_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#define IMBALANCE_SCORE_AGGR_H_
template <class Tuple, class Tuple_Key>
class KeyExtractor
{
public:
	virtual Tuple_Key extract_key(const Tuple& t) const { return Tuple_Key(); }
};

class TpchQueryOneKeyExtractor : public KeyExtractor<Experiment::Tpch::lineitem, std::string>
{
public:
	std::string extract_key(const Experiment::Tpch::lineitem& l) const;
};

class TpchQueryThreeCustomerKeyExtractor : public KeyExtractor<Experiment::Tpch::q3_customer, uint32_t>
{
public:
	uint32_t extract_key(const Experiment::Tpch::q3_customer& c) const;
};

class TpchQueryThreeOrderKeyExtractor : public KeyExtractor<Experiment::Tpch::order, uint32_t>
{
public:
	uint32_t extract_key(const Experiment::Tpch::order& o) const;
};

class TpchQueryThreeLineitemKeyExtractor : public KeyExtractor<Experiment::Tpch::lineitem, uint32_t>
{
public:
	uint32_t extract_key(const Experiment::Tpch::lineitem& l) const;
};

template <class Tuple, class Tuple_Key>
class ImbalanceScoreAggr
{
public:
	ImbalanceScoreAggr();
	~ImbalanceScoreAggr();
	void measure_score(const std::vector<std::vector<Tuple>>& partitioned_stream, const KeyExtractor<Tuple, Tuple_Key>& extractor);
	float imbalance();
	float cardinality_imbalance();
private:
	std::vector<unsigned long long> tuple_count;
	std::vector<std::unordered_set<Tuple_Key>> key_count;
};
#endif // !IMBALANCE_SCORE_AGGR_H_

inline std::string TpchQueryOneKeyExtractor::extract_key(const Experiment::Tpch::lineitem& l) const
{
	return std::to_string(l.l_returnflag) + "," + std::to_string(l.l_linestatus);
}

inline uint32_t TpchQueryThreeCustomerKeyExtractor::extract_key(const Experiment::Tpch::q3_customer& c) const
{
	return c.c_custkey;
}

inline uint32_t TpchQueryThreeOrderKeyExtractor::extract_key(const Experiment::Tpch::order& o) const
{
	return o.o_custkey;
}

inline uint32_t TpchQueryThreeLineitemKeyExtractor::extract_key(const Experiment::Tpch::lineitem& l) const
{
	return l.l_order_key;
}

template<class Tuple, class Tuple_Key>
inline ImbalanceScoreAggr<Tuple, Tuple_Key>::ImbalanceScoreAggr() {}

template<class Tuple, class Tuple_Key>
inline ImbalanceScoreAggr<Tuple, Tuple_Key>::~ImbalanceScoreAggr()
{
	tuple_count.clear();
	for (auto it = key_count.begin(); it != key_count.end(); ++it)
	{
		it->clear();
	}
	key_count.clear();
}

template<class Tuple, class Tuple_Key>
inline void ImbalanceScoreAggr<Tuple, Tuple_Key>::measure_score(const std::vector<std::vector<Tuple>>& partitioned_stream, const KeyExtractor<Tuple, Tuple_Key>& extractor)
{
	this->key_count.clear();
	this->tuple_count.clear();
	for (std::vector<std::vector<Tuple>>::const_iterator sub_stream_it = partitioned_stream.cbegin(); sub_stream_it != partitioned_stream.cend(); ++sub_stream_it)
	{
		std::unordered_set<Tuple_Key> substream_key_set;
		for (std::vector<Tuple>::const_iterator sub_stream_tuple_it = sub_stream_it->cbegin(); sub_stream_tuple_it != sub_stream_it->cend(); ++sub_stream_tuple_it)
		{
			Tuple_Key key = extractor.extract_key(*sub_stream_tuple_it);
			substream_key_set.insert(key);
		}
		this->key_count.push_back(substream_key_set);
		this->tuple_count.push_back(sub_stream_it->size());
	}
}

template<class Tuple, class Tuple_Key>
inline float ImbalanceScoreAggr<Tuple, Tuple_Key>::imbalance()
{
	std::vector<unsigned long long>::iterator max_it = std::max_element(tuple_count.begin(), tuple_count.end());
	float mean_tuple_count = (float)std::accumulate(tuple_count.begin(), tuple_count.end(), 0.0) / tuple_count.size();
	return *max_it - mean_tuple_count;
}

template<class Tuple, class Tuple_Key>
inline float ImbalanceScoreAggr<Tuple, Tuple_Key>::cardinality_imbalance()
{
	std::vector<size_t> key_count_size;
	for (std::vector<std::unordered_set<Tuple_Key>>::const_iterator it = this->key_count.cbegin(); it != this->key_count.cend(); ++it)
	{
		key_count_size.push_back(it->size());
	}
	std::vector<size_t>::iterator max_key_it = std::max_element(key_count_size.begin(), key_count_size.end());
	float mean_key_count = (float)std::accumulate(key_count_size.begin(), key_count_size.end(), 0.0) / key_count_size.size();
	return *max_key_it - mean_key_count;
}
