#pragma once
#include <vector>
#include <unordered_set>
#include <numeric>

#ifndef IMBALANCE_SCORE_AGGR_H_
#define IMBALANCE_SCORE_AGGR_H_

template <class Tuple, class Tuple_Key>
class KeyExtractor
{
public:
	virtual Tuple_Key extract_key(const Tuple& t) const { return Tuple_Key(); }
};



template <class Tuple, class Tuple_Key>
class ImbalanceScoreAggr
{
public:
	ImbalanceScoreAggr(unsigned long tasks, const KeyExtractor<Tuple, Tuple_Key>& extractor);
	ImbalanceScoreAggr(const ImbalanceScoreAggr<Tuple, Tuple_Key>& i);
	~ImbalanceScoreAggr();
	void measure_score(const std::vector<std::vector<Tuple>>& partitioned_stream, const KeyExtractor<Tuple, Tuple_Key>& extractor);
	void incremental_measure_score(size_t index, const Tuple& t);
	void incremental_measure_score(size_t index, const Tuple_Key& k);
	void incremental_measure_score_tuple_count(size_t index, const Tuple& t);
	void incremental_measure_score_tuple_count(size_t index, const Tuple_Key& k);
	float imbalance();
	float cardinality_imbalance();
	bool locate_replicated_keys();
private:
	KeyExtractor<Tuple, Tuple_Key> key_extractor;
	unsigned long tasks;
	std::vector<unsigned long long> tuple_count;
	std::vector<std::unordered_set<Tuple_Key>> key_count;
};
#endif // !IMBALANCE_SCORE_AGGR_H_

template<class Tuple, class Tuple_Key>
inline ImbalanceScoreAggr<Tuple, Tuple_Key>::ImbalanceScoreAggr(unsigned long tasks, const KeyExtractor<Tuple, Tuple_Key>& extractor) : key_extractor(extractor), 
tasks(tasks), tuple_count(tasks, 0), key_count(tasks, std::unordered_set<Tuple_Key>()) 
{
}

template <class Tuple, class Tuple_Key>
ImbalanceScoreAggr<Tuple, Tuple_Key>::ImbalanceScoreAggr(const ImbalanceScoreAggr<Tuple, Tuple_Key>& i) : key_extractor(i.key_extractor), tasks(i.tasks), tuple_count(i.tuple_count), 
key_count(i.key_count)
{
}

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
	for (typename std::vector<std::vector<Tuple>>::const_iterator sub_stream_it = partitioned_stream.cbegin(); sub_stream_it != partitioned_stream.cend(); ++sub_stream_it)
	{
		std::unordered_set<Tuple_Key> substream_key_set;
		for (typename std::vector<Tuple>::const_iterator sub_stream_tuple_it = sub_stream_it->cbegin(); sub_stream_tuple_it != sub_stream_it->cend(); ++sub_stream_tuple_it)
		{
			Tuple_Key key = extractor.extract_key(*sub_stream_tuple_it);
			substream_key_set.insert(key);
		}
		this->key_count.push_back(substream_key_set);
		this->tuple_count.push_back(sub_stream_it->size());
	}
}

template<class Tuple, class Tuple_Key>
inline void ImbalanceScoreAggr<Tuple, Tuple_Key>::incremental_measure_score(size_t index, const Tuple & t)
{
	Tuple_Key key = key_extractor.extract_key(t);
	tuple_count[index]++;
	key_count[index].insert(key);
}

template <class Tuple, class Tuple_Key>
void ImbalanceScoreAggr<Tuple, Tuple_Key>::incremental_measure_score(size_t index, const Tuple_Key& k)
{
	tuple_count[index]++;
	key_count[index].insert(k);
}

template<class Tuple, class Tuple_Key>
inline void ImbalanceScoreAggr<Tuple, Tuple_Key>::incremental_measure_score_tuple_count(size_t index, const Tuple & t)
{
	tuple_count[index]++;
}

template <class Tuple, class Tuple_Key>
void ImbalanceScoreAggr<Tuple, Tuple_Key>::incremental_measure_score_tuple_count(size_t index, const Tuple_Key& k)
{
	tuple_count[index]++;
}

template<class Tuple, class Tuple_Key>
inline float ImbalanceScoreAggr<Tuple, Tuple_Key>::imbalance()
{
	std::vector<unsigned long long>::iterator max_it = std::max_element(tuple_count.begin(), tuple_count.end());
	float mean_tuple_count = static_cast<float>(std::accumulate(tuple_count.begin(), tuple_count.end(), 0.0)) / tuple_count.size();
	return *max_it - mean_tuple_count;
}

template<class Tuple, class Tuple_Key>
inline float ImbalanceScoreAggr<Tuple, Tuple_Key>::cardinality_imbalance()
{
	std::vector<size_t> key_count_size;
	for (typename std::vector<std::unordered_set<Tuple_Key>>::const_iterator it = this->key_count.cbegin(); it != this->key_count.cend(); ++it)
	{
		key_count_size.push_back(it->size());
	}
	std::vector<size_t>::iterator max_key_it = std::max_element(key_count_size.begin(), key_count_size.end());
	float mean_key_count = static_cast<float>(std::accumulate(key_count_size.begin(), key_count_size.end(), 0.0)) / key_count_size.size();
	return *max_key_it - mean_key_count;
}

template<class Tuple, class Tuple_Key>
inline bool ImbalanceScoreAggr<Tuple, Tuple_Key>::locate_replicated_keys()
{
	for (auto it = key_count.cbegin(); it != key_count.cend(); ++it)
	{
		for (auto inner_it = it + 1; inner_it != key_count.cend(); ++inner_it)
		{
			for (auto element_it = it->cbegin(); element_it != it->cend(); ++element_it)
			{
				if (inner_it->find(*element_it) != inner_it->cend())
				{
					return true;
				}
			}
		}
	}
	return false;
}
