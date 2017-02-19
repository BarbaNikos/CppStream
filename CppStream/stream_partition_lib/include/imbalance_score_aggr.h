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
	virtual ~KeyExtractor() {}
	virtual Tuple_Key extract_key(const Tuple& t) const = 0;
	virtual KeyExtractor* clone() const = 0;
};

template <class Tuple, class Tuple_Key>
class ImbalanceScoreAggr
{
public:
	ImbalanceScoreAggr(size_t tasks, const KeyExtractor<Tuple, Tuple_Key>& extractor) 
	: key_extractor(extractor.clone()), tasks(tasks), tuple_count(tasks, 0), 
	key_count(tasks, std::unordered_set<Tuple_Key>()) {}

	ImbalanceScoreAggr(const ImbalanceScoreAggr<Tuple, Tuple_Key>& i) 
		: key_extractor(i.key_extractor->clone()), tasks(i.tasks), tuple_count(i.tuple_count), 
	key_count(i.key_count) {}

	void incremental_measure_score(size_t index, const Tuple& t)
	{
		Tuple_Key key = key_extractor->extract_key(t);
		tuple_count[index] += 1;
		key_count[index].insert(key);
	}
	void incremental_measure_score(size_t index, const Tuple_Key& k)
	{
		tuple_count[index] += 1;
		key_count[index].insert(k);
	}

	void incremental_measure_score_tuple_count(size_t index, const Tuple& t)
	{
		tuple_count[index] += 1;
	}
	void incremental_measure_score_tuple_count(size_t index, const Tuple_Key& k)
	{
		tuple_count[index] += 1;
	}

	double imbalance() const
	{
		double max_ = *(std::max_element(tuple_count.cbegin(), tuple_count.cend()));
		double mean_ = std::accumulate(tuple_count.cbegin(), tuple_count.cend(), 0.0) / tuple_count.size();
		/*std::cout << "tuple count: ";
		for (size_t i = 0; i < tuple_count.size(); ++i)
		std::cout << "(" << tuple_count[i] << ", " << key_count[i].size() << ") ";
		std::cout << "\n";*/
		return max_ - mean_;
	}

	float cardinality_imbalance()
	{
		std::vector<size_t> key_count_size;
		for (auto it = key_count.cbegin(); it != key_count.cend(); ++it)
			key_count_size.push_back(it->size());
		double max_ = double(*(std::max_element(key_count_size.cbegin(), key_count_size.cend())));
		double mean_ = std::accumulate(key_count_size.cbegin(), key_count_size.cend(), 0.0) / key_count_size.size();
		return max_ - mean_;
	}

	bool locate_replicated_keys()
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
private:
	std::unique_ptr<KeyExtractor<Tuple, Tuple_Key>> key_extractor;
	size_t tasks;
	std::vector<size_t> tuple_count;
	std::vector<std::unordered_set<Tuple_Key>> key_count;
};
#endif // !IMBALANCE_SCORE_AGGR_H_
