#pragma once
#include <iostream>
#include <vector>
#include <unordered_set>
#include <numeric>

#ifndef TPCH_STRUCT_LIB_H_
#include "tpch_struct_lib.h"
#endif // !TPCH_STRUCT_LIB_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif //!IMBALANCE_SCORE_AGGR_H_

#ifndef TPCH_KEY_EXTRACTOR_H_
#define TPCH_KEY_EXTRACTOR_H_
class TpchQueryOneKeyExtractor : public KeyExtractor<Experiment::Tpch::lineitem, std::string>
{
public:
	std::string extract_key(const Experiment::Tpch::lineitem& l) const override;
};

class TpchQueryThreeCustomerKeyExtractor : public KeyExtractor<Experiment::Tpch::q3_customer, uint32_t>
{
public:
	uint32_t extract_key(const Experiment::Tpch::q3_customer& c) const override;
};

class TpchQueryThreeOrderKeyExtractor : public KeyExtractor<Experiment::Tpch::order, uint32_t>
{
public:
	uint32_t extract_key(const Experiment::Tpch::order& o) const override;
};

class TpchQueryThreeLineitemKeyExtractor : public KeyExtractor<Experiment::Tpch::lineitem, uint32_t>
{
public:
	uint32_t extract_key(const Experiment::Tpch::lineitem& l) const override;
};
#endif

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