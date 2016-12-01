#pragma once
#include <iostream>
#include <vector>
#include <unordered_set>
#include <numeric>
#include <sstream>

#ifndef DEBS_STRUCTURE_LIB_H_
#include "debs_structure_lib.h"
#endif // !DEBS_STRUCTURE_LIB_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif //!IMBALANCE_SCORE_AGGR_H_

#ifndef DEBS_KEY_EXTRACTOR_H_
#define DEBS_KEY_EXTRACTOR_H_
class DebsFrequentRideKeyExtractor : public KeyExtractor<Experiment::DebsChallenge::CompactRide, std::string>
{
public:
	std::string extract_key(const Experiment::DebsChallenge::CompactRide& r) const override;
};

class DebsProfitCellMedallionKeyExtractor : public KeyExtractor<Experiment::DebsChallenge::CompactRide, std::string>
{
public:
	std::string extract_key(const Experiment::DebsChallenge::CompactRide& r) const override;
};

class DebsProfCellCompleteFareKeyExtractor : public KeyExtractor<std::pair<std::string, std::vector<float>>, std::string>
{
public:
	std::string extract_key(const std::pair<std::string, std::vector<float>>& p) const override;
};

class DebsProfCellDropoffCellKeyExtractor : public KeyExtractor<std::pair<std::string, std::pair<std::string, std::time_t>>, std::string>
{
public:
	std::string extract_key(const std::pair<std::string, std::pair<std::string, std::time_t>>& p) const override;
};
#endif //!DEBS_KEY_EXTRACTOR_H_

inline std::string DebsFrequentRideKeyExtractor::extract_key(const Experiment::DebsChallenge::CompactRide& r) const
{
	std::stringstream str_stream;
	str_stream << static_cast<unsigned short>(r.pickup_cell.first) << "." << static_cast<unsigned short>(r.pickup_cell.second) << "-" <<
		static_cast<unsigned short>(r.dropoff_cell.first) << "." << static_cast<unsigned short>(r.dropoff_cell.second);
	return str_stream.str();
}

inline std::string DebsProfitCellMedallionKeyExtractor::extract_key(const Experiment::DebsChallenge::CompactRide& r) const
{
	char med_buffer[33];
	memcpy(med_buffer, r.medallion, 32 * sizeof(char));
	med_buffer[32] = '\0';
	std::string key = med_buffer;
	return key;
}

inline std::string DebsProfCellCompleteFareKeyExtractor::extract_key(const std::pair<std::string, std::vector<float>>& p) const
{
	return p.first;
}

inline std::string DebsProfCellDropoffCellKeyExtractor::extract_key(const std::pair<std::string, std::pair<std::string, std::time_t>>& p) const
{
	return p.second.first;
}