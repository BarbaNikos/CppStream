#pragma once
#include <iostream>
#include <vector>
#include <cmath>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif // !PARTITIONER_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#include "round_robin_partitioner.h"
#endif // !ROUND_ROBIN_PARTITIONER_H_

#ifndef HASH_FLD_PARTITIONER_H_
#include "hash_fld_partitioner.h"
#endif // !HASH_FLD_PARTITIONER_H_

#ifndef PKG_PARTITIONER_H_
#include "pkg_partitioner.h"
#endif // !PKG_PARTITIONER_H_

#ifndef CA_PARTITION_LIB_H_
#include "ca_partition_lib.h"
#endif // !CA_PARTITION_LIB_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif // !IMBALANCE_SCORE_AGGR_H_

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif // !DEBS_QUERY_LIB_H_

#ifndef PARTITION_LATENCY_EXP_
#define PARTITION_LATENCY_EXP_
class PartitionLatency {
private:
	static float get_percentile_duration(const std::vector<double>& sorted_durations, float k);

public:
	static float get_percentile(const std::vector<double>& v, float k);
	static float get_mean(const std::vector<double>& durations);
	void measure_latency(unsigned int task_number, const std::vector<Experiment::DebsChallenge::CompactRide>& frequent_ride_table);
	void debs_frequent_route_partition_latency(const std::string& partitioner_name, Partitioner* partitioner, 
		const std::vector<Experiment::DebsChallenge::CompactRide>& frequent_ride_table);
};
#endif // !PARTITION_LATENCY_EXP_
