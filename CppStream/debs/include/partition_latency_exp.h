#pragma once
#include <cmath>
#include <iostream>
#include <vector>

#ifndef PARTITIONER_H_
#include "partitioner.h"
#endif  // !PARTITIONER_H_

#ifndef ROUND_ROBIN_PARTITIONER_H_
#include "round_robin_partitioner.h"
#endif  // !ROUND_ROBIN_PARTITIONER_H_

#ifndef HASH_FLD_PARTITIONER_H_
#include "hash_fld_partitioner.h"
#endif  // !HASH_FLD_PARTITIONER_H_

#ifndef PKG_PARTITIONER_H_
#include "pkg_partitioner.h"
#endif  // !PKG_PARTITIONER_H_

#ifndef CA_PARTITION_LIB_H_
#include "ca_partition_lib.h"
#endif  // !CA_PARTITION_LIB_H_

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif  // !IMBALANCE_SCORE_AGGR_H_

#ifndef DEBS_QUERY_LIB_H_
#include "../include/debs_query_lib.h"
#endif  // !DEBS_QUERY_LIB_H_

#ifndef STREAM_PARTITION_LIB_UTILS_
#include "stat_util.h"
#endif  // !STREAM_PARTITION_LIB_UTILS_

#ifndef STREAM_PARTITION_LIB_NAME_UTIL_
#include "name_util.h"
#endif

#ifndef PARTITION_LATENCY_EXP_
#define PARTITION_LATENCY_EXP_
namespace Experiment {
namespace DebsChallenge {
class PartitionLatency {
 public:
  static void measure_latency(
      size_t task_number, const std::vector<CompactRide>& frequent_ride_table) {
    CardinalityAwarePolicy ca_policy;
    LoadAwarePolicy la_policy;
    std::vector<uint16_t> tasks(task_number, 0);
    for (uint16_t i = 0; i < task_number; i++) {
      tasks[i] = i;
    }
    tasks.shrink_to_fit();
    CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
    CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
    std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)),
        fld(new HashFieldPartitioner(tasks)),
        pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy,
                                             naive_estimator)),
        ca_naive(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy,
                                                  naive_estimator)),
        ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)),
        ca_hll(
            new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
        ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)),
        la_naive(new CaPartitionLib::CA<uint64_t>(tasks, la_policy,
                                                  naive_estimator)),
        la_hll(
            new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator));
    std::cout << "tasks: " << task_number << "\n";
    std::cout << "name,mean,median,90 %ile,99 %ile, sum\n";
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::shuffle_partitioner(), rrg,
        frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::field_partitioner(), fld,
        frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::partial_key_partitioner(), pkg,
        frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::cardinality_naive_partitioner(),
        ca_naive, frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::affinity_naive_partitioner(),
        ca_aff_naive, frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::cardinality_hip_partitioner(), ca_hll,
        frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::affinity_hip_partitioner(), ca_aff_hll,
        frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::load_naive_partitioner(), la_naive,
        frequent_ride_table);
    debs_frequent_route_partition_latency(
        StreamPartitionLib::Name::Util::load_hip_partitioner(), la_hll,
        frequent_ride_table);
  }
  static void debs_frequent_route_partition_latency(
      const std::string& partitioner_name,
      std::unique_ptr<Partitioner>& partitioner,
      const std::vector<CompactRide>& frequent_ride_table) {
    uint64_t sum = 0;
    const int debs_window_size_in_sec = 1800;  // 30 minutes
    std::vector<double> durations;
    std::vector<double> mean_durations;
    std::vector<double> median_durations;
    std::vector<double> ninety_pile_durations;
    std::vector<double> ninetynine_pile_durations;
    std::vector<CompactRide> window_buffer;
    for (size_t iteration = 0; iteration < 5; ++iteration) {
      std::time_t window_start = frequent_ride_table.size() > 0
                                     ? frequent_ride_table[0].dropoff_datetime
                                     : 0;
      for (auto it = frequent_ride_table.cbegin();
           it != frequent_ride_table.cend(); ++it) {
        double sec_diff = std::difftime(it->dropoff_datetime, window_start);
        if (sec_diff >= debs_window_size_in_sec) {
          // send to partitioner and time how much time it took to partition
          partitioner->init();
          std::chrono::system_clock::time_point start =
              std::chrono::system_clock::now();
          for (auto window_it = window_buffer.cbegin();
               window_it != window_buffer.cend(); ++window_it) {
            std::stringstream str_stream;
            str_stream << (unsigned short)it->pickup_cell.first << "."
                       << (unsigned short)it->pickup_cell.second << "-"
                       << (unsigned short)it->dropoff_cell.first << "."
                       << (unsigned short)it->dropoff_cell.second;
            std::string key = str_stream.str();
            unsigned int task =
                partitioner->partition_next(key.c_str(), strlen(key.c_str()));
            sum += task;
          }
          std::chrono::system_clock::time_point end =
              std::chrono::system_clock::now();
          std::chrono::duration<double, std::milli> execution_time =
              end - start;
          durations.push_back(float(execution_time.count()));
          // clean buffer and move window
          window_start = it->dropoff_datetime;
          window_buffer.clear();
          window_buffer.push_back(Experiment::DebsChallenge::CompactRide(*it));
        } else {
          window_buffer.push_back(Experiment::DebsChallenge::CompactRide(*it));
        }
      }
      std::sort(durations.begin(), durations.end());
      ninety_pile_durations.push_back(
          StreamPartitionLib::Stats::Util::get_percentile(durations, 0.9));
      ninetynine_pile_durations.push_back(
          StreamPartitionLib::Stats::Util::get_percentile(durations, 0.99));
      median_durations.push_back(
          StreamPartitionLib::Stats::Util::get_percentile(durations, 0.5));
      mean_durations.push_back(
          StreamPartitionLib::Stats::Util::get_mean(durations));
      durations.clear();
      window_buffer.clear();
    }
    auto max_it =
        std::max_element(mean_durations.begin(), mean_durations.end());
    mean_durations.erase(max_it);
    auto min_it =
        std::min_element(mean_durations.begin(), mean_durations.end());
    mean_durations.erase(min_it);
    max_it = std::max_element(median_durations.begin(), median_durations.end());
    median_durations.erase(max_it);
    min_it = std::min_element(median_durations.begin(), median_durations.end());
    median_durations.erase(min_it);
    max_it = std::max_element(ninety_pile_durations.begin(),
                              ninety_pile_durations.end());
    ninety_pile_durations.erase(max_it);
    min_it = std::min_element(ninety_pile_durations.begin(),
                              ninety_pile_durations.end());
    ninety_pile_durations.erase(min_it);
    max_it = std::max_element(ninetynine_pile_durations.begin(),
                              ninetynine_pile_durations.end());
    ninetynine_pile_durations.erase(max_it);
    min_it = std::min_element(ninetynine_pile_durations.begin(),
                              ninetynine_pile_durations.end());
    ninetynine_pile_durations.erase(min_it);
    float mean_latency =
        StreamPartitionLib::Stats::Util::get_mean(mean_durations);
    float median_latency =
        StreamPartitionLib::Stats::Util::get_mean(median_durations);
    float ninety_ile_latency =
        StreamPartitionLib::Stats::Util::get_mean(ninety_pile_durations);
    float ninetynine_ile_latency =
        StreamPartitionLib::Stats::Util::get_mean(ninetynine_pile_durations);
    std::cout << partitioner_name << ", " << mean_latency << ", "
              << median_latency << ", " << ninety_ile_latency << ", "
              << ninetynine_ile_latency << ", " << sum << "\n";
  }
};
}
}
#endif
