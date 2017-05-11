#pragma once
#include <fstream>

#ifndef GOOGLE_CLUSTER_MONITOR_QUERY_H_
#include "../include/google_cluster_monitor_query.h"
#endif  // GOOGLE_CLUSTER_MONITOR_QUERY_H_

#ifndef STREAM_PARTITION_LIB_UTILS_
#include "stat_util.h"
#endif  // !STREAM_PARTITION_LIB_UTILS_

#ifndef STREAM_PARTITION_LIB_NAME_UTIL_
#include "name_util.h"
#endif

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::update(
    const task_event& task_event) {
  auto it = result.find(task_event.scheduling_class);
  if (it != result.end())
    it->second.total_cpu += task_event.cpu_request;
  else
    result[task_event.scheduling_class] =
        cm_one_result(task_event.timestamp, task_event.scheduling_class,
                      task_event.cpu_request);
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryWorker::submit(
    std::vector<cm_one_result>& buffer) const {
  for (auto it = result.cbegin(); it != result.cend(); ++it)
    buffer.push_back(it->second);
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryAggregator::
    sort_final_aggregation(const std::vector<cm_one_result>& buffer,
                           std::vector<cm_one_result>& final_result) {
  std::map<int, std::vector<cm_one_result>> result_buffer;
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    if (result_buffer.find(it->total_cpu) == result_buffer.end()) {
      std::vector<cm_one_result> l;
      l.push_back(*it);
      result_buffer[it->total_cpu] = l;
    } else {
      auto c_it = result_buffer.find(it->total_cpu);
      c_it->second.push_back(*it);
      result_buffer[it->total_cpu] = c_it->second;
    }
  }
  for (auto it = result_buffer.crbegin(); it != result_buffer.crend(); ++it) {
    for (auto l_it = it->second.cbegin(); l_it != it->second.cend(); ++l_it) {
      final_result.push_back(*l_it);
      if (final_result.size() >= 10) break;
    }
    if (final_result.size() >= 10) break;
  }
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryAggregator::
    sort_final_aggregation(const std::unordered_map<int, cm_one_result>& buffer,
                           std::vector<cm_one_result>& result) {
  std::map<int, std::vector<cm_one_result>> result_buffer;
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    if (result_buffer.find(it->second.total_cpu) == result_buffer.end()) {
      std::vector<cm_one_result> l;
      l.push_back(it->second);
      result_buffer[it->second.total_cpu] = l;
    } else {
      auto c_it = result_buffer.find(it->second.total_cpu);
      c_it->second.push_back(it->second);
      result_buffer[it->second.total_cpu] = c_it->second;
    }
  }
  for (auto it = result_buffer.crbegin(); it != result_buffer.crend(); ++it) {
    for (auto l_it = it->second.cbegin(); l_it != it->second.cend(); ++l_it) {
      result.push_back(*l_it);
      if (result.size() >= 10) break;
    }
    if (result.size() >= 10) break;
  }
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryAggregator::
    calculate_final_aggregation(
        const std::vector<cm_one_result>& partial_aggregates,
        std::unordered_map<int, cm_one_result>& final_result_buffer) {
  for (auto it = partial_aggregates.cbegin(); it != partial_aggregates.cend();
       ++it) {
    auto cit = final_result_buffer.find(it->category);
    if (cit != final_result_buffer.end()) {
      cit->second.total_cpu += it->total_cpu;
    } else {
      final_result_buffer[it->category] = *it;
    }
  }
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryAggregator::
    write_output_to_file(const std::vector<cm_one_result>& final_result,
                         const std::string& outfile_name) {
  FILE* fd;
  fd = fopen(outfile_name.c_str(), "w");
  for (auto it = final_result.cbegin(); it != final_result.cend(); ++it) {
    auto buffer = std::to_string(it->category) + "," +
                  std::to_string(it->timestamp) + "," +
                  std::to_string(it->total_cpu) + "\n";
    fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
  }
  fflush(fd);
  fclose(fd);
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
    parse_task_events(const std::string& input_file_name,
                      std::vector<task_event>& buffer) {
  std::string line;
  std::ifstream input(input_file_name);
  if (!input.is_open()) {
    std::cout << "GoogleClusterMonitor::TotalCpuPerCategoryPartition::parse_"
                 "task_events(): failed to open file: "
              << input_file_name << ".\n";
    exit(1);
  }
  while (getline(input, line)) {
    task_event t_e;
    try {
      t_e.deserealize(line);
      buffer.push_back(t_e);
    } catch (std::exception& e) {
      std::cout << "Experiment::GoogleClusterMonitor::"
                   "TotalCpuPerCategoryOfflineAggregator::parse_task_events() "
                   "threw an exception: "
                << e.what() << "\n";
    }
  }
  input.close();
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
    parse_task_events_from_directory(const std::string& input_dir_name,
                                     std::vector<task_event>& buffer) {
  size_t number_of_files_scanned = 0;
  const std::string file_suffix = ".csv";
  std::vector<std::string> file_names;
  Util::get_files(file_names, input_dir_name);
  for (auto file_it = file_names.cbegin(); file_it != file_names.cend();
       ++file_it) {
    // check if it is a task_event file
    if (Util::ends_with(*file_it, file_suffix)) {
      parse_task_events(*file_it, buffer);
      number_of_files_scanned++;
    }
  }
  std::cout << "parse_task_events_from_directory():: total files scanned: "
            << number_of_files_scanned << ".\n";
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
    query_simulation(const std::vector<task_event>& buffer,
                     const size_t task_number) {
  std::vector<uint16_t> tasks;
  CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
  CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
  LoadAwarePolicy la_policy;
  CardinalityAwarePolicy ca_policy;
  for (uint16_t i = 0; i < task_number; i++) {
    tasks.push_back(i);
  }
  tasks.shrink_to_fit();
  std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)),
      fld(new HashFieldPartitioner(tasks)),
      pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)),
      ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
      ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)),
      la_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
      la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator));

  std::string sh_file_name =
      "shg_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string fld_file_name =
      "fld_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string pkg_file_name =
      "pkg_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_naive_file_name =
      "ca_naive_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_aff_naive_file_name =
      "ca_aff_naive_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_hll_file_name =
      "ca_hll_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_aff_hll_file_name =
      "ca_aff_hll_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string la_naive_file_name =
      "la_naive_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::string la_hll_file_name =
      "la_hll_google_q1_" + std::to_string(tasks.size()) + ".csv";
  std::cout << "GCM Q1 ***\n";
  std::stringstream info_stream;
  info_stream << "partitioner,task-num,min-exec-msec,max-exec-msec,avg-exec-"
                 "msec,avg-aggr-msec,io-msec,avg-order-msec,imb,key-imb\n";
  std::cout << info_stream.str();
  query_partitioner_simulation(true, buffer, tasks, rrg, "sh", sh_file_name);
  query_partitioner_simulation(true, buffer, tasks, fld, "fld", fld_file_name);
  query_partitioner_simulation(true, buffer, tasks, pkg, "pk", pkg_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_naive, "cn",
                               ca_naive_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_aff_naive, "an",
                               ca_aff_naive_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_hll, "chll",
                               ca_hll_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_aff_hll, "ahll",
                               ca_aff_hll_file_name);
  query_partitioner_simulation(true, buffer, tasks, la_naive, "ln",
                               la_naive_file_name);
  query_partitioner_simulation(true, buffer, tasks, la_hll, "lhll",
                               la_hll_file_name);
}

std::vector<double> Experiment::GoogleClusterMonitor::
    TotalCpuPerCategoryPartition::query_partitioner_simulation(
        const bool write, const std::vector<task_event>& buffer,
        const std::vector<uint16_t>& tasks,
        std::unique_ptr<Partitioner>& partitioner,
        const std::string& partitioner_name,
        const std::string& worker_output_file_name) {
  float class_imbalance, class_key_imbalance;
  // get maximum and minimum running times
  std::vector<double> exec_durations(tasks.size(), 0.0), aggr_durations,
      order_durations;
  double write_output_duration;
  std::vector<cm_one_result> intermediate_buffer;
  std::vector<std::vector<task_event>> worker_input_buffer(
      tasks.size(), std::vector<task_event>());
  // partition tuples
  GCMTaskEventKeyExtractor key_extractor;
  ImbalanceScoreAggr<task_event, int> sch_class_imb_aggregator(tasks.size(),
                                                               key_extractor);
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    uint16_t task = partitioner->partition_next(&(it->scheduling_class),
                                                sizeof(it->scheduling_class));
    if (task < worker_input_buffer.size()) {
      worker_input_buffer[task].push_back(*it);
      sch_class_imb_aggregator.incremental_measure_score(task, *it);
    }
  }
  class_imbalance = sch_class_imb_aggregator.imbalance();
  class_key_imbalance = sch_class_imb_aggregator.cardinality_imbalance();
  // start gathering results
  for (size_t i = 0; i < tasks.size(); ++i)
    worker_input_buffer[i].shrink_to_fit();
  worker_input_buffer.shrink_to_fit();
  for (size_t i = 0; i < tasks.size(); ++i) {
    std::vector<double> durations;
    for (size_t run = 0; run < 7; ++run) {
      TotalCpuPerCategoryWorker worker;
      std::vector<cm_one_result> intermediate_buffer_copy(intermediate_buffer);
      // TIME CRITICAL - START
      std::chrono::system_clock::time_point start =
          std::chrono::system_clock::now();
      for (auto it = worker_input_buffer[i].begin();
           it != worker_input_buffer[i].end(); ++it)
        worker.update(*it);
      worker.submit(intermediate_buffer_copy);
      std::chrono::system_clock::time_point end =
          std::chrono::system_clock::now();
      // TIME CRITICAL - END
      std::chrono::duration<double, std::micro> execution_time = end - start;
      durations.push_back(execution_time.count());
      if (run >= 6) intermediate_buffer = intermediate_buffer_copy;
    }
    auto min_it = std::min_element(durations.begin(), durations.end());
    durations.erase(min_it);
    auto max_it = std::max_element(durations.begin(), durations.end());
    durations.erase(max_it);
    exec_durations[i] =
        std::accumulate(durations.begin(), durations.end(), 0.0) /
        durations.size();
    durations.clear();
    worker_input_buffer[i].clear();
  }
  for (size_t aggr_run = 0; aggr_run < 7; ++aggr_run) {
    TotalCpuPerCategoryAggregator aggregator;
    std::vector<cm_one_result> final_result;
    std::unordered_map<int, cm_one_result> full_result_buffer;
    // TIME CRITICAL - START
    if (StreamPartitionLib::Name::Util::single_choice_partitioner(
            partitioner_name) == false) {
      std::chrono::system_clock::time_point start =
          std::chrono::system_clock::now();
      aggregator.calculate_final_aggregation(intermediate_buffer,
                                             full_result_buffer);
      std::chrono::system_clock::time_point end =
          std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> aggr_time = end - start;
      aggr_durations.push_back(aggr_time.count());
      start = std::chrono::system_clock::now();
      aggregator.sort_final_aggregation(full_result_buffer, final_result);
      end = std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> order_time = end - start;
      order_durations.push_back(order_time.count());
    } else {
      std::chrono::system_clock::time_point start =
          std::chrono::system_clock::now();
      std::chrono::system_clock::time_point end =
          std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> aggr_time = end - start;
      aggr_durations.push_back(aggr_time.count());
      start = std::chrono::system_clock::now();
      aggregator.sort_final_aggregation(intermediate_buffer, final_result);
      end = std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> order_time = end - start;
      order_durations.push_back(order_time.count());
    }
    // TIME CRITICAL - END
    if (aggr_run >= 6) {
      std::chrono::system_clock::time_point output_start =
          std::chrono::system_clock::now();
      // aggregator.write_output_to_file(final_result, worker_output_file_name);
      std::chrono::system_clock::time_point output_end =
          std::chrono::system_clock::now();
      write_output_duration = (output_end - output_start).count();
    }
  }
  intermediate_buffer.clear();
  auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
  aggr_durations.erase(min_it);
  auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
  aggr_durations.erase(max_it);
  double mean_aggr_duration =
      std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) /
      aggr_durations.size();
  aggr_durations.clear();
  min_it = std::min_element(order_durations.begin(), order_durations.end());
  order_durations.erase(min_it);
  max_it = std::max_element(order_durations.begin(), order_durations.end());
  order_durations.erase(max_it);
  double mean_order_time =
      std::accumulate(order_durations.begin(), order_durations.end(), 0.0) /
      order_durations.size();
  if (write) {
    std::stringstream result_stream;
    result_stream
        << partitioner_name << "," << tasks.size() << ","
        << *std::min_element(exec_durations.begin(), exec_durations.end())
        << ","
        << *std::max_element(exec_durations.begin(), exec_durations.end())
        << ","
        << (std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) /
            exec_durations.size())
        << "," << mean_aggr_duration << "," << write_output_duration << ","
        << mean_order_time << "," << class_imbalance << ","
        << class_key_imbalance << "\n";
    std::cout << result_stream.str();
  }
  std::vector<double> duration_results;
  duration_results.push_back(
      *std::min_element(exec_durations.begin(), exec_durations.end()));
  duration_results.push_back(
      *std::max_element(exec_durations.begin(), exec_durations.end()));
  duration_results.push_back(
      (std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) /
       exec_durations.size()));
  duration_results.push_back(mean_aggr_duration);
  duration_results.push_back(mean_order_time);
  duration_results.push_back(class_imbalance);
  duration_results.push_back(class_key_imbalance);
  return duration_results;
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
    query_window_simulation(const std::vector<task_event>& buffer,
                            const size_t task_number) {
  std::vector<uint16_t> tasks;
  for (uint16_t i = 0; i < task_number; i++) {
    tasks.push_back(i);
  }
  CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
  CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
  LoadAwarePolicy la_policy;
  CardinalityAwarePolicy ca_policy;
  std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)),
      fld(new HashFieldPartitioner(tasks)),
      pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)),
      ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
      ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)),
      la_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
      la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator)),
      mpk(new MultiPkPartitioner(tasks)),
      man(new CaPartitionLib::MultiAN<uint64_t>(tasks, naive_estimator)),
      can(new CaPartitionLib::CountAN<uint64_t>(tasks, naive_estimator)),
      mcan(new CaPartitionLib::MultiCountAN<uint64_t>(tasks, naive_estimator));
  std::cout << "*** GCM Q1 ***\n";
  std::stringstream info_stream;
  info_stream << "name,tasks,min-exec,max-exec,99ile-max-exec,avg-exec,aggr,"
                 "99ile-aggr,order,imb,key-imb,window-size\n";
  std::cout << info_stream.str();
  query_window_partitioner_simulation(
      buffer, tasks, rrg,
      StreamPartitionLib::Name::Util::shuffle_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, fld, StreamPartitionLib::Name::Util::field_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, pkg,
      StreamPartitionLib::Name::Util::partial_key_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_naive,
      StreamPartitionLib::Name::Util::cardinality_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_aff_naive,
      StreamPartitionLib::Name::Util::affinity_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_hll,
      StreamPartitionLib::Name::Util::cardinality_hip_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_aff_hll,
      StreamPartitionLib::Name::Util::affinity_hip_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, la_naive,
      StreamPartitionLib::Name::Util::load_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, la_hll,
      StreamPartitionLib::Name::Util::load_hip_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, man,
      StreamPartitionLib::Name::Util::multi_affinity_naive_partitioner(5));
  query_window_partitioner_simulation(
      buffer, tasks, mpk,
      StreamPartitionLib::Name::Util::multi_partial_key_partitioner(5));
  query_window_partitioner_simulation(
      buffer, tasks, can,
      StreamPartitionLib::Name::Util::affinity_count_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, mcan,
      StreamPartitionLib::Name::Util::multiple_affinity_count_naive_partitioner(
          5));
}

void Experiment::GoogleClusterMonitor::TotalCpuPerCategoryPartition::
    query_window_partitioner_simulation(
        const std::vector<task_event>& buffer,
        const std::vector<uint16_t>& tasks,
        std::unique_ptr<Partitioner>& partitioner,
        const std::string& partitioner_name) {
  const long window_size_in_msec = 60 * 1000;
  std::vector<double> min_parallel_durations, max_parallel_durations,
      mean_parallel_durations, aggr_durations, order_durations, imbalances,
      k_imbalances, window_sizes;
  std::vector<task_event> window;
  long window_start;
  if (buffer.size() > 0)
    window_start = buffer[0].timestamp;
  else
    return;
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    task_event t_event(*it);
    double sec_diff = std::abs(t_event.timestamp - window_start);
    if (sec_diff >= window_size_in_msec) {
      partitioner->init();
      // process window
      auto window_results = query_partitioner_simulation(
          false, window, tasks, partitioner, partitioner_name, "result");
      // accumulate results
      min_parallel_durations.push_back(window_results[0]);
      max_parallel_durations.push_back(window_results[1]);
      mean_parallel_durations.push_back(window_results[2]);
      aggr_durations.push_back(window_results[3]);
      order_durations.push_back(window_results[4]);
      imbalances.push_back(window_results[5]);
      k_imbalances.push_back(window_results[6]);
      window_sizes.push_back(window.size());
      // progress window
      window_start = t_event.timestamp;
      window.clear();
      window.push_back(t_event);
    } else {
      window.push_back(t_event);
    }
  }
  if (window.size() > 0) {
    partitioner->init();
    // process window
    auto window_results = query_partitioner_simulation(
        false, window, tasks, partitioner, partitioner_name, "result");
    // accumulate results
    min_parallel_durations.push_back(window_results[0]);
    max_parallel_durations.push_back(window_results[1]);
    mean_parallel_durations.push_back(window_results[2]);
    aggr_durations.push_back(window_results[3]);
    order_durations.push_back(window_results[4]);
    imbalances.push_back(window_results[5]);
    k_imbalances.push_back(window_results[6]);
    window_sizes.push_back(window.size());
    window.clear();
    window_results.clear();
  }
  double mean_min_parallel_duration =
      StreamPartitionLib::Stats::Util::get_mean(min_parallel_durations);
  double mean_max_parallel_duration =
      StreamPartitionLib::Stats::Util::get_mean(max_parallel_durations);
  double ninetynile_max_parallel_duration =
      StreamPartitionLib::Stats::Util::get_percentile(max_parallel_durations,
                                                      0.99);
  double mean_mean_parallel_duration =
      StreamPartitionLib::Stats::Util::get_mean(mean_parallel_durations);
  double mean_aggr_duration =
      StreamPartitionLib::Stats::Util::get_mean(aggr_durations);
  double ninetynile_aggr_duration =
      StreamPartitionLib::Stats::Util::get_percentile(aggr_durations, 0.99);
  double mean_order_duration =
      StreamPartitionLib::Stats::Util::get_mean(order_durations);
  double mean_imbalance = StreamPartitionLib::Stats::Util::get_mean(imbalances);
  double mean_key_imbalance =
      StreamPartitionLib::Stats::Util::get_mean(k_imbalances);
  double mean_window_size =
      StreamPartitionLib::Stats::Util::get_mean(window_sizes);

  std::stringstream result_stream;
  result_stream << partitioner_name << "," << tasks.size() << ","
                << mean_min_parallel_duration << ","
                << mean_max_parallel_duration << ","
                << ninetynile_max_parallel_duration << ","
                << mean_mean_parallel_duration << "," << mean_aggr_duration
                << "," << ninetynile_aggr_duration << "," << mean_order_duration
                << "," << mean_imbalance << "," << mean_key_imbalance << ","
                << mean_window_size << "\n";
  std::cout << result_stream.str();
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::update(
    const task_event& task_event) {
  auto it = this->result.find(task_event.job_id);
  if (it != result.end()) {
    it->second.sum_cpu += task_event.cpu_request;
    it->second.count += 1;
  } else {
    result[task_event.job_id] = cm_two_result(
        task_event.timestamp, task_event.job_id, task_event.cpu_request, 1);
  }
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdWorker::submit(
    std::vector<cm_two_result>& buffer) const {
  for (auto it = result.cbegin(); it != result.cend(); ++it)
    buffer.push_back(it->second);
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdAggregator::
    order_final_result(const std::vector<cm_two_result>& buffer,
                       std::vector<cm_two_result>& final_result) {
  std::map<double, std::vector<cm_two_result>> sorted_buffer;
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    double mean_value = double(it->sum_cpu) / double(it->count);
    if (sorted_buffer.find(mean_value) == sorted_buffer.end()) {
      std::vector<cm_two_result> l;
      l.push_back(*it);
      sorted_buffer[mean_value] = l;
    } else {
      auto l_it = sorted_buffer[mean_value];
      l_it.push_back(*it);
      sorted_buffer[mean_value] = l_it;
    }
  }
  for (auto it = sorted_buffer.crbegin(); it != sorted_buffer.crend(); ++it) {
    for (auto l_it = it->second.cbegin(); l_it != it->second.cend(); ++l_it) {
      cm_two_result c(*l_it);
      c.mean_cpu = c.sum_cpu / c.count;
      final_result.push_back(c);
    }
    if (final_result.size() >= 10) {
      break;
    }
  }
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdAggregator::
    order_final_result(const std::unordered_map<long, cm_two_result>& buffer,
                       std::vector<cm_two_result>& final_result) {
  std::map<double, std::vector<cm_two_result>> sorted_buffer;
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    double mean_value = double(it->second.sum_cpu) / double(it->second.count);
    if (sorted_buffer.find(mean_value) == sorted_buffer.end()) {
      std::vector<cm_two_result> l;
      l.push_back(it->second);
      sorted_buffer[mean_value] = l;
    } else {
      auto l_it = sorted_buffer[mean_value];
      l_it.push_back(it->second);
      sorted_buffer[mean_value] = l_it;
    }
  }
  for (auto it = sorted_buffer.crbegin(); it != sorted_buffer.crend(); ++it) {
    for (auto l_it = it->second.cbegin(); l_it != it->second.cend(); ++l_it) {
      cm_two_result c(*l_it);
      c.mean_cpu = c.sum_cpu / c.count;
      final_result.push_back(c);
    }
    if (final_result.size() >= 10) {
      break;
    }
  }
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdAggregator::
    calculate_final_aggregation(
        const std::vector<cm_two_result>& partial_aggregates,
        std::unordered_map<long, cm_two_result>& final_result) {
  for (auto it = partial_aggregates.cbegin(); it != partial_aggregates.cend();
       ++it) {
    auto cit = final_result.find(it->job_id);
    if (cit != final_result.end()) {
      cit->second.sum_cpu += it->sum_cpu;
      cit->second.count += it->count;
    } else {
      final_result[it->job_id] = *it;
    }
  }
  for (auto it = final_result.begin(); it != final_result.end(); ++it)
    it->second.mean_cpu = it->second.sum_cpu / it->second.count;
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdAggregator::
    write_output_to_file(const std::vector<cm_two_result>& final_result,
                         const std::string& outfile_name) {
  FILE* fd;
  fd = fopen(outfile_name.c_str(), "w");
  for (auto it = final_result.cbegin(); it != final_result.cend(); ++it) {
    auto buffer = std::to_string(it->job_id) + "," +
                  std::to_string(it->sum_cpu / it->count) + "\n";
    fwrite(buffer.c_str(), sizeof(char), buffer.length(), fd);
  }
  fflush(fd);
  fclose(fd);
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
    query_simulation(const std::vector<task_event>& buffer,
                     const size_t task_number) {
  CardinalityAwarePolicy ca_policy;
  LoadAwarePolicy la_policy;
  std::vector<uint16_t> tasks;
  for (uint16_t i = 0; i < task_number; i++) {
    tasks.push_back(i);
  }
  tasks.shrink_to_fit();
  CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
  CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
  std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)),
      fld(new HashFieldPartitioner(tasks)),
      pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)),
      ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
      ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)),
      la_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
      la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator));
  std::cout << "GCM Q2 ***\n";
  std::stringstream info_stream;
  info_stream << "partitioner,task-num,min-exec-msec,max-exec-msec,avg-exec-"
                 "msec,avg-aggr-msec,avg-order-msec,io-msec\n";
  std::cout << info_stream.str();
  std::string sh_file_name =
      "shg_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string fld_file_name =
      "fld_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string pkg_file_name =
      "pkg_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_naive_file_name =
      "ca_naive_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_aff_naive_file_name =
      "ca_aff_naive_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_hll_file_name =
      "ca_hll_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string ca_aff_hll_file_name =
      "ca_aff_hll_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string la_naive_file_name =
      "la_naive_google_q2_" + std::to_string(tasks.size()) + ".csv";
  std::string la_hll_file_name =
      "la_hll_google_q2_" + std::to_string(tasks.size()) + ".csv";

  query_partitioner_simulation(true, buffer, tasks, rrg, "sh", sh_file_name);
  query_partitioner_simulation(true, buffer, tasks, fld, "fld", fld_file_name);
  query_partitioner_simulation(true, buffer, tasks, pkg, "pk", pkg_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_naive, "cn",
                               ca_naive_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_naive, "an",
                               ca_aff_naive_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_hll, "chll",
                               ca_hll_file_name);
  query_partitioner_simulation(true, buffer, tasks, ca_aff_hll, "ahll",
                               ca_aff_hll_file_name);
  query_partitioner_simulation(true, buffer, tasks, la_naive, "ln",
                               la_naive_file_name);
  query_partitioner_simulation(true, buffer, tasks, la_hll, "lhll",
                               la_hll_file_name);
}

std::vector<double> Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
    query_partitioner_simulation(bool write,
                                 const std::vector<task_event>& buffer,
                                 const std::vector<uint16_t>& tasks,
                                 std::unique_ptr<Partitioner>& partitioner,
                                 const std::string& partitioner_name,
                                 const std::string& worker_output_file_name) {
  // get maximum and minimum running times
  std::vector<double> exec_durations, aggr_durations, order_durations;
  double aggr_duration, write_output_duration;
  std::vector<cm_two_result> intermediate_buffer;
  std::vector<std::vector<task_event>> worker_input_buffer(
      tasks.size(), std::vector<task_event>());
  // partition tuples
  GCMTaskEventJobIdExtractor key_extractor;
  ImbalanceScoreAggr<task_event, long> job_id_imb_aggregator(tasks.size(),
                                                             key_extractor);
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    uint16_t task = partitioner->partition_next(&(it->job_id), sizeof(long));
    if (task < worker_input_buffer.size()) {
      worker_input_buffer[task].push_back(*it);
      job_id_imb_aggregator.incremental_measure_score(task, *it);
    }
  }
  for (size_t i = 0; i < tasks.size(); ++i)
    worker_input_buffer[i].shrink_to_fit();
  worker_input_buffer.shrink_to_fit();
  for (size_t i = 0; i < tasks.size(); ++i) {
    std::vector<double> durations;
    for (size_t run = 0; run < 7; ++run) {
      MeanCpuPerJobIdWorker worker;
      std::vector<cm_two_result> intermediate_buffer_copy(intermediate_buffer);
      // TIME CRITICAL - START
      std::chrono::system_clock::time_point start =
          std::chrono::system_clock::now();
      for (auto it = worker_input_buffer[i].begin();
           it != worker_input_buffer[i].end(); ++it)
        worker.update(*it);
      worker.submit(intermediate_buffer_copy);
      std::chrono::system_clock::time_point end =
          std::chrono::system_clock::now();
      // TIME CRITICAL - END
      std::chrono::duration<double, std::micro> exec_time = end - start;
      durations.push_back(exec_time.count());
      if (run >= 6) intermediate_buffer = intermediate_buffer_copy;
    }
    auto min_it = std::min_element(durations.begin(), durations.end());
    durations.erase(min_it);
    auto max_it = std::max_element(durations.begin(), durations.end());
    durations.erase(max_it);
    worker_input_buffer[i].clear();
    exec_durations.push_back(
        std::accumulate(durations.begin(), durations.end(), 0.0) /
        durations.size());
  }
  worker_input_buffer.clear();
  for (size_t aggr_run = 0; aggr_run < 7; ++aggr_run) {
    std::vector<cm_two_result> final_result;
    std::unordered_map<long, cm_two_result> full_result;
    MeanCpuPerJobIdAggregator aggregator;
    // TIME CRITICAL - START
    if (StreamPartitionLib::Name::Util::single_choice_partitioner(
            partitioner_name) == false) {
      std::chrono::system_clock::time_point start =
          std::chrono::system_clock::now();
      aggregator.calculate_final_aggregation(intermediate_buffer, full_result);
      std::chrono::system_clock::time_point end =
          std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> aggr_time = end - start;
      aggr_durations.push_back(aggr_time.count());
      start = std::chrono::system_clock::now();
      aggregator.order_final_result(full_result, final_result);
      end = std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> order_time = end - start;
      order_durations.push_back(order_time.count());
    } else {
      std::chrono::system_clock::time_point start =
          std::chrono::system_clock::now();
      std::chrono::system_clock::time_point end =
          std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> aggr_time = end - start;
      aggr_durations.push_back(aggr_time.count());
      start = std::chrono::system_clock::now();
      aggregator.order_final_result(intermediate_buffer, final_result);
      end = std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> order_time = end - start;
      order_durations.push_back(order_time.count());
    }
    // TIME CRITICAL - END
    if (aggr_run >= 6) {
      std::chrono::system_clock::time_point output_start =
          std::chrono::system_clock::now();
      // aggregator.write_output_to_file(final_result, worker_output_file_name);
      std::chrono::system_clock::time_point output_end =
          std::chrono::system_clock::now();
      std::chrono::duration<double, std::micro> io_time =
          output_end - output_start;
      write_output_duration = io_time.count();
    }
  }
  auto min_it = std::min_element(aggr_durations.begin(), aggr_durations.end());
  aggr_durations.erase(min_it);
  auto max_it = std::max_element(aggr_durations.begin(), aggr_durations.end());
  aggr_durations.erase(max_it);
  aggr_duration =
      std::accumulate(aggr_durations.begin(), aggr_durations.end(), 0.0) /
      aggr_durations.size();
  intermediate_buffer.clear();
  min_it = std::min_element(order_durations.begin(), order_durations.end());
  order_durations.erase(min_it);
  max_it = std::max_element(order_durations.begin(), order_durations.end());
  order_durations.erase(max_it);
  double mean_order_time =
      std::accumulate(order_durations.begin(), order_durations.end(), 0.0) /
      order_durations.size();

  if (write) {
    std::stringstream result_stream;
    result_stream
        << partitioner_name << "," << tasks.size() << ","
        << *std::min_element(exec_durations.begin(), exec_durations.end())
        << ","
        << *std::max_element(exec_durations.begin(), exec_durations.end())
        << ","
        << (std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) /
            exec_durations.size())
        << "," << aggr_duration << "," << mean_order_time << ","
        << write_output_duration << "\n";
    std::cout << result_stream.str();
  }
  std::vector<double> result_durations;
  result_durations.push_back(
      *std::min_element(exec_durations.begin(), exec_durations.end()));
  result_durations.push_back(
      *std::max_element(exec_durations.begin(), exec_durations.end()));
  result_durations.push_back(
      (std::accumulate(exec_durations.begin(), exec_durations.end(), 0.0) /
       exec_durations.size()));
  result_durations.push_back(aggr_duration);
  result_durations.push_back(mean_order_time);
  result_durations.push_back(job_id_imb_aggregator.imbalance());
  result_durations.push_back(job_id_imb_aggregator.cardinality_imbalance());
  return result_durations;
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
    query_window_simulation(const std::vector<task_event>& buffer,
                            const size_t task_number) {
  std::vector<uint16_t> tasks;
  for (uint16_t i = 0; i < task_number; i++) {
    tasks.push_back(i);
  }
  CardinalityEstimator::naive_cardinality_estimator<uint64_t> naive_estimator;
  CardinalityEstimator::hip_cardinality_estimator<uint64_t> hip_estimator;
  LoadAwarePolicy la_policy;
  CardinalityAwarePolicy ca_policy;
  std::unique_ptr<Partitioner> rrg(new RoundRobinPartitioner(tasks)),
      fld(new HashFieldPartitioner(tasks)),
      pkg(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, naive_estimator)),
      ca_aff_naive(new CaPartitionLib::AN<uint64_t>(tasks, naive_estimator)),
      ca_hll(new CaPartitionLib::CA<uint64_t>(tasks, ca_policy, hip_estimator)),
      ca_aff_hll(new CaPartitionLib::AN<uint64_t>(tasks, hip_estimator)),
      la_naive(
          new CaPartitionLib::CA<uint64_t>(tasks, la_policy, naive_estimator)),
      la_hll(new CaPartitionLib::CA<uint64_t>(tasks, la_policy, hip_estimator)),
      mpk(new MultiPkPartitioner(tasks)),
      man(new CaPartitionLib::MultiAN<uint64_t>(tasks, naive_estimator)),
      can(new CaPartitionLib::CountAN<uint64_t>(tasks, naive_estimator)),
      mcan(new CaPartitionLib::MultiCountAN<uint64_t>(tasks, naive_estimator));
  std::cout << "*** GCM Q2 ***\n";
  std::stringstream info_stream;
  info_stream << "name,tasks,min-exec,max-exec,99ile-max-exec,avg-exec,aggr,"
                 "99ile-aggr,order,imb,key-imb,window-size\n";
  std::cout << info_stream.str();
  query_window_partitioner_simulation(
      buffer, tasks, rrg,
      StreamPartitionLib::Name::Util::shuffle_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, fld, StreamPartitionLib::Name::Util::field_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, pkg,
      StreamPartitionLib::Name::Util::partial_key_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_naive,
      StreamPartitionLib::Name::Util::cardinality_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_aff_naive,
      StreamPartitionLib::Name::Util::affinity_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_hll,
      StreamPartitionLib::Name::Util::cardinality_hip_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, ca_aff_hll,
      StreamPartitionLib::Name::Util::affinity_hip_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, la_naive,
      StreamPartitionLib::Name::Util::load_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, la_hll,
      StreamPartitionLib::Name::Util::load_hip_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, man,
      StreamPartitionLib::Name::Util::multi_affinity_naive_partitioner(5));
  query_window_partitioner_simulation(
      buffer, tasks, mpk,
      StreamPartitionLib::Name::Util::multi_partial_key_partitioner(5));
  query_window_partitioner_simulation(
      buffer, tasks, can,
      StreamPartitionLib::Name::Util::affinity_count_naive_partitioner());
  query_window_partitioner_simulation(
      buffer, tasks, mcan,
      StreamPartitionLib::Name::Util::multiple_affinity_count_naive_partitioner(
          5));
}

void Experiment::GoogleClusterMonitor::MeanCpuPerJobIdPartition::
    query_window_partitioner_simulation(
        const std::vector<task_event>& buffer,
        const std::vector<uint16_t>& tasks,
        std::unique_ptr<Partitioner>& partitioner,
        const std::string& partitioner_name) {
  const long window_size_in_msec = 60 * 1000;
  std::vector<double> min_parallel_durations, max_parallel_durations,
      mean_parallel_durations, aggr_durations, order_durations, imbalances,
      k_imbalances, window_sizes;
  std::vector<task_event> window;
  long window_start;
  if (buffer.size() > 0)
    window_start = buffer[0].timestamp;
  else
    return;
  for (auto it = buffer.cbegin(); it != buffer.cend(); ++it) {
    task_event t_event(*it);
    double sec_diff = std::abs(t_event.timestamp - window_start);
    if (sec_diff >= window_size_in_msec) {
      partitioner->init();
      // process window
      auto window_results = query_partitioner_simulation(
          false, window, tasks, partitioner, partitioner_name, "result");
      // accumulate results
      min_parallel_durations.push_back(window_results[0]);
      max_parallel_durations.push_back(window_results[1]);
      mean_parallel_durations.push_back(window_results[2]);
      aggr_durations.push_back(window_results[3]);
      order_durations.push_back(window_results[4]);
      imbalances.push_back(window_results[5]);
      k_imbalances.push_back(window_results[6]);
      window_sizes.push_back(window.size());
      // progress window
      window_start = t_event.timestamp;
      window.clear();
      window.push_back(t_event);
    } else {
      window.push_back(t_event);
    }
  }
  if (window.size() > 0) {
    partitioner->init();
    // process window
    auto window_results = query_partitioner_simulation(
        false, window, tasks, partitioner, partitioner_name, "result");
    // accumulate results
    min_parallel_durations.push_back(window_results[0]);
    max_parallel_durations.push_back(window_results[1]);
    mean_parallel_durations.push_back(window_results[2]);
    aggr_durations.push_back(window_results[3]);
    order_durations.push_back(window_results[4]);
    imbalances.push_back(window_results[5]);
    k_imbalances.push_back(window_results[6]);
    window_sizes.push_back(window.size());
    window.clear();
    window_results.clear();
  }
  double mean_min_parallel_duration =
      StreamPartitionLib::Stats::Util::get_mean(min_parallel_durations);
  double mean_max_parallel_duration =
      StreamPartitionLib::Stats::Util::get_mean(max_parallel_durations);
  double ninetynile_max_parallel_duration =
      StreamPartitionLib::Stats::Util::get_percentile(max_parallel_durations,
                                                      0.99);
  double mean_mean_parallel_duration =
      StreamPartitionLib::Stats::Util::get_mean(mean_parallel_durations);
  double mean_aggr_duration =
      StreamPartitionLib::Stats::Util::get_mean(aggr_durations);
  double ninetynile_aggr_duration =
      StreamPartitionLib::Stats::Util::get_percentile(aggr_durations, 0.99);
  double mean_order_duration =
      StreamPartitionLib::Stats::Util::get_mean(order_durations);
  double mean_imbalance = StreamPartitionLib::Stats::Util::get_mean(imbalances);
  double mean_key_imbalance =
      StreamPartitionLib::Stats::Util::get_mean(k_imbalances);
  double mean_window_size =
      StreamPartitionLib::Stats::Util::get_mean(window_sizes);

  std::stringstream result_stream;
  result_stream << partitioner_name << "," << tasks.size() << ","
                << mean_min_parallel_duration << ","
                << mean_max_parallel_duration << ","
                << ninetynile_max_parallel_duration << ","
                << mean_mean_parallel_duration << "," << mean_aggr_duration
                << "," << ninetynile_aggr_duration << "," << mean_order_duration
                << "," << mean_imbalance << "," << mean_key_imbalance << ","
                << mean_window_size << "\n";
  std::cout << result_stream.str();
}
