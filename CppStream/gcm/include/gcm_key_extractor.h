#pragma once

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif  //! IMBALANCE_SCORE_AGGR_H_

#ifndef GCM_KEY_EXTRACTOR_H_
#define GCM_KEY_EXTRACTOR_H_
class GCMTaskEventKeyExtractor
    : public KeyExtractor<Experiment::GoogleClusterMonitor::task_event, int> {
 public:
  int extract_key(
      const Experiment::GoogleClusterMonitor::task_event& e) const override {
    return e.scheduling_class;
  }

  KeyExtractor<Experiment::GoogleClusterMonitor::task_event, int>* clone()
      const override {
    return new GCMTaskEventKeyExtractor();
  }
};
class GCMTaskEventJobIdExtractor
    : public KeyExtractor<Experiment::GoogleClusterMonitor::task_event, long> {
 public:
  long extract_key(
      const Experiment::GoogleClusterMonitor::task_event& e) const override {
    return e.job_id;
  }

  KeyExtractor<Experiment::GoogleClusterMonitor::task_event, long>* clone()
      const override {
    return new GCMTaskEventJobIdExtractor();
  }
};
#endif
