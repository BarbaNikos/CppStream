#pragma once

#ifndef IMBALANCE_SCORE_AGGR_H_
#include "imbalance_score_aggr.h"
#endif //!IMBALANCE_SCORE_AGGR_H_

#ifndef GCM_KEY_EXTRACTOR_H_
#define GCM_KEY_EXTRACTOR_H_
class GCMTaskEventKeyExtractor : public KeyExtractor<Experiment::GoogleClusterMonitor::task_event, int>
{
public:
	int extract_key(const Experiment::GoogleClusterMonitor::task_event& e) const override;
};
#endif

inline int GCMTaskEventKeyExtractor::extract_key(const Experiment::GoogleClusterMonitor::task_event& e) const
{
	return e.scheduling_class;
}