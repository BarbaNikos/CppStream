#ifndef CARDINTALITY_ESTIMATION_H_
#include "../include/cardinality_estimation_utils.h"
#endif // !CARDINTALITY_ESTIMATION_H_

const double CardinalityEstimator::ProbCount::phi = 0.77351;

const double CardinalityEstimator::HyperLoglog::a_16 = 0.673;

const double CardinalityEstimator::HyperLoglog::a_32 = 0.697;

const double CardinalityEstimator::HyperLoglog::a_64 = 0.709;