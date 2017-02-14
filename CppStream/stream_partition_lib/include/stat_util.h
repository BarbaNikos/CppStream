#pragma once
#include <vector>
#include <cmath>
#include <algorithm>
#include <numeric>

#ifndef STREAM_PARTITION_LIB_UTILS_
#define STREAM_PARTITION_LIB_UTILS_
namespace StreamPartitionLib
{
	namespace Stats
	{
		class Util
		{
		private:
			static float get_percentile_duration(const std::vector<double>& sorted_durations, float k)
			{
				{
					if (k <= 0 || k >= 1)
						return -1.0f;
					float int_part = 0.0f;
					float float_part = 0.0f;
					float vector_index = k * sorted_durations.size();
					float_part = modff(vector_index, &int_part);
					if (float_part != 0)
					{
						int index = ceil(vector_index);
						if (index < sorted_durations.size()) {
							return sorted_durations[index];
						}
						else
						{
							return sorted_durations[floor(vector_index)];
						}
					}
					else
					{
						if (int_part < (sorted_durations.size() - 1))
						{
							return (sorted_durations[int_part] + sorted_durations[int_part + 1]) / 2;
						}
					}
				}
			}
		public:
			static float get_percentile(const std::vector<double>& v, float k)
			{
				if (k <= 0 || k >= 1 || v.size() <= 0)
				{
					return -1.0f;
				}
				std::vector<double> to_sort(v);
				std::sort(to_sort.begin(), to_sort.end());
				return get_percentile_duration(to_sort, k);
			}

			static float get_mean(const std::vector<double>& durations)
			{
				if (durations.size() <= 0)
					return 0.0f;
				return std::accumulate(durations.cbegin(), durations.end(), 0.0) / durations.size();
			}
		};
	}
}
#endif