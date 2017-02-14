#pragma once
#include <iostream>

#ifndef STREAM_PARTITION_LIB_NAME_UTIL_
#define STREAM_PARTITION_LIB_NAME_UTIL_
namespace StreamPartitionLib
{
	namespace Name
	{
		class Util
		{
		public:
			static std::string shuffle_partitioner() 
			{
				return "sh";
			}

			static std::string field_partitioner()
			{
				return "fld";
			}

			static std::string partial_key_partitioner()
			{
				return "pk";
			}

			static std::string cardinality_naive_partitioner()
			{
				return "cn";
			}

			static std::string affinity_naive_partitioner()
			{
				return "an";
			}

			static std::string cardinality_hip_partitioner()
			{
				return "chll";
			}

			static std::string affinity_hip_partitioner()
			{
				return "ahll";
			}

			static std::string load_naive_partitioner()
			{
				return "ln";
			}

			static std::string load_hip_partitioner()
			{
				return "lhll";
			}

			static std::string multi_partial_key_partitioner()
			{
				return "mpk";
			}

			static std::string multi_affinity_naive_partitioner()
			{
				return "man";
			}

			static bool single_choice_partitioner(const std::string& name)
			{
				if (name.compare(field_partitioner()) == 0 || name.compare(affinity_naive_partitioner()) == 0 ||
					name.compare(affinity_hip_partitioner()) == 0 || name.compare(multi_affinity_naive_partitioner()) == 0)
					return true;
				return false;
			}
		};
	}
}
#endif // !STREAM_PARTITION_LIB_NAME_UTIL_
