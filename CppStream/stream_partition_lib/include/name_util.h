#pragma once

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

			static std::string multiple_partial_key_partitioner(size_t m)
			{
				return std::to_string(m) + "-pk";
			}

			static std::string cardinality_naive_partitioner()
			{
				return "cn";
			}

			static std::string affinity_naive_partitioner()
			{
				return "an";
			}

			static std::string multiple_affinity_naive_partitioner(size_t m)
			{
				return std::to_string(m) + "-an";
			}

			static std::string affinity_count_naive_partitioner()
			{
				return "can";
			}

			static std::string multiple_affinity_count_naive_partitioner(size_t m)
			{
				return std::to_string(m) + "-can";
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

			static std::string multi_partial_key_partitioner(size_t m)
			{
				return std::to_string(m) + "-pk";
			}

			static std::string multi_affinity_naive_partitioner(size_t m)
			{
				return std::to_string(m) + "-an";
			}

			static bool single_choice_partitioner(const std::string& name)
			{
				bool multi_an = name.find("-" + affinity_naive_partitioner()) != std::string::npos;
				bool multi_can = name.find("-" + affinity_count_naive_partitioner()) != std::string::npos;
				if (name.compare(field_partitioner()) == 0 || name.compare(affinity_naive_partitioner()) == 0 || 
					name.compare(affinity_count_naive_partitioner()) == 0 || name.compare(affinity_hip_partitioner()) == 0 || 
					multi_an || multi_can)
					return true;
				return false;
			}
		};
	}
}
#endif // !STREAM_PARTITION_LIB_NAME_UTIL_
