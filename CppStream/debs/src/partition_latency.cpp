#ifndef PARTITION_LATENCY_EXP_
#include "../include/partition_latency.h"
#endif // !PARTITION_LATENCY_EXP_

float PartitionLatency::get_percentile_duration(const std::vector<float>& sorted_durations, float k)
{
	if (k <= 0 || k >= 1)
	{
		return -1.0f;
	}
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

float PartitionLatency::get_mean(const std::vector<float>& durations)
{
	if (durations.size() <= 0)
	{
		return 0.0f;
	}
	else
	{
		return std::accumulate(durations.cbegin(), durations.end(), 0.0) / durations.size();
	}
}

void PartitionLatency::measure_latency(unsigned int task_number, const std::vector<Experiment::DebsChallenge::CompactRide>& frequent_ride_table)
{
	CardinalityAwarePolicy ca_policy;
	LoadAwarePolicy la_policy;
	std::vector<uint16_t> tasks(task_number, 0);

	Partitioner* rrg;
	Partitioner* fld;
	Partitioner* pkg;
	Partitioner* ca_naive;
	Partitioner* ca_aff_naive;
	Partitioner* ca_hll;
	Partitioner* ca_aff_hll;
	Partitioner* la_naive;
	Partitioner* la_hll;

	for (uint16_t i = 0; i < task_number; i++)
	{
		tasks[i] = i;
	}
	tasks.shrink_to_fit();

	std::cout << "tasks: " << task_number << "\n";
	std::cout << "name, mean, median, 90-ile, 99-ile, sum\n";
	rrg = new RoundRobinPartitioner(tasks);
	fld = new HashFieldPartitioner(tasks);
	pkg = new PkgPartitioner(tasks);
	ca_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &ca_policy);
	ca_aff_naive = new CaPartitionLib::CA_Exact_Aff_Partitioner(tasks);
	ca_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &ca_policy, 12);
	ca_aff_hll = new CaPartitionLib::CA_HLL_Aff_Partitioner(tasks, 12);
	la_naive = new CaPartitionLib::CA_Exact_Partitioner(tasks, &la_policy);
	la_hll = new CaPartitionLib::CA_HLL_Partitioner(tasks, &la_policy, 12);

	debs_frequent_route_partition_latency("shf", rrg, frequent_ride_table);
	debs_frequent_route_partition_latency("fld", fld, frequent_ride_table);
	debs_frequent_route_partition_latency("pkg", pkg, frequent_ride_table);
	debs_frequent_route_partition_latency("cn", ca_naive, frequent_ride_table);
	debs_frequent_route_partition_latency("an", ca_aff_naive, frequent_ride_table);
	debs_frequent_route_partition_latency("chll", ca_hll, frequent_ride_table);
	debs_frequent_route_partition_latency("ahll", ca_aff_hll, frequent_ride_table);
	debs_frequent_route_partition_latency("ln", la_naive, frequent_ride_table);
	debs_frequent_route_partition_latency("lhll", la_hll, frequent_ride_table);

	delete rrg;
	delete fld;
	delete pkg;
	delete ca_naive;
	delete ca_aff_naive;
	delete ca_hll;
	delete ca_aff_hll;
	delete la_naive;
	delete la_hll;
	tasks.clear();
}

void PartitionLatency::debs_frequent_route_partition_latency(const std::string& partitioner_name, Partitioner * partitioner, const std::vector<Experiment::DebsChallenge::CompactRide>& frequent_ride_table)
{
	uint64_t sum = 0;
	const int debs_window_size_in_sec = 1800; // 30 minutes
	std::vector<float> durations;
	std::vector<float> mean_durations;
	std::vector<float> median_durations;
	std::vector<float> ninety_pile_durations;
	std::vector<float> ninetynine_pile_durations;
	std::vector<Experiment::DebsChallenge::CompactRide> window_buffer;
	for (size_t iteration = 0; iteration < 7; ++iteration)
	{
		std::time_t window_start = frequent_ride_table.size() > 0 ? frequent_ride_table[0].dropoff_datetime : 0;
		for (auto it = frequent_ride_table.cbegin(); it != frequent_ride_table.cend(); ++it)
		{
			double sec_diff = std::difftime(it->dropoff_datetime, window_start);
			if (sec_diff >= debs_window_size_in_sec)
			{
				// send to partitioner and time how much time it took to partition
				std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
				for (auto window_it = window_buffer.cbegin(); window_it != window_buffer.cend(); ++window_it)
				{
					std::stringstream str_stream;
					str_stream << (unsigned short)it->pickup_cell.first << "." << (unsigned short)it->pickup_cell.second << "-" <<
						(unsigned short)it->dropoff_cell.first << "." << (unsigned short)it->dropoff_cell.second;
					std::string key = str_stream.str();
					unsigned int task = partitioner->partition_next(key.c_str(), strlen(key.c_str()));
					sum += task;
				}
				std::chrono::system_clock::time_point end = std::chrono::system_clock::now();
				std::chrono::duration<double, std::milli> execution_time = end - start;
				durations.push_back(float(execution_time.count()));
				// clean buffer and move window
				partitioner->init();
				window_start = it->dropoff_datetime;
				window_buffer.clear();
				window_buffer.push_back(Experiment::DebsChallenge::CompactRide(*it));
			}
			else
			{
				window_buffer.push_back(Experiment::DebsChallenge::CompactRide(*it));
			}
		}
		std::sort(durations.begin(), durations.end());
		ninety_pile_durations.push_back(this->get_percentile_duration(durations, 0.9));
		ninetynine_pile_durations.push_back(this->get_percentile_duration(durations, 0.99));
		median_durations.push_back(this->get_percentile_duration(durations, 0.5));
		mean_durations.push_back(this->get_mean(durations));
		durations.clear();
		window_buffer.clear();
	}
	auto max_it = std::max_element(mean_durations.begin(), mean_durations.end());
	mean_durations.erase(max_it);
	auto min_it = std::min_element(mean_durations.begin(), mean_durations.end());
	mean_durations.erase(min_it);
	max_it = std::max_element(median_durations.begin(), median_durations.end());
	median_durations.erase(max_it);
	min_it = std::min_element(median_durations.begin(), median_durations.end());
	median_durations.erase(min_it);
	max_it = std::max_element(ninety_pile_durations.begin(), ninety_pile_durations.end());
	ninety_pile_durations.erase(max_it);
	min_it = std::min_element(ninety_pile_durations.begin(), ninety_pile_durations.end());
	ninety_pile_durations.erase(min_it);
	max_it = std::max_element(ninetynine_pile_durations.begin(), ninetynine_pile_durations.end());
	ninetynine_pile_durations.erase(max_it);
	min_it = std::min_element(ninetynine_pile_durations.begin(), ninetynine_pile_durations.end());
	ninetynine_pile_durations.erase(min_it);
	float mean_latency = get_mean(mean_durations);
	float median_latency = get_mean(median_durations);
	float ninety_ile_latency = get_mean(ninety_pile_durations);
	float ninetynine_ile_latency = get_mean(ninetynine_pile_durations);
	std::cout << partitioner_name << ", " << mean_latency << ", " << median_latency << ", " << 
		ninety_ile_latency << ", " << ninetynine_ile_latency << ", " << sum << "\n";
}
