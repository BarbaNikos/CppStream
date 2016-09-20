#pragma once
#include <fstream>
#include <vector>
#include <array>
#include <sstream>
#include <cstring>

#include "partition_policy.h"
#include "hash_fld_partitioner.h"
#include "pkg_partitioner.h"
#include "cag_partitioner.h"

#ifndef EXPERIMENT_DEBS_H_
#define EXPERIMENT_DEBS_H_
namespace Experiment
{
	namespace DebsChallenge
	{
		typedef struct
		{
			std::size_t operator () (const std::pair<uint16_t, uint16_t> &p) const
			{
				auto h1 = std::hash<uint16_t>{}(p.first);
				auto h2 = std::hash<uint16_t>{}(p.second);
				std::size_t result = h1 ^ h2;
				return result;
			}
		}pair_hash;

		class TaxiCoordinateHelper
		{
		public:
			static std::array<double, 2> get_coordinate_point(double latitude, double longitude, double distance, double bearing);

			static std::array<double, 8> get_square_edges(double latitude, double longitude, double squareSideLength);

			static void get_squares(double latitude, double longitude, double distance, double grid_distance,
				std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash>& cells);

			static double point_distance(double latitude, double longitude, double latitude_2, double longitude_2);

			static bool cell_membership(double point_latitude, double point_longitude, std::array<double, 8>& cell);

			static std::pair<uint16_t, uint16_t> recursive_location(int min_x, int max_x, int min_y, int max_y,
				std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude);

		private:

			static std::pair<uint16_t, uint16_t> locate(int min_x, int max_x, int min_y, int max_y,
				std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash>& cells,
				double point_latitude, double point_longitude);

			static double to_degrees(double value_in_radians);

			static double to_radians(double value_in_degrees);

			static const double R;

			static const double PI;
		};

		typedef struct
		{
			char medallion[32];
			char hack_license[32];
			std::time_t pickup_datetime;
			std::time_t dropoff_datetime;
			uint8_t trip_time_in_secs;
			float_t trip_distance;
			std::pair<uint8_t, uint8_t> pickup_cell;
			std::pair<uint8_t, uint8_t> dropoff_cell;
			char payment_type[3];
			float_t fare_amount;
			float_t surcharge;
			float_t mta_tax;
			float_t tip_amount;
			float_t tolls_amount;
			float_t total_amount;
		}CustomRide;

		typedef struct
		{
			std::string medallion;
			std::string hack_license;
			std::time_t pickup_datetime;
			std::time_t dropoff_datetime;
			uint8_t trip_time_in_secs;
			float_t trip_distance;
			std::pair<uint8_t, uint8_t> pickup_cell;
			std::pair<uint8_t, uint8_t> dropoff_cell;
			std::string payment_type;
			float_t fare_amount;
			float_t surcharge;
			float_t mta_tax;
			float_t tip_amount;
			float_t tolls_amount;
			float_t total_amount;
		}Ride;

		class CellAssign
		{
		public:
			void parse_cells(std::string ride_info, Experiment::DebsChallenge::Ride& ride);
			CellAssign();
			~CellAssign();
		private:
			time_t produce_timestamp(const std::string& datetime_literal);
			std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash> cells;
			const static double latitude;
			const static double longitude;
			const static uint32_t cell_distance;
			const static uint32_t grid_distance;
		};

		typedef struct
		{
			uint64_t count;
		}frequent_route_result;

		class FrequentRoute
		{
		public:
			FrequentRoute(std::queue<Experiment::DebsChallenge::Ride>* input_queue, std::mutex* mu, std::condition_variable* cond);
			~FrequentRoute();
			void operate();
			void update(DebsChallenge::Ride& ride);
			void finalize();
		private:
			std::time_t* oldest_time;
			std::mutex* mu;
			std::condition_variable* cond;
			std::unordered_map<std::string, Experiment::DebsChallenge::frequent_route_result> result;
			std::queue<DebsChallenge::Ride>* input_queue;
		};

		class FrequentRoutePartition
		{
		public:
			static std::vector<Experiment::DebsChallenge::Ride> parse_debs_rides(const std::string input_file_name);
			static void debs_compare_cag_correctness(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::Ride> rides);
			static void debs_partition_performance(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::Ride> rides);
			static void debs_frequent_route_fld_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_pkg_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_cag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_lag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_cag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_lag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_cag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
			static void debs_frequent_route_lag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table);
		private:
			static void debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRoute* frequent_route);
		};
	}
}

const double Experiment::DebsChallenge::TaxiCoordinateHelper::R = double(6371000);

const double Experiment::DebsChallenge::TaxiCoordinateHelper::PI = double(3.141592653589793);

const double Experiment::DebsChallenge::CellAssign::latitude = double(41.474937);

const double Experiment::DebsChallenge::CellAssign::longitude = double(-74.913585);

const uint32_t Experiment::DebsChallenge::CellAssign::cell_distance = 500;

const uint32_t Experiment::DebsChallenge::CellAssign::grid_distance = 300;

inline std::array<double, 2> Experiment::DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(double latitude, double longitude, double distance, double bearing)
{
	std::array<double, 2> coordinates;
	double delta = distance / Experiment::DebsChallenge::TaxiCoordinateHelper::R;
	double rad_latitude = Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(latitude);
	double rad_bearing = Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(bearing);

	coordinates[0] = Experiment::DebsChallenge::TaxiCoordinateHelper::to_degrees(asin(sin(rad_latitude) * cos(delta) + cos(rad_latitude) * sin(delta) * cos(rad_bearing)));
	double val_1 = atan2(sin(rad_bearing) * sin(delta) * cos(rad_latitude),
		cos(delta) - sin(rad_latitude) * sin(Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(coordinates[0])));
	coordinates[1] = longitude + Experiment::DebsChallenge::TaxiCoordinateHelper::to_degrees(val_1);
	return coordinates;
}

inline std::array<double, 8> Experiment::DebsChallenge::TaxiCoordinateHelper::get_square_edges(double latitude, double longitude, double squareSideLength)
{
	double alpha = squareSideLength * sqrt(2);
	std::array<double, 2> north_west = Experiment::DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(315));
	std::array<double, 2> north_east = Experiment::DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(45));
	std::array<double, 2> south_east = Experiment::DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(135));
	std::array<double, 2> south_west = Experiment::DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(225));
	std::array<double, 8> square = { north_east[0], north_east[1], south_east[0], south_east[1], south_west[0], south_west[1],
		north_west[0], north_west[1] };
	return square;
}

void Experiment::DebsChallenge::TaxiCoordinateHelper::get_squares(double latitude, double longitude, double distance, double grid_distance,
	std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash>& cells)
{
	double center_latitude, center_longitude;
	double first_row_latitude = latitude;
	double first_row_longitude = longitude;
	uint16_t i = 1;
	do
	{
		center_latitude = first_row_latitude;
		center_longitude = first_row_longitude;
		uint16_t j = 1;
		do
		{
			std::array<double, 2> coordinates = { center_latitude, center_longitude };
			if (j > 1)
			{
				coordinates = get_coordinate_point(center_latitude, center_longitude, distance, double(90));
			}
			std::array<double, 8> square = get_square_edges(coordinates[0], coordinates[1], distance);
			std::pair<uint16_t, uint16_t> key = std::make_pair(uint16_t(i), uint16_t(j));
			cells.insert(std::make_pair(key, square));
			center_latitude = coordinates[0];
			center_longitude = coordinates[1];
			j++;
		} while (j <= grid_distance);
		std::array<double, 2> coordinates = get_coordinate_point(first_row_latitude, first_row_longitude, distance, double(180));
		first_row_latitude = coordinates[0];
		first_row_longitude = coordinates[1];
		i++;
	} while (i <= grid_distance);
}

inline double Experiment::DebsChallenge::TaxiCoordinateHelper::point_distance(double latitude, double longitude, double latitude_2, double longitude_2)
{
	double delta_latitude = Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(latitude_2 - latitude);
	double delta_longitude = Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(longitude_2 - longitude);
	double sin_delta_latitude = sin(delta_latitude / 2);
	double sin_delta_longitude = sin(delta_longitude / 2);
	double alpha = pow(sin_delta_latitude, 2) + cos(Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(latitude)) *
		cos(Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(latitude_2)) * pow(sin_delta_longitude, 2);
	double c = 2 * atan2(sqrt(alpha), sqrt(double(1.0) - alpha));
	return Experiment::DebsChallenge::TaxiCoordinateHelper::R * c;
}

inline bool Experiment::DebsChallenge::TaxiCoordinateHelper::cell_membership(double point_latitude, double point_longitude, std::array<double, 8>& cell)
{
	double x_a = cell[0];
	double y_a = cell[1];
	double x_b = cell[2];
	double y_b = cell[3];
	double x_c = cell[4];
	double y_c = cell[5];
	double x_d = cell[6];
	double y_d = cell[7];
	double f_ab = (x_a - x_b) * (point_longitude - y_b) - (y_a - y_b) * (point_latitude - x_b);
	double f_bc = (x_b - x_c) * (point_longitude - y_c) - (y_b - y_c) * (point_latitude - x_c);
	double f_cd = (x_c - x_d) * (point_longitude - y_d) - (y_c - y_d) * (point_latitude - x_d);
	double f_da = (x_d - x_a) * (point_longitude - y_a) - (y_d - y_a) * (point_latitude - x_a);
	return (f_ab <= 0 && f_bc <= 0 && f_cd <= 0 && f_da <= 0);
}

std::pair<uint16_t, uint16_t> Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(int min_x, int max_x, int min_y, int max_y,
	std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude)
{
	int mid_x = int(floor((min_x + max_x) / 2));
	int mid_y = int(floor((min_y + max_y) / 2));
	std::pair<uint16_t, uint16_t> key = std::make_pair(mid_x, mid_y);
	std::array<double, 8> middle_cell = cells[key];
	double f_upper = (middle_cell[6] - middle_cell[0]) * (point_longitude - middle_cell[1]) - (middle_cell[7] - middle_cell[1]) * (point_latitude - middle_cell[0]);
	double f_right = (middle_cell[4] - middle_cell[6]) * (point_longitude - middle_cell[7]) - (middle_cell[5] - middle_cell[7]) * (point_latitude - middle_cell[6]);
	if (f_upper > 0)
	{
		if (f_right > 0)
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::locate(min_x, mid_x - 1, min_y, mid_y - 1, cells, point_latitude, point_longitude);
			}
			else
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(min_x, mid_x, min_y, mid_y, cells, point_latitude, point_longitude);
			}
		}
		else
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::locate(min_x, mid_x - 1, mid_y, max_y, cells, point_latitude, point_longitude);
			}
			else
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(min_x, mid_x, mid_y, max_y, cells, point_latitude, point_longitude);
			}
		}
	}
	else
	{
		if (f_right > 0)
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::locate(mid_x, max_x, min_y, mid_y - 1, cells, point_latitude, point_longitude);
			}
			else
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(mid_x, max_x, min_y, mid_y, cells, point_latitude, point_longitude);
			}
		}
		else
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::locate(mid_x, max_x, mid_y, max_y, cells, point_latitude, point_longitude);
			}
			else
			{
				return Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(mid_x, max_x, mid_y, max_y, cells, point_latitude, point_longitude);
			}
		}
	}
}

std::pair<uint16_t, uint16_t> Experiment::DebsChallenge::TaxiCoordinateHelper::locate(int min_x, int max_x, int min_y, int max_y,
	std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, Experiment::DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude)
{
	for (uint16_t x = min_x; x <= max_x; ++x)
	{
		for (uint16_t y = min_y; y <= max_y; ++y)
		{
			auto candidate_cell = std::make_pair(uint16_t(x), uint16_t(y));
			if (Experiment::DebsChallenge::TaxiCoordinateHelper::cell_membership(point_latitude, point_longitude, cells[candidate_cell]))
			{
				return candidate_cell;
			}
		}
	}
	auto failed_cell = std::make_pair(-1, -1);
	return failed_cell;
}

inline double Experiment::DebsChallenge::TaxiCoordinateHelper::to_degrees(double value_in_radians)
{
	return value_in_radians * double(180) / Experiment::DebsChallenge::TaxiCoordinateHelper::PI;
}

inline double Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(double value_in_degrees)
{
	return value_in_degrees / double(180) * Experiment::DebsChallenge::TaxiCoordinateHelper::PI;
}

void Experiment::DebsChallenge::CellAssign::parse_cells(std::string ride_info, Experiment::DebsChallenge::Ride& ride)
{
	std::stringstream str_stream(ride_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, ','))
	{
		tokens.push_back(token);
	}
	ride.medallion = tokens[0];
	ride.hack_license = tokens[1];
	// format sample: 2013-01-01 00:00:00
	ride.pickup_datetime = produce_timestamp(tokens[2]);
	ride.dropoff_datetime = produce_timestamp(tokens[3]);
	ride.trip_time_in_secs = std::stoi(tokens[4]);
	ride.trip_distance = std::stof(tokens[5]);
	double pickup_longitude = std::stod(tokens[6]);
	double pickup_latitude = std::stod(tokens[7]);
	double dropoff_longitude = std::stod(tokens[8]);
	double dropoff_latitude = std::stod(tokens[9]);
	ride.pickup_cell = Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(1, grid_distance, 1, grid_distance, cells, pickup_latitude, pickup_longitude);
	ride.dropoff_cell = Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(1, grid_distance, 1, grid_distance, cells, dropoff_latitude, dropoff_longitude);
	ride.payment_type = tokens[10];
	ride.fare_amount = std::stof(tokens[11]);
	ride.surcharge = std::stof(tokens[12]);
	ride.mta_tax = std::stof(tokens[13]);
	ride.tip_amount = std::stof(tokens[14]);
	ride.tolls_amount = std::stof(tokens[15]);
	ride.total_amount = std::stof(tokens[16]);
}

Experiment::DebsChallenge::CellAssign::CellAssign()
{
	Experiment::DebsChallenge::TaxiCoordinateHelper::get_squares(Experiment::DebsChallenge::CellAssign::latitude, Experiment::DebsChallenge::CellAssign::longitude,
		Experiment::DebsChallenge::CellAssign::cell_distance, Experiment::DebsChallenge::CellAssign::grid_distance, cells);
}

Experiment::DebsChallenge::CellAssign::~CellAssign()
{
	cells.clear();
}

time_t Experiment::DebsChallenge::CellAssign::produce_timestamp(const std::string & datetime_literal)
{
	std::tm* time_info;
	std::time_t rawtime;
	int y = 0, m = 0, d = 0, h = 0, min = 0, sec = 0;
	time(&rawtime);
	sscanf(datetime_literal.c_str(), "%d-%d-%d %d:%d:%d", &y, &m, &d, &h, &min, &sec);
	time_info = gmtime(&rawtime);
	time_info->tm_year = y - 1900;
	time_info->tm_mon = m - 1;
	time_info->tm_mday = d;
	time_info->tm_hour = h;
	time_info->tm_min = min;
	time_info->tm_sec = sec;
	return std::mktime(time_info);
}

Experiment::DebsChallenge::FrequentRoute::FrequentRoute(std::queue<Experiment::DebsChallenge::Ride>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

inline Experiment::DebsChallenge::FrequentRoute::~FrequentRoute()
{
	result.clear();
}

inline void Experiment::DebsChallenge::FrequentRoute::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Experiment::DebsChallenge::Ride ride = input_queue->back();
		input_queue->pop();
		// process
		if (ride.trip_distance >= 0)
		{
			update(ride);
		}
		else
		{
			finalize();
			locker.unlock();
			cond->notify_all();
			break;
		}
		locker.unlock();
		cond->notify_all();
	}
}

void Experiment::DebsChallenge::FrequentRoute::update(Experiment::DebsChallenge::Ride& ride)
{
	std::string key = std::to_string(ride.pickup_cell.first) +
		std::to_string(ride.pickup_cell.second) +
		std::to_string(ride.dropoff_cell.first) +
		std::to_string(ride.dropoff_cell.second);
	// update counts
	std::unordered_map<std::string, DebsChallenge::frequent_route_result>::iterator it = result.find(key);
	if (it != result.end())
	{
		it->second.count += 1;
	}
	else
	{
		DebsChallenge::frequent_route_result tmp;
		tmp.count = 1;
		result.insert(std::make_pair(key, tmp));
	}
}

void Experiment::DebsChallenge::FrequentRoute::finalize()
{
	//std::cout << "number of groups: " << result.size() << ".\n";
}

std::vector<Experiment::DebsChallenge::Ride> Experiment::DebsChallenge::FrequentRoutePartition::parse_debs_rides(const std::string input_file_name)
{
	std::vector<Experiment::DebsChallenge::Ride> lines;
	std::string line;
	std::ifstream file(input_file_name);
	if (!file.is_open())
	{
		std::cout << "failed to open file.\n";
		exit(1);
	}
	Experiment::DebsChallenge::CellAssign cell_assign;
	std::chrono::system_clock::time_point scan_start = std::chrono::system_clock::now();
	while (getline(file, line))
	{
		Experiment::DebsChallenge::Ride ride;
		cell_assign.parse_cells(line, ride);
		lines.push_back(ride);
	}
	std::chrono::system_clock::time_point scan_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
	lines.shrink_to_fit();
	file.close();
	std::cout << "Time to scan and serialize file: " << scan_time.count() << " (msec).\n";
	return lines;
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_compare_cag_correctness(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::Ride> rides)
{
	// CAG-Naive
	CardinalityAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, policy);
	// CAG-PC
	CagPartionLib::CagPcPartitioner cag_pc(tasks, policy);
	std::vector<std::unordered_set<std::string>> cag_pc_key_per_task;
	std::vector<std::unordered_set<std::string>> cag_naive_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_naive_key_per_task.push_back(std::unordered_set<std::string>());
		cag_pc_key_per_task.push_back(std::unordered_set<std::string>());
	}
	cag_naive_key_per_task.shrink_to_fit();
	cag_pc_key_per_task.shrink_to_fit();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_naive.partition_next(key, key.length());
		short task_1 = cag_pc.partition_next(key, key.length());
		cag_naive_key_per_task[task].insert(key);
		cag_pc_key_per_task[task_1].insert(key);
	}
	size_t cag_naive_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t cag_pc_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t cag_naive_max_cardinality = std::numeric_limits<uint64_t>::min();
	size_t cag_pc_max_cardinality = std::numeric_limits<uint64_t>::min();
	double cag_naive_average_cardinality = 0;
	double cag_pc_average_cardinality = 0;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (cag_naive_min_cardinality > cag_naive_key_per_task[i].size())
		{
			cag_naive_min_cardinality = cag_naive_key_per_task[i].size();
		}
		if (cag_pc_min_cardinality > cag_pc_key_per_task[i].size())
		{
			cag_pc_min_cardinality = cag_pc_key_per_task[i].size();
		}
		if (cag_naive_max_cardinality < cag_naive_key_per_task[i].size())
		{
			cag_naive_max_cardinality = cag_naive_key_per_task[i].size();
		}
		if (cag_pc_max_cardinality < cag_pc_key_per_task[i].size())
		{
			cag_pc_max_cardinality = cag_pc_key_per_task[i].size();
		}
		cag_naive_average_cardinality += cag_naive_key_per_task[i].size();
		cag_pc_average_cardinality += cag_pc_key_per_task[i].size();
		cag_naive_key_per_task[i].clear();
		cag_pc_key_per_task[i].clear();
	}
	cag_naive_average_cardinality = cag_naive_average_cardinality / tasks.size();
	cag_pc_average_cardinality = cag_pc_average_cardinality / tasks.size();
	cag_naive_key_per_task.clear();
	cag_pc_key_per_task.clear();
	std::cout << "Time partition using CAG(naive) - Min: " << cag_naive_min_cardinality <<
		", Max: " << cag_naive_max_cardinality << ", AVG: " << cag_naive_average_cardinality << "\n";
	std::cout << "Time partition using CAG(pc) - Min: " << cag_pc_min_cardinality <<
		", Max: " << cag_pc_max_cardinality << ", AVG: " << cag_pc_average_cardinality << "\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_partition_performance(const std::vector<uint16_t>& tasks, const std::vector<Experiment::DebsChallenge::Ride> rides)
{
	// FLD
	HashFieldPartitioner fld(tasks);
	std::vector<std::unordered_set<std::string>> fld_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		fld_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point fld_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = fld.partition_next(key, key.length());
		fld_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point fld_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> fld_partition_time = fld_end - fld_start;
	size_t fld_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t fld_max_cardinality = std::numeric_limits<uint64_t>::min();
	double fld_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (fld_min_cardinality > fld_key_per_task[i].size())
		{
			fld_min_cardinality = fld_key_per_task[i].size();
		}
		if (fld_max_cardinality < fld_key_per_task[i].size())
		{
			fld_max_cardinality = fld_key_per_task[i].size();
		}
		fld_average_cardinality += fld_key_per_task[i].size();
		std::cout << fld_key_per_task[i].size() << " ";
		fld_key_per_task[i].clear();
	}
	std::cout << "\n";
	fld_average_cardinality = fld_average_cardinality / tasks.size();
	fld_key_per_task.clear();
	std::cout << "Time partition using FLD: " << fld_partition_time.count() << " (msec). Min: " << fld_min_cardinality << 
		", Max: " << fld_max_cardinality << ", AVG: " << fld_average_cardinality << "\n";

	
	// PKG
	PkgPartitioner pkg(tasks);
	std::vector<std::unordered_set<std::string>> pkg_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		pkg_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = pkg.partition_next(key, key.length());
		pkg_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	size_t pkg_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t pkg_max_cardinality = std::numeric_limits<uint64_t>::min();
	double pkg_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (pkg_min_cardinality > pkg_key_per_task[i].size())
		{
			pkg_min_cardinality = pkg_key_per_task[i].size();
		}
		if (pkg_max_cardinality < pkg_key_per_task[i].size())
		{
			pkg_max_cardinality = pkg_key_per_task[i].size();
		}
		pkg_average_cardinality += pkg_key_per_task[i].size();
		std::cout << pkg_key_per_task[i].size() << " ";
		pkg_key_per_task[i].clear();
	}
	std::cout << "\n";
	pkg_average_cardinality = pkg_average_cardinality / tasks.size();
	pkg_key_per_task.clear();
	std::cout << "Time partition using PKG: " << pkg_partition_time.count() << " (msec). Min: " << pkg_min_cardinality << 
		", Max: " << pkg_max_cardinality << ", AVG: " << pkg_average_cardinality << "\n";

	// CAG-Naive
	CardinalityAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, policy);
	std::vector<std::unordered_set<std::string>> cag_naive_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_naive_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_naive.partition_next(key, key.length());
		cag_naive_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	size_t cag_naive_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t cag_naive_max_cardinality = std::numeric_limits<uint64_t>::min();
	double cag_naive_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (cag_naive_min_cardinality > cag_naive_key_per_task[i].size())
		{
			cag_naive_min_cardinality = cag_naive_key_per_task[i].size();
		}
		if (cag_naive_max_cardinality < cag_naive_key_per_task[i].size())
		{
			cag_naive_max_cardinality = cag_naive_key_per_task[i].size();
		}
		cag_naive_average_cardinality += cag_naive_key_per_task[i].size();
		std::cout << cag_naive_key_per_task[i].size() << " ";
		cag_naive_key_per_task[i].clear();
	}
	std::cout << "\n";
	cag_naive_average_cardinality = cag_naive_average_cardinality / tasks.size();
	cag_naive_key_per_task.clear();
	std::cout << "Time partition using CAG(naive): " << cag_naive_partition_time.count() << " (msec). Min: " << cag_naive_min_cardinality << 
		", Max: " << cag_naive_max_cardinality << ", AVG: " << cag_naive_average_cardinality << "\n";
	// LAG-Naive
	LoadAwarePolicy lag_policy;
	CagPartionLib::CagNaivePartitioner lag_naive(tasks, lag_policy);
	std::vector<std::unordered_set<std::string>> lag_naive_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		lag_naive_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = lag_naive.partition_next(key, key.length());
		lag_naive_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	size_t lag_naive_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t lag_naive_max_cardinality = std::numeric_limits<uint64_t>::min();
	double lag_naive_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (lag_naive_min_cardinality > lag_naive_key_per_task[i].size())
		{
			lag_naive_min_cardinality = lag_naive_key_per_task[i].size();
		}
		if (lag_naive_max_cardinality < lag_naive_key_per_task[i].size())
		{
			lag_naive_max_cardinality = lag_naive_key_per_task[i].size();
		}
		lag_naive_average_cardinality += lag_naive_key_per_task[i].size();
		std::cout << lag_naive_key_per_task[i].size() << " ";
		lag_naive_key_per_task[i].clear();
	}
	std::cout << "\n";
	lag_naive_average_cardinality = lag_naive_average_cardinality / tasks.size();
	lag_naive_key_per_task.clear();
	std::cout << "Time partition using LAG(naive): " << lag_naive_partition_time.count() << " (msec). Min: " << lag_naive_min_cardinality << 
		", Max: " << lag_naive_max_cardinality << ", AVG: " << lag_naive_average_cardinality << "\n";
	// CAG-PC
	CagPartionLib::CagPcPartitioner cag_pc(tasks, policy);
	std::vector<std::unordered_set<std::string>> cag_pc_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_pc_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_pc.partition_next(key, key.length());
		cag_pc_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	size_t cag_pc_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t cag_pc_max_cardinality = std::numeric_limits<uint64_t>::min();
	double cag_pc_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (cag_pc_min_cardinality > cag_pc_key_per_task[i].size())
		{
			cag_pc_min_cardinality = cag_pc_key_per_task[i].size();
		}
		if (cag_pc_max_cardinality < cag_pc_key_per_task[i].size())
		{
			cag_pc_max_cardinality = cag_pc_key_per_task[i].size();
		}
		cag_pc_average_cardinality += cag_pc_key_per_task[i].size();
		std::cout << cag_pc_key_per_task[i].size() << " ";
		cag_pc_key_per_task[i].clear();
	}
	std::cout << "\n";
	cag_pc_average_cardinality = cag_pc_average_cardinality / tasks.size();
	cag_pc_key_per_task.clear();
	std::cout << "Time partition using CAG(pc): " << cag_pc_partition_time.count() << " (msec). Min: " << cag_pc_min_cardinality << 
		", Max: " << cag_pc_max_cardinality << ", AVG: " << cag_pc_average_cardinality << "\n";
	// LAG-PC
	CagPartionLib::CagPcPartitioner lag_pc(tasks, lag_policy);
	std::vector<std::unordered_set<std::string>> lag_pc_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		lag_pc_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = lag_pc.partition_next(key, key.length());
		lag_pc_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	size_t lag_pc_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t lag_pc_max_cardinality = std::numeric_limits<uint64_t>::min();
	double lag_pc_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (lag_pc_min_cardinality > lag_pc_key_per_task[i].size())
		{
			lag_pc_min_cardinality = lag_pc_key_per_task[i].size();
		}
		if (lag_pc_max_cardinality < lag_pc_key_per_task[i].size())
		{
			lag_pc_max_cardinality = lag_pc_key_per_task[i].size();
		}
		lag_pc_average_cardinality += lag_pc_key_per_task[i].size();
		std::cout << lag_pc_key_per_task[i].size() << " ";
		lag_pc_key_per_task[i].clear();
	}
	std::cout << "\n";
	lag_pc_average_cardinality = lag_pc_average_cardinality / tasks.size();
	lag_pc_key_per_task.clear();
	std::cout << "Time partition using LAG(pc): " << lag_pc_partition_time.count() << " (msec). Min: " << lag_pc_min_cardinality << 
		", Max: " << lag_pc_max_cardinality << ", AVG: " << lag_pc_average_cardinality << "\n";
	// CAG-HLL
	CagPartionLib::CagHllPartitioner cag_hll(tasks, policy, 5);
	std::vector<std::unordered_set<std::string>> cag_hll_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cag_hll_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = cag_hll.partition_next(key, key.length());
		cag_hll_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	size_t cag_hll_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t cag_hll_max_cardinality = std::numeric_limits<uint64_t>::min();
	double cag_hll_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (cag_hll_min_cardinality > cag_hll_key_per_task[i].size())
		{
			cag_hll_min_cardinality = cag_hll_key_per_task[i].size();
		}
		if (cag_hll_max_cardinality < cag_hll_key_per_task[i].size())
		{
			cag_hll_max_cardinality = cag_hll_key_per_task[i].size();
		}
		cag_hll_average_cardinality += cag_hll_key_per_task[i].size();
		std::cout << cag_hll_key_per_task[i].size() << " ";
		cag_hll_key_per_task[i].clear();
	}
	std::cout << "\n";
	cag_hll_average_cardinality = cag_hll_average_cardinality / tasks.size();
	cag_hll_key_per_task.clear();
	std::cout << "Time partition using CAG(hll): " << cag_hll_partition_time.count() << " (msec). Min: " << cag_hll_min_cardinality << 
		", Max: " << cag_hll_max_cardinality << ", AVG: " << cag_hll_average_cardinality << "\n";
	// LAG-HLL
	CagPartionLib::CagHllPartitioner lag_hll(tasks, lag_policy, 5);
	std::vector<std::unordered_set<std::string>> lag_hll_key_per_task;
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		lag_hll_key_per_task.push_back(std::unordered_set<std::string>());
	}
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<Experiment::DebsChallenge::Ride>::const_iterator it = rides.begin(); it != rides.end(); ++it)
	{
		std::string pickup_cell = std::to_string(it->pickup_cell.first) + "." + std::to_string(it->pickup_cell.second);
		std::string dropoff_cell = std::to_string(it->dropoff_cell.first) + "." + std::to_string(it->dropoff_cell.second);
		std::string key = pickup_cell + "," + dropoff_cell;
		short task = lag_hll.partition_next(key, key.length());
		lag_hll_key_per_task[task].insert(key);
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	size_t lag_hll_min_cardinality = std::numeric_limits<uint64_t>::max();
	size_t lag_hll_max_cardinality = std::numeric_limits<uint64_t>::min();
	double lag_hll_average_cardinality = 0;
	std::cout << "Cardinalities: ";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		if (lag_hll_min_cardinality > lag_hll_key_per_task[i].size())
		{
			lag_hll_min_cardinality = lag_hll_key_per_task[i].size();
		}
		if (lag_hll_max_cardinality < lag_hll_key_per_task[i].size())
		{
			lag_hll_max_cardinality = lag_hll_key_per_task[i].size();
		}
		lag_hll_average_cardinality += lag_hll_key_per_task[i].size();
		std::cout << lag_hll_key_per_task[i].size() << " ";
		lag_hll_key_per_task[i].clear();
	}
	std::cout << "\n";
	lag_hll_average_cardinality = lag_hll_average_cardinality / tasks.size();
	lag_hll_key_per_task.clear();
	std::cout << "Time partition using LAG(hll): " << lag_hll_partition_time.count() << " (msec). Min: " << lag_hll_min_cardinality << 
		", Max: " << lag_hll_max_cardinality << ", AVG: " << lag_hll_average_cardinality << "\n";
	std::cout << "------ END -----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_fld_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	HashFieldPartitioner fld(tasks);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point fld_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = fld.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point fld_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> fld_partition_time = fld_end - fld_start;
	std::cout << "Partioner thread (FLD) total partition time: " <<
		fld_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_pkg_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	PkgPartitioner pkg(tasks);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point pkg_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = pkg.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point pkg_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> pkg_partition_time = pkg_end - pkg_start;
	std::cout << "Partioner thread (PKG) total partition time: " <<
		pkg_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_cag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	CardinalityAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner cag_naive(tasks, policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point cag_naive_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = cag_naive.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_naive_partition_time = cag_naive_end - cag_naive_start;
	std::cout << "Partioner thread (CAG-naive) total partition time: " <<
		cag_naive_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_lag_naive_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	LoadAwarePolicy policy;
	CagPartionLib::CagNaivePartitioner lag_naive(tasks, policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point lag_naive_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = lag_naive.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_naive_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_naive_partition_time = lag_naive_end - lag_naive_start;
	std::cout << "Partioner thread (LAG-naive) total partition time: " <<
		lag_naive_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_cag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	CardinalityAwarePolicy policy;
	CagPartionLib::CagPcPartitioner cag_pc(tasks, policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point cag_pc_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = cag_pc.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_pc_partition_time = cag_pc_end - cag_pc_start;
	std::cout << "Partioner thread (CAG-pc) total partition time: " <<
		cag_pc_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_lag_pc_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	LoadAwarePolicy policy;
	CagPartionLib::CagPcPartitioner lag_pc(tasks, policy);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point lag_pc_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = lag_pc.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_pc_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_pc_partition_time = lag_pc_end - lag_pc_start;
	std::cout << "Partioner thread (LAG-pc) total partition time: " <<
		lag_pc_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_cag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	CardinalityAwarePolicy policy;
	CagPartionLib::CagHllPartitioner cag_hll(tasks, policy, 5);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point cag_hll_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = cag_hll.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point cag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> cag_hll_partition_time = cag_hll_end - cag_hll_start;
	std::cout << "Partioner thread (CAG-hll) total partition time: " <<
		cag_hll_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_lag_hll_concurrent_partition(const std::vector<uint16_t>& tasks, const std::vector<DebsChallenge::Ride>& route_table)
{
	// initialize shared memory
	std::queue<Experiment::DebsChallenge::Ride>** queues = new std::queue<Experiment::DebsChallenge::Ride>*[tasks.size()];
	std::mutex* mu_xes = new std::mutex[tasks.size()];
	std::condition_variable* cond_vars = new std::condition_variable[tasks.size()];
	std::thread** threads = new std::thread*[tasks.size()];
	Experiment::DebsChallenge::FrequentRoute** query_workers = new Experiment::DebsChallenge::FrequentRoute*[tasks.size()];

	for (size_t i = 0; i < tasks.size(); ++i)
	{
		queues[i] = new std::queue<Experiment::DebsChallenge::Ride>();
		query_workers[i] = new Experiment::DebsChallenge::FrequentRoute(queues[i], &mu_xes[i], &cond_vars[i]);
		threads[i] = new std::thread(debs_frequent_route_worker, query_workers[i]);
	}
	LoadAwarePolicy policy;
	CagPartionLib::CagHllPartitioner lag_hll(tasks, policy, 5);
	std::cout << "Partitioner thread INITIATES partitioning.\n";
	// start partitioning
	std::chrono::system_clock::time_point lag_hll_start = std::chrono::system_clock::now();
	for (std::vector<DebsChallenge::Ride>::const_iterator it = route_table.begin(); it != route_table.end(); ++it)
	{
		std::string key = std::to_string(it->pickup_cell.first) + "." +
			std::to_string(it->pickup_cell.second) + "," +
			std::to_string(it->dropoff_cell.first) + "." +
			std::to_string(it->dropoff_cell.second);
		short task = lag_hll.partition_next(key, key.length());
		std::unique_lock<std::mutex> locker(mu_xes[task]);
		queues[task]->push(*it);
		locker.unlock();
		cond_vars[task].notify_all();
	}
	std::cout << "Partitioner thread SENT ALL rides.\n";
	// send conclusive values
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		Experiment::DebsChallenge::Ride final_ride;
		final_ride.trip_distance = -1;
		std::unique_lock<std::mutex> locker(mu_xes[i]);
		queues[i]->push(final_ride);
		locker.unlock();
		cond_vars[i].notify_all();
	}
	// wait for workers to join
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		threads[i]->join();
	}
	std::chrono::system_clock::time_point lag_hll_end = std::chrono::system_clock::now();
	std::chrono::duration<double, std::milli> lag_hll_partition_time = lag_hll_end - lag_hll_start;
	std::cout << "Partioner thread (LAG-hll) total partition time: " <<
		lag_hll_partition_time.count() << " (msec).\n";
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		delete threads[i];
		delete query_workers[i];
		delete queues[i];
	}
	delete[] threads;
	delete[] query_workers;
	delete[] queues;
	delete[] mu_xes;
	delete[] cond_vars;
	std::cout << "------END-----\n";
}

void Experiment::DebsChallenge::FrequentRoutePartition::debs_frequent_route_worker(Experiment::DebsChallenge::FrequentRoute* frequent_route)
{
	frequent_route->operate();
}

#endif // !EXPERIMENT_DEBS_H_
