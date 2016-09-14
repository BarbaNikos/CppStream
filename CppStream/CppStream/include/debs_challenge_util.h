#pragma once
#include <iostream>
#include <string>
#include <cmath>
#include <array>
#include <utility>
#include <functional>
#include <unordered_map>
#include <sstream>
#include <locale>
#include <iomanip>
#include <chrono>

#ifndef DEBS_COORDINATE_HELPER_H_
#define DEBS_COORDINATE_HELPER_H_

#if defined(_MSC_VER)

#include <ctime>
#define gmtime(result,time)	gmtime_s(result,time)

#else	// defined(_MSC_VER)
#define __STDC_WANT_LIB_EXT1__ 1
#include <ctime>
#define gmtime(result,time)	gmtime_s(time,result) 
#endif // !defined(_MSC_VER_)

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
			std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells);
		
		static double point_distance(double latitude, double longitude, double latitude_2, double longitude_2);
		
		static bool cell_membership(double point_latitude, double point_longitude, std::array<double, 8>& cell);
		
		static std::pair<uint16_t, uint16_t> recursive_location(int min_x, int max_x, int min_y, int max_y,
			std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude);

	private:
		
		static std::pair<uint16_t, uint16_t> locate(int min_x, int max_x, int min_y, int max_y, 
			std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells,
			double point_latitude, double point_longitude);

		static double to_degrees(double value_in_radians);

		static double to_radians(double value_in_degrees);
		
		static const double R;

		static const double PI;
	};

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
		void parse_cells(std::string ride_info, Ride& ride);
		CellAssign();
		~CellAssign();
	private:
		time_t produce_timestamp(const std::string& datetime_literal);
		std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>, DebsChallenge::pair_hash> cells;
		const static double latitude;
		const static double longitude;
		const static uint32_t cell_distance;
		const static uint32_t grid_distance;
	};
}
#endif // !DEBS_COORDINATE_HELPER_H_
