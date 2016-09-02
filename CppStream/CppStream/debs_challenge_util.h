#pragma once

#include <array>
#include <utility>
#include <functional>
#include <unordered_map>

#ifndef DEBS_COORDINATE_HELPER_H_
#define DEBS_COORDINATE_HELPER_H_
namespace DebsChallenge
{
	typedef struct 
	{
		std::size_t operator () (const std::pair<uint8_t, uint8_t> &p) const
		{
			auto h1 = std::hash<uint8_t>{}(p.first);
			auto h2 = std::hash<uint8_t>{}(p.first);
			return h1 ^ h2;
		}
	}pair_hash;

	class TaxiCoordinateHelper
	{
	public:
		static std::array<double, 2> get_coordinate_point(double latitude, double longitude, double distance, double bearing);

		static std::array<double, 8> get_square_edges(double latitude, double longitude, double squareSideLength);
		
		static std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash> get_squares(double latitude, double longitude, double distance, double grid_distance);
		
		static double point_distance(double latitude, double longitude, double latitude_2, double longitude_2);
		
		static bool cell_membership(double point_latitude, double point_longitude, std::array<double, 8> cell);
		
		static std::pair<uint8_t, uint8_t> recursive_location(int min_x, int max_x, int min_y, int max_y,
			std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude);

	private:
		
		static std::pair<uint8_t, uint8_t> locate(int min_x, int max_x, int min_y, int max_y, 
			std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells,
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
		std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash> cells;
		const static double latitude;
		const static double longitude;
		const static uint32_t cell_distance;
		const static uint32_t grid_distance;
	};
}
#endif // !DEBS_COORDINATE_HELPER_H_
