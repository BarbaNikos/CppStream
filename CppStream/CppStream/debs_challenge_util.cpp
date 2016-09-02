#pragma once
#include <iostream>
#include <cmath>
#include <string>
#include <sstream>
#include <ctime>
#include <locale>
#include <iomanip>

#include "debs_challenge_util.h"

const double DebsChallenge::TaxiCoordinateHelper::R = double(6371000);

const double DebsChallenge::TaxiCoordinateHelper::PI = double(3.141592653589793);

const double DebsChallenge::CellAssign::latitude = double(41.474937);

const double DebsChallenge::CellAssign::longitude = double(-74.913585);

const uint32_t DebsChallenge::CellAssign::cell_distance = 500;

const uint32_t DebsChallenge::CellAssign::grid_distance = 300;

inline std::array<double, 2> DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(double latitude, double longitude, double distance, double bearing)
{
	std::array<double, 2> coordinates;
	double delta = distance / DebsChallenge::TaxiCoordinateHelper::R;
	double rad_latitude = DebsChallenge::TaxiCoordinateHelper::to_radians(latitude);
	double rad_bearing = DebsChallenge::TaxiCoordinateHelper::to_radians(bearing);

	coordinates[0] = DebsChallenge::TaxiCoordinateHelper::to_degrees(asin(sin(rad_latitude) * cos(delta) + cos(rad_latitude) * sin(delta) * cos(rad_bearing)));
	double val_1 = atan2(sin(rad_bearing) * sin(delta) * cos(rad_latitude), 
		cos(delta) - sin(rad_latitude) * sin(DebsChallenge::TaxiCoordinateHelper::to_radians(coordinates[0])));
	coordinates[1] = longitude + DebsChallenge::TaxiCoordinateHelper::to_degrees(val_1);
	return coordinates;
}

inline std::array<double, 8> DebsChallenge::TaxiCoordinateHelper::get_square_edges(double latitude, double longitude, double squareSideLength)
{
	double alpha = squareSideLength * sqrt(2);
	std::array<double, 2> north_west = DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(315));
	std::array<double, 2> north_east = DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(45));
	std::array<double, 2> south_east = DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(135));
	std::array<double, 2> south_west = DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(latitude, longitude, alpha, double(225));
	std::array<double, 8> square = {north_east[0], north_east[1], south_east[0], south_east[1], south_west[0], south_west[1], 
		north_west[0], north_west[1]};
	return square;
}

std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash> DebsChallenge::TaxiCoordinateHelper::get_squares(double latitude, double longitude, 
	double distance, double grid_distance)
{
	std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash> cell_grid;
	double center_latitude;
	double center_longitude;
	double first_row_latitude = latitude;
	double first_row_longitude = longitude;
	for (size_t i = 1; i <= grid_distance; i++)
	{
		if (i > 1)
		{
			std::array<double, 2> coordinates = get_coordinate_point(first_row_latitude, first_row_longitude, distance, double(180));
			first_row_latitude = coordinates[0];
			first_row_longitude = coordinates[1];
		}
		center_latitude = first_row_latitude;
		center_longitude = first_row_longitude;
		for (size_t j = 1; j <= grid_distance; j++)
		{
			std::array<double, 2> coordinates;
			if (j > 1)
			{
				coordinates = get_coordinate_point(center_latitude, center_longitude, distance, double(90));
			}
			else
			{
				coordinates[0] = center_latitude;
				coordinates[1] = center_longitude;
			}
			std::array<double, 8> square = get_square_edges(coordinates[0], coordinates[1], distance);
			std::pair<uint8_t, uint8_t> key = std::make_pair(uint8_t(i), uint8_t(j));
			cell_grid.insert(std::make_pair(key, square));
			center_latitude = coordinates[0];
			center_longitude = coordinates[1];
		}
	}
	return cell_grid;
}

inline double DebsChallenge::TaxiCoordinateHelper::point_distance(double latitude, double longitude, double latitude_2, double longitude_2)
{
	double delta_latitude = DebsChallenge::TaxiCoordinateHelper::to_radians(latitude_2 - latitude);
	double delta_longitude = DebsChallenge::TaxiCoordinateHelper::to_radians(longitude_2 - longitude);
	double sin_delta_latitude = sin(delta_latitude / 2);
	double sin_delta_longitude = sin(delta_longitude / 2);
	double alpha = pow(sin_delta_latitude, 2) + cos(DebsChallenge::TaxiCoordinateHelper::to_radians(latitude)) * 
		cos(DebsChallenge::TaxiCoordinateHelper::to_radians(latitude_2)) * pow(sin_delta_longitude, 2);
	double c = 2 * atan2(sqrt(alpha), sqrt(double(1.0) - alpha));
	return DebsChallenge::TaxiCoordinateHelper::R * c;
}

inline bool DebsChallenge::TaxiCoordinateHelper::cell_membership(double point_latitude, double point_longitude, std::array<double, 8> cell)
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

std::pair<uint8_t, uint8_t> DebsChallenge::TaxiCoordinateHelper::recursive_location(int min_x, int max_x, int min_y, int max_y, 
	std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude)
{
	int mid_x = int(floor((min_x + max_x) / 2));
	int mid_y = int(floor((min_y + max_y) / 2));
	std::pair<uint8_t, uint8_t> key = std::make_pair(mid_x, mid_y);
	std::array<double, 8> middle_cell = cells[key];
	double f_upper = (middle_cell[6] - middle_cell[0]) * (point_longitude - middle_cell[1]) - (middle_cell[7] - middle_cell[1]) * (point_latitude - middle_cell[0]);
	double f_right = (middle_cell[4] - middle_cell[6]) * (point_longitude - middle_cell[7]) - (middle_cell[5] - middle_cell[7]) * (point_latitude - middle_cell[6]);
	if (f_upper > 0)
	{
		if (f_right > 0)
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return DebsChallenge::TaxiCoordinateHelper::locate(min_x, mid_x - 1, min_y, mid_y - 1, cells, point_latitude, point_longitude);
			}
			else
			{
				return DebsChallenge::TaxiCoordinateHelper::recursive_location(min_x, mid_x, min_y, mid_y, cells, point_latitude, point_longitude);
			}
		}
		else
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return DebsChallenge::TaxiCoordinateHelper::locate(min_x, mid_x - 1, mid_y, max_y, cells, point_latitude, point_longitude);
			}
			else
			{
				return DebsChallenge::TaxiCoordinateHelper::recursive_location(min_x, mid_x, mid_y, max_y, cells, point_latitude, point_longitude);
			}
		}
	}
	else
	{
		if (f_right > 0)
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return DebsChallenge::TaxiCoordinateHelper::locate(mid_x, max_x, min_y, mid_y - 1, cells, point_latitude, point_longitude);
			}
			else
			{
				return DebsChallenge::TaxiCoordinateHelper::recursive_location(mid_x, max_x, min_y, mid_y, cells, point_latitude, point_longitude);
			}
		}
		else
		{
			if (abs(max_x - min_x) <= 3 && abs(max_y - min_y) <= 3)
			{
				return DebsChallenge::TaxiCoordinateHelper::locate(mid_x, max_x, mid_y, max_y, cells, point_latitude, point_longitude);
			}
			else
			{
				return DebsChallenge::TaxiCoordinateHelper::recursive_location(mid_x, max_x, mid_y, max_y, cells, point_latitude, point_longitude);
			}
		}
	}
}

std::pair<uint8_t, uint8_t> DebsChallenge::TaxiCoordinateHelper::locate(int min_x, int max_x, int min_y, int max_y, 
	std::unordered_map<std::pair<uint8_t, uint8_t>, std::array<double, 8>, DebsChallenge::pair_hash>& cells, double point_latitude, double point_longitude)
{
	for (size_t x = min_x; x <= max_x; ++x)
	{
		for (size_t y = min_y; y <= max_y; ++y)
		{
			auto candidate_cell = std::make_pair(uint8_t(x), uint8_t(y));
			if (DebsChallenge::TaxiCoordinateHelper::cell_membership(point_latitude, point_longitude, cells[candidate_cell]))
			{
				return candidate_cell;
			}
		}
	}
}

inline double DebsChallenge::TaxiCoordinateHelper::to_degrees(double value_in_radians)
{
	return value_in_radians * double(180) / DebsChallenge::TaxiCoordinateHelper::PI;
}

inline double DebsChallenge::TaxiCoordinateHelper::to_radians(double value_in_degrees)
{
	return value_in_degrees / double(180) * DebsChallenge::TaxiCoordinateHelper::PI;
}

void DebsChallenge::CellAssign::parse_cells(std::string ride_info, Ride& ride)
{
	std::tm pickup_t = {};
	std::tm dropoff_t = {};
	std::stringstream str_stream(ride_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, ','))
	{
		tokens.push_back(token);
	}
	ride.medallion = tokens[0];
	ride.hack_license = tokens[1];
	str_stream = std::stringstream(tokens[2]);
	str_stream >> std::get_time(&pickup_t, "%Y-%m-%d %H:%M:%S"); // format sample: 2013-01-01 00:00:00
	ride.pickup_datetime = std::mktime(&pickup_t);
	str_stream = std::stringstream(tokens[3]);
	str_stream >> std::get_time(&dropoff_t, "%Y-%m-%d %H:%M:%S");
	ride.dropoff_datetime = std::mktime(&dropoff_t);
	ride.trip_time_in_secs = std::stoi(tokens[4]);
	ride.trip_distance = std::stof(tokens[5]);
	double pickup_longitude = std::stod(tokens[6]);
	double pickup_latitude = std::stod(tokens[7]);
	double dropoff_longitude = std::stod(tokens[8]);
	double dropoff_latitude = std::stod(tokens[9]);
	ride.pickup_cell = DebsChallenge::TaxiCoordinateHelper::recursive_location(1, grid_distance, 1, grid_distance, cells, pickup_latitude, pickup_longitude);
	ride.dropoff_cell = DebsChallenge::TaxiCoordinateHelper::recursive_location(1, grid_distance, 1, grid_distance, cells, dropoff_latitude, dropoff_longitude);
	ride.payment_type = tokens[10];
	ride.fare_amount = std::stof(tokens[11]);
	ride.surcharge = std::stof(tokens[12]);
	ride.mta_tax = std::stof(tokens[13]);
	ride.tip_amount = std::stof(tokens[14]);
	ride.tolls_amount = std::stof(tokens[15]);
	ride.total_amount = std::stof(tokens[16]);
}

DebsChallenge::CellAssign::CellAssign()
{
	cells = DebsChallenge::TaxiCoordinateHelper::get_squares(CellAssign::latitude, CellAssign::longitude, 
		CellAssign::cell_distance, CellAssign::grid_distance);
}

DebsChallenge::CellAssign::~CellAssign()
{
	cells.clear();
}
