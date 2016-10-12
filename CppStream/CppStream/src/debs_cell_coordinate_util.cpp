#include "../include/debs_cell_coordinate_util.h"

const double Experiment::DebsChallenge::TaxiCoordinateHelper::R = double(6371000);

const double Experiment::DebsChallenge::TaxiCoordinateHelper::PI = double(3.141592653589793);

const double Experiment::DebsChallenge::DebsCellAssignment::latitude = double(41.474937);

const double Experiment::DebsChallenge::DebsCellAssignment::longitude = double(-74.913585);

std::array<double, 2> Experiment::DebsChallenge::TaxiCoordinateHelper::get_coordinate_point(double latitude, double longitude, double distance, double bearing)
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

std::array<double, 8> Experiment::DebsChallenge::TaxiCoordinateHelper::get_square_edges(double latitude, double longitude, double squareSideLength)
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

double Experiment::DebsChallenge::TaxiCoordinateHelper::point_distance(double latitude, double longitude, double latitude_2, double longitude_2)
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

bool Experiment::DebsChallenge::TaxiCoordinateHelper::cell_membership(double point_latitude, double point_longitude, std::array<double, 8>& cell)
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

double Experiment::DebsChallenge::TaxiCoordinateHelper::to_degrees(double value_in_radians)
{
	return value_in_radians * double(180) / Experiment::DebsChallenge::TaxiCoordinateHelper::PI;
}

double Experiment::DebsChallenge::TaxiCoordinateHelper::to_radians(double value_in_degrees)
{
	return value_in_degrees / double(180) * Experiment::DebsChallenge::TaxiCoordinateHelper::PI;
}

Experiment::DebsChallenge::DebsCellAssignment::DebsCellAssignment(uint32_t cell_side_size, uint32_t grid_side_size_in_cells)
{
	cell_distance = cell_side_size;
	grid_distance = grid_side_size_in_cells;
	Experiment::DebsChallenge::TaxiCoordinateHelper::get_squares(Experiment::DebsChallenge::DebsCellAssignment::latitude,
		Experiment::DebsChallenge::DebsCellAssignment::longitude, cell_distance, grid_distance, cells);
}

Experiment::DebsChallenge::DebsCellAssignment::~DebsCellAssignment()
{
	cells.clear();
}

void Experiment::DebsChallenge::DebsCellAssignment::parse_ride(std::string ride_info, Experiment::DebsChallenge::Ride& ride)
{
	std::stringstream str_stream(ride_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, ','))
	{
		tokens.push_back(token);
	}
	memcpy(ride.medallion, tokens[0].c_str(), 32 * sizeof(char));
	memcpy(ride.hack_license, tokens[1].c_str(), 32 * sizeof(char));
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
	memcpy(ride.payment_type, tokens[10].c_str(), 3 * sizeof(char));
	ride.fare_amount = std::stof(tokens[11]);
	ride.surcharge = std::stof(tokens[12]);
	ride.mta_tax = std::stof(tokens[13]);
	ride.tip_amount = std::stof(tokens[14]);
	ride.tolls_amount = std::stof(tokens[15]);
	ride.total_amount = std::stof(tokens[16]);
}

int Experiment::DebsChallenge::DebsCellAssignment::parse_compact_ride(std::string ride_info, Experiment::DebsChallenge::CompactRide & ride)
{
	double pickup_longitude = 0, pickup_latitude = 0, dropoff_longitude = 0, dropoff_latitude = 0;
	std::stringstream str_stream(ride_info);
	std::string token;
	std::vector<std::string> tokens;
	while (getline(str_stream, token, ','))
	{
		tokens.push_back(token);
	}
	memcpy(ride.medallion, tokens[0].c_str(), 32 * sizeof(char));
	ride.trip_distance = std::stof(tokens[5]);
	try
	{
		pickup_longitude = std::stod(tokens[6]);
	}
	catch (const std::invalid_argument&)
	{
		return -1;
	}
	try
	{
		pickup_latitude = std::stod(tokens[7]);
	}
	catch (const std::exception&)
	{
		return -1;
	}
	try
	{
		dropoff_longitude = std::stod(tokens[8]);
	}
	catch (const std::exception&)
	{
		return -1;
	}
	try
	{
		dropoff_latitude = std::stod(tokens[9]);
	}
	catch (const std::exception&)
	{
		return -1;
	}
	ride.pickup_cell = Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(1, grid_distance, 1, grid_distance, cells, pickup_latitude, pickup_longitude);
	ride.dropoff_cell = Experiment::DebsChallenge::TaxiCoordinateHelper::recursive_location(1, grid_distance, 1, grid_distance, cells, dropoff_latitude, dropoff_longitude);
	ride.fare_amount = std::stof(tokens[11]);
	ride.tip_amount = std::stof(tokens[14]);
	return 0;
}

void Experiment::DebsChallenge::DebsCellAssignment::output_to_file(std::vector<Experiment::DebsChallenge::CompactRide>& rides, const std::string output_file_name)
{
	std::ifstream check_file(output_file_name);
	if (check_file.good())
	{
		std::cout << "file with name\"" << output_file_name << "\" already exists. Please, delete it and then re-run the operation.\n";
		exit(1);
	}
	else
	{
		std::ofstream out_file(output_file_name);
		if (out_file.is_open())
		{
			for (auto it = rides.begin(); it != rides.end(); ++it)
			{
				out_file << it->to_string() << "\n";
			}
			out_file.flush();
			out_file.close();
			std::cout << "file with name\"" << output_file_name << "\" has been successfully created and populated accordingly.\n";
		}
		else
		{
			std::cout << "unable to create file \"" << output_file_name << "\".\n";
			exit(1);
		}
	}
}

time_t Experiment::DebsChallenge::DebsCellAssignment::produce_timestamp(const std::string & datetime_literal)
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