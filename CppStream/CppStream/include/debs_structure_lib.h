#pragma once
#include <iostream>
#include <cstring>
#include <ctime>
#include <cinttypes>
#include <cmath>

#ifndef DEBS_STRUCTURE_LIB_H_
#define DEBS_STRUCTURE_LIB_H_
namespace Experiment
{
	namespace DebsChallenge
	{
		typedef struct ride_str
		{
			ride_str()
			{
				memset(medallion, 0x0, 32 * sizeof(char));
				memset(hack_license, 0x0, 32 * sizeof(char));
				pickup_datetime = 0;
				dropoff_datetime = 0;
				trip_time_in_secs = 0;
				trip_distance = 0;
				pickup_cell.first = 0;
				pickup_cell.second = 0;
				dropoff_cell.first = 0;
				dropoff_cell.second = 0;
				memset(payment_type, 0x0, 3 * sizeof(char));
				fare_amount = 0;
				surcharge = 0;
				mta_tax = 0;
				tip_amount = 0;
				tolls_amount = 0;
				total_amount = 0;
			}
			ride_str(const ride_str& o)
			{
				memcpy(medallion, o.medallion, 32 * sizeof(char));
				memcpy(hack_license, o.hack_license, 32 * sizeof(char));
				pickup_datetime = o.pickup_datetime;
				dropoff_datetime = o.dropoff_datetime;
				trip_time_in_secs = o.trip_time_in_secs;
				trip_distance = o.trip_distance;
				pickup_cell.first = o.pickup_cell.first;
				pickup_cell.second = o.pickup_cell.second;
				dropoff_cell.first = o.dropoff_cell.first;
				dropoff_cell.second = o.dropoff_cell.second;
				memcpy(payment_type, o.payment_type, 3 * sizeof(char));
				fare_amount = o.fare_amount;
				surcharge = o.surcharge;
				mta_tax = o.mta_tax;
				tip_amount = o.tip_amount;
				tolls_amount = o.tolls_amount;
				total_amount = o.total_amount;
			}
			~ride_str() {}
			ride_str& operator= (const ride_str& o)
			{
				if (this != &o)
				{
					memcpy(medallion, o.medallion, 32 * sizeof(char));
					memcpy(hack_license, o.hack_license, 32 * sizeof(char));
					pickup_datetime = o.pickup_datetime;
					dropoff_datetime = o.dropoff_datetime;
					trip_time_in_secs = o.trip_time_in_secs;
					trip_distance = o.trip_distance;
					pickup_cell.first = o.pickup_cell.first;
					pickup_cell.second = o.pickup_cell.second;
					dropoff_cell.first = o.dropoff_cell.first;
					dropoff_cell.second = o.dropoff_cell.second;
					memcpy(payment_type, o.payment_type, 3 * sizeof(char));
					fare_amount = o.fare_amount;
					surcharge = o.surcharge;
					mta_tax = o.mta_tax;
					tip_amount = o.tip_amount;
					tolls_amount = o.tolls_amount;
					total_amount = o.total_amount;
				}
				return *this;
			}
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
		}Ride;

		typedef struct compact_ride_str
		{
			compact_ride_str()
			{
				memset(medallion, 0x0, 32 * sizeof(char));
				trip_distance = 0;
				pickup_cell.first = 0;
				pickup_cell.second = 0;
				dropoff_cell.first = 0;
				dropoff_cell.second = 0;
				fare_amount = 0;
				tip_amount = 0;
			}
			compact_ride_str(const compact_ride_str& o)
			{
				memcpy(medallion, o.medallion, 32 * sizeof(char));
				trip_distance = o.trip_distance;
				pickup_cell.first = o.pickup_cell.first;
				pickup_cell.second = o.pickup_cell.second;
				dropoff_cell.first = o.dropoff_cell.first;
				dropoff_cell.second = o.dropoff_cell.second;
				fare_amount = o.fare_amount;
				tip_amount = o.tip_amount;
			}
			compact_ride_str(const std::string& line)
			{
				parse_string(line);
			}
			~compact_ride_str() {}
			compact_ride_str& operator= (const compact_ride_str& o)
			{
				if (this != &o)
				{
					memcpy(medallion, o.medallion, 32 * sizeof(char));
					trip_distance = o.trip_distance;
					pickup_cell.first = o.pickup_cell.first;
					pickup_cell.second = o.pickup_cell.second;
					dropoff_cell.first = o.dropoff_cell.first;
					dropoff_cell.second = o.dropoff_cell.second;
					fare_amount = o.fare_amount;
					tip_amount = o.tip_amount;
				}
				return *this;
			}
			std::string to_string() const
			{
				char med_buffer[33];
				memcpy(med_buffer, medallion, 32 * sizeof(char));
				med_buffer[32] = '\0';
				std::string med = med_buffer;
				return med + "," + std::to_string(trip_distance) + "," +
					std::to_string(pickup_cell.first) + "," + std::to_string(pickup_cell.second) + "," +
					std::to_string(dropoff_cell.first) + "," + std::to_string(dropoff_cell.second) + "," +
					std::to_string(fare_amount) + "," + std::to_string(tip_amount);
			}
			void parse_string(const std::string& ride_info)
			{
				std::stringstream str_stream(ride_info);
				std::string token;
				std::vector<std::string> tokens;
				while (getline(str_stream, token, ','))
				{
					tokens.push_back(token);
				}
				memcpy(medallion, tokens[0].c_str(), 32 * sizeof(char));
				try
				{
					trip_distance = std::stof(tokens[1]);
				}
				catch (const std::exception&)
				{
					std::cerr << "parse_string():: failed on trip-distance: " << tokens[1] << ", line: " << ride_info << "\n";
				}
				pickup_cell.first = std::stoi(tokens[2]);
				pickup_cell.second = std::stoi(tokens[3]);
				dropoff_cell.first = std::stoi(tokens[4]);
				dropoff_cell.second = std::stoi(tokens[5]);
				try
				{
					fare_amount = std::stof(tokens[6]);
				}
				catch (const std::exception&)
				{
					std::cerr << "parse_string():: failed on fare-amount: " << tokens[6] << ", line: " << ride_info << "\n";
				}
				try
				{
					tip_amount = std::stof(tokens[7]);
				}
				catch (const std::exception&)
				{
					std::cerr << "parse_string():: failed on tip-amount: " << tokens[7] << ", line: " << ride_info << "\n";
				}
			}
			char medallion[32];
			float_t trip_distance;
			std::pair<uint8_t, uint8_t> pickup_cell;
			std::pair<uint8_t, uint8_t> dropoff_cell;
			float_t fare_amount;
			float_t tip_amount;
		}CompactRide;

		typedef struct frequent_route_str
		{
			frequent_route_str() : route(), count(0) {}
			frequent_route_str(const std::string route, const uint64_t count) : route(route), count(count) {}
			frequent_route_str(const frequent_route_str& o)
			{
				route = o.route;
				count = o.count;
			}
			~frequent_route_str() {}
			frequent_route_str& operator= (const frequent_route_str& o)
			{
				if (this != &o)
				{
					route = o.route;
					count = o.count;
				}
				return *this;
			}
			std::string route;
			uint64_t count;
		}frequent_route;

		typedef struct most_profitable_cell_str
		{
			most_profitable_cell_str() : cell(), profit(0) {}
			most_profitable_cell_str(std::string cell, const double profit) : cell(cell), profit(profit) {}
			most_profitable_cell_str(const most_profitable_cell_str& o)
			{
				cell = o.cell;
				profit = o.profit;
			}
			~most_profitable_cell_str() {}
			most_profitable_cell_str& operator= (const most_profitable_cell_str& o)
			{
				if (this != &o)
				{
					cell = o.cell;
					profit = o.profit;
				}
				return *this;
			}
			std::string cell;
			double profit;
		}most_profitable_cell;

	}
}
#endif // !DEBS_STRUCTURE_LIB_H_
