#pragma once
#include <array>
#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <vector>
#include <vector>

#ifndef DEBS_STRUCTURE_LIB_H_
#include "debs_structure_lib.h"
#endif  // !DEBS_STRUCTURE_LIB_H_

#ifndef DEBS_CELL_COORDINATE_UTIL_H_
#define DEBS_CELL_COORDINATE_UTIL_H_
namespace Experiment {
namespace DebsChallenge {
typedef struct {
  std::size_t operator()(const std::pair<uint16_t, uint16_t>& p) const {
    auto h1 = std::hash<uint16_t>{}(p.first);
    auto h2 = std::hash<uint16_t>{}(p.second);
    return h1 ^ h2;
  }
} pair_hash;

class TaxiCoordinateHelper {
 public:
  static std::array<double, 2> get_coordinate_point(double latitude,
                                                    double longitude,
                                                    double distance,
                                                    double bearing);
  static std::array<double, 8> get_square_edges(double latitude,
                                                double longitude,
                                                double squareSideLength);
  static void get_squares(
      double latitude, double longitude, double distance, double grid_distance,
      std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>,
                         Experiment::DebsChallenge::pair_hash>& cells);
  static double point_distance(double latitude, double longitude,
                               double latitude_2, double longitude_2);
  static bool cell_membership(double point_latitude, double point_longitude,
                              std::array<double, 8>& cell);
  static std::pair<uint16_t, uint16_t> recursive_location(
      int min_x, int max_x, int min_y, int max_y,
      std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>,
                         Experiment::DebsChallenge::pair_hash>& cells,
      double point_latitude, double point_longitude);

 private:
  static std::pair<uint16_t, uint16_t> locate(
      int min_x, int max_x, int min_y, int max_y,
      std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>,
                         Experiment::DebsChallenge::pair_hash>& cells,
      double point_latitude, double point_longitude);
  static double to_degrees(double value_in_radians);
  static double to_radians(double value_in_degrees);
  static const double R;
  static const double PI;
};

// cell-distance = 500; (frequent-route) // 250 (most-profitable cell)
// grid-distance = 300; (frequent-route) // 600 (most-profitable cell)
class DebsCellAssignment {
 public:
  DebsCellAssignment(uint32_t cell_side_size, uint32_t grid_side_size_in_cells);
  ~DebsCellAssignment();
  void parse_ride(std::string ride_info, Experiment::DebsChallenge::Ride& ride);
  int parse_compact_ride(std::string ride_info,
                         Experiment::DebsChallenge::CompactRide& ride);
  static void output_to_file(
      std::vector<Experiment::DebsChallenge::CompactRide>& rides,
      const std::string output_file_name);
  static time_t produce_timestamp(const std::string& datetime_literal);

 private:
  std::unordered_map<std::pair<uint16_t, uint16_t>, std::array<double, 8>,
                     Experiment::DebsChallenge::pair_hash>
      cells;
  const static double latitude;
  const static double longitude;
  uint32_t cell_distance;
  uint32_t grid_distance;
};

class Parser {
 public:
  /**
  * De-serialize rides from file
  */
  static void produce_compact_ride_file(const std::string& input_file_name,
                                        const std::string& output_file_name,
                                        uint32_t cell_side_size,
                                        uint32_t grid_side_size_in_cells) {
    std::string line;
    std::ifstream file(input_file_name);
    std::ofstream output_file(output_file_name);
    if (!file.is_open()) {
      std::cout << "failed to open file.\n";
      exit(1);
    }
    Experiment::DebsChallenge::DebsCellAssignment cell_assign(
        cell_side_size, grid_side_size_in_cells);
    while (getline(file, line)) {
      Experiment::DebsChallenge::CompactRide ride;
      int val = cell_assign.parse_compact_ride(line, ride);
      if (val == 0) {
        output_file << ride.to_string() << "\n";
      }
    }
    file.close();
    output_file.flush();
    output_file.close();
  }
  static std::vector<Experiment::DebsChallenge::CompactRide> parse_debs_rides(
      const std::string input_file_name, uint32_t cell_side_size,
      uint32_t grid_side_size_in_cells) {
    std::vector<Experiment::DebsChallenge::CompactRide> lines;
    std::string line;
    std::ifstream file(input_file_name);
    if (!file.is_open()) {
      std::cout << "failed to open file.\n";
      exit(1);
    }
    Experiment::DebsChallenge::DebsCellAssignment cell_assign(
        cell_side_size, grid_side_size_in_cells);
    std::chrono::system_clock::time_point scan_start =
        std::chrono::system_clock::now();
    while (getline(file, line)) {
      Experiment::DebsChallenge::CompactRide ride;
      int val = cell_assign.parse_compact_ride(line, ride);
      if (val == 0) {
        lines.push_back(ride);
      }
    }
    std::chrono::system_clock::time_point scan_end =
        std::chrono::system_clock::now();
    std::chrono::duration<double, std::milli> scan_time = scan_end - scan_start;
    lines.shrink_to_fit();
    file.close();
    std::cout << "Time to scan and serialize file: " << scan_time.count()
              << " (msec).\n";
    return lines;
  }
  static void parse_debs_rides_with_to_string(
      const std::string input_file_name,
      std::vector<Experiment::DebsChallenge::CompactRide>* buffer) {
    std::string line;
    std::ifstream file(input_file_name);
    if (!file.is_open()) {
      std::cout << "failed to open file.\n";
      exit(1);
    }
    while (getline(file, line)) {
      buffer->push_back(Experiment::DebsChallenge::CompactRide(line));
    }
    buffer->shrink_to_fit();
    file.close();
  }
};
}
}
#endif  // DEBS_CELL_COORDINATE_UTIL_H_
