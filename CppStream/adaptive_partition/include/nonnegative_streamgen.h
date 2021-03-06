#pragma once
#include <iostream>
#include <vector>
#include <random>
#include <exception>
#include <string>
#include <sstream>
#include <map>
#include <iomanip>
#include <cmath>

#ifndef NON_NEGATIVE_STREAMGEN_H_
#define NON_NEGATIVE_STREAMGEN_H_
class StreamGenerator
{
public:
	static void generate_uniform_stream(unsigned int length, uint64_t max, std::vector<uint64_t>& stream)
	{
		std::random_device rd;
		std::mt19937 gen(rd());
		std::uniform_int_distribution<uint64_t> dis(0, max);
		while (stream.size() < length)
		{
			stream.push_back(dis(gen));
		}
	}

	static void generate_normal_stream(unsigned int length, uint64_t max, std::vector<int>& stream)
	{
		double fractpart, intpart;
		std::stringstream error_message;
		if (length <= 0 || max <= 0)
		{
			error_message << "Non-negative length and maximum value have to be provided.";
			throw std::exception(error_message.str().c_str());
		}
		double mean = double(max) / 2;
		double stddev = (max - mean) / 100;
		std::random_device rd;
		std::mt19937 gen(rd());
		std::normal_distribution<> d(mean, stddev);
		while (stream.size() < length)
		{
			double element = d(gen);
			if (element < 0)
			{
				error_message << "Negative integer generated by normal distribution: " << element;
				throw std::exception(error_message.str().c_str());
			}
			else
			{
				fractpart = modf(element, &intpart);
				auto integral_element = fractpart < 0.5 ? int(intpart) : int(intpart + 1);
				stream.push_back(integral_element);
			}
		}
	}

	static void generate_histogram(const std::vector<int>& stream)
	{
		std::map<uint64_t, uint64_t> histogram;
		for (auto it : stream)
		{
			++histogram[it];
		}
		for (auto p : histogram)
		{
			std::cout << std::fixed << std::setprecision(1) << std::setw(2) << p.first << ' ' <<
				std::string(p.second / 200, '*') << '\n';
		}
	}

	static void generate_lognormal_stream(unsigned int length, uint64_t max, std::vector<int>& stream)
	{
		double fractpart, intpart;
		std::stringstream error_message;
		if (length <= 0 || max <= 0)
		{
			error_message << "Non-negative length and maximum value have to be provided.";
			throw std::exception(error_message.str().c_str());
		}
		double mean = double(max) / 2;
		double stddev = (max - mean) / 100;
		std::random_device rd;
		std::mt19937 gen(rd());
		std::lognormal_distribution<> d(mean, stddev);
		while (stream.size() < length)
		{
			double element = d(gen);
			if (element < 0)
			{
				error_message << "Negative integer generated by log-normal distribution: " << element;
				throw std::exception(error_message.str().c_str());
			}
			else
			{
				fractpart = modf(element, &intpart);
				auto integral_element = fractpart < 0.5 ? int(intpart) : int(intpart + 1);
				stream.push_back(integral_element);
			}
		}
	}
};
#endif // 
