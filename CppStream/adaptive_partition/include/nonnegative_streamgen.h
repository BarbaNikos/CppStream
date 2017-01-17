#pragma once
#include <iostream>
#include <vector>
#include <random>

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

	static void generate_normal_stream(unsigned int length, uint64_t max, std::vector<uint64_t>& stream)
	{
		double mean = max / 2;
		double stddev = (max - mean - 1) / 3;
		std::random_device rd;
		std::mt19937 gen(rd());
		std::normal_distribution<> d(std::round(mean), stddev);
		while (stream.size() < length)
		{
			stream.push_back(std::round(d(gen)));
		}
	}

	static void generate_lognormal_stream(unsigned int length, uint64_t max, std::vector<uint64_t>& stream)
	{
		double mean = max / 2;
		double stddev = (max - mean - 1) / 3;
		std::random_device rd;
		std::mt19937 gen(rd());
		std::lognormal_distribution<> d(std::round(mean), stddev);
		while (stream.size() < length)
		{
			stream.push_back(std::round(d(gen)));
		}
	}
};
#endif // 
