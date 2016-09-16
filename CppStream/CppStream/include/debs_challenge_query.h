#include <iostream>
#include <queue>
#include <unordered_map>

#include <thread>
#include <future>

#include "debs_challenge_util.h"

#ifndef DEBS_CHALLENGE_QUERY_H_
#define DEBS_CHALLENGE_QUERY_H_
namespace DebsChallenge
{
	typedef struct
	{
		std::pair<uint8_t, uint8_t> pickup_cell;
		std::pair<uint8_t, uint8_t> dropoff_cell;
		uint64_t count;
	}frequent_route_result;

	class FrequentRoute
	{
	public:
		FrequentRoute(std::queue<DebsChallenge::Ride>* input_queue, std::mutex* mu, std::condition_variable* cond);
		~FrequentRoute();
		void operate();
		void update(DebsChallenge::Ride& ride);
		void finalize();
	private:
		std::mutex* mu;
		std::condition_variable* cond;
		std::unordered_map<std::string, DebsChallenge::frequent_route_result> result;
		std::queue<DebsChallenge::Ride>* input_queue;
	};
}

DebsChallenge::FrequentRoute::FrequentRoute(std::queue<DebsChallenge::Ride>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

DebsChallenge::FrequentRoute::~FrequentRoute()
{
	result.clear();
}

inline void DebsChallenge::FrequentRoute::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		DebsChallenge::Ride ride = input_queue->back();
		input_queue->pop();
		// process
		if (ride.trip_time_in_secs >= 0)
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

void DebsChallenge::FrequentRoute::update(DebsChallenge::Ride& ride)
{
	std::string key = std::to_string(ride.pickup_cell.first) + "." + 
		std::to_string(ride.pickup_cell.second) + "," + 
		std::to_string(ride.dropoff_cell.first) + "." + 
		std::to_string(ride.dropoff_cell.second);
	std::unordered_map<std::string, DebsChallenge::frequent_route_result>::iterator it = result.find(key);
	if (it != result.end())
	{
		it->second.count += 1;
	}
	else
	{
		DebsChallenge::frequent_route_result tmp;
		tmp.pickup_cell = std::make_pair(ride.pickup_cell.first, ride.pickup_cell.second);
		tmp.dropoff_cell = std::make_pair(ride.dropoff_cell.first, ride.dropoff_cell.second);
		tmp.count = 1;
		result.insert(std::make_pair(key, tmp));
	}
}

void DebsChallenge::FrequentRoute::finalize()
{
	std::cout << "number of groups: " << result.size() << ".\n";
}
#endif // !DEBS_CHALLENGE_QUERY_H_
