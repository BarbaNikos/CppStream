#include <iostream>
#include <queue>
#include <unordered_map>

#include <thread>
#include <future>

#include "tpch_util.h"

#ifndef TPCH_QUERY_H_
#define TPCH_QUERY_H_
namespace Tpch
{
	typedef struct
	{
		float sum_qty;
		float sum_base_price;
		float sum_disc_price;
		float sum_charge;
		float avg_qty;
		float avg_price;
		float avg_disc;
		uint32_t count_order;
	}query_one_result;

	class QueryOne
	{
	public:
		QueryOne(std::queue<Tpch::lineitem>* input_queue, std::mutex* mu, std::condition_variable* cond);
		~QueryOne();
		void operate();
		void update(Tpch::lineitem& line_item);
		void finalize();
	private:
		std::mutex* mu;
		std::condition_variable* cond;
		std::unordered_map<std::string, Tpch::query_one_result> result;
		std::queue<Tpch::lineitem>* input_queue;
	};
}

Tpch::QueryOne::QueryOne(std::queue<Tpch::lineitem>* input_queue, std::mutex* mu, std::condition_variable* cond)
{
	this->mu = mu;
	this->cond = cond;
	this->input_queue = input_queue;
}

inline Tpch::QueryOne::~QueryOne()
{
	result.clear();
}

inline void Tpch::QueryOne::operate()
{
	while (true)
	{
		std::unique_lock<std::mutex> locker(*mu);
		cond->wait(locker, [this]() { return input_queue->size() > 0; });
		Tpch::lineitem line_item = input_queue->back();
		input_queue->pop();
		// process
		if (line_item.l_linenumber >= 0)
		{
			update(line_item);
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

void Tpch::QueryOne::update(Tpch::lineitem& line_item)
{
	std::string key = std::to_string(line_item.l_returnflag) + std::to_string(line_item.l_linestatus);
	std::unordered_map<std::string, Tpch::query_one_result>::iterator it = result.find(key);
	if (it != result.end())
	{
		it->second.sum_qty += line_item.l_quantity;
		it->second.sum_base_price += line_item.l_extendedprice;
		it->second.sum_disc_price += (line_item.l_extendedprice * (1 - line_item.l_discount));
		it->second.sum_charge += (line_item.l_extendedprice * (1 - line_item.l_discount) * (1 + line_item.l_tax));
		it->second.avg_qty += line_item.l_quantity;
		it->second.avg_price += line_item.l_extendedprice;
		it->second.avg_disc += line_item.l_discount;
		it->second.count_order += 1;
	}
	else
	{
		Tpch::query_one_result tmp;
		tmp.sum_qty = line_item.l_quantity;
		tmp.sum_base_price = line_item.l_extendedprice;
		tmp.sum_disc_price = line_item.l_extendedprice * (1 - line_item.l_discount);
		tmp.sum_charge = line_item.l_extendedprice * (1 - line_item.l_discount) * (1 + line_item.l_tax);
		tmp.avg_qty = line_item.l_quantity;
		tmp.avg_price = line_item.l_extendedprice;
		tmp.avg_disc = line_item.l_discount;
		tmp.count_order = 1;
		result.insert(std::make_pair(key, tmp));
	}
}

inline void Tpch::QueryOne::finalize()
{
	std::cout << "number of groups: " << result.size() << ".\n";
	for (std::unordered_map<std::string, Tpch::query_one_result>::iterator it = result.begin(); it != result.end(); ++it)
	{
		it->second.avg_qty = it->second.avg_qty / it->second.count_order;
		it->second.avg_price = it->second.avg_price / it->second.count_order;
		it->second.avg_disc = it->second.avg_disc / it->second.count_order;
	}
}
#endif // !TPCH_QUERY_H_
