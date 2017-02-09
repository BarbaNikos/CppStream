#pragma once
#include <vector>
#include <unordered_set>

#ifndef BASIC_WINDOW_H_
#define BASIC_WINDOW_H_
namespace WindowLib
{
	template<class T>
	class BasicWindow
	{
	public:
		BasicWindow(size_t task_number, int64_t start_t, int64_t end_t);
		~BasicWindow();
		void set_time(int64_t start_t, int64_t end_t);
		void init();
		int64_t start_t;
		int64_t end_t;
		std::vector<std::unordered_set<T>> cardinality;
		std::vector<uint64_t> byte_state;
		std::vector<uint64_t> count;
	};
}

template<class T>
WindowLib::BasicWindow<T>::BasicWindow(size_t task_number, int64_t start_t, int64_t end_t) :
	cardinality(task_number, std::unordered_set<T>()), byte_state(task_number, uint64_t(0)), count(task_number, uint64_t(0))
{
	this->start_t = start_t;
	this->end_t = end_t;
}

template<class T>
WindowLib::BasicWindow<T>::~BasicWindow()
{
}

template<class T>
void WindowLib::BasicWindow<T>::set_time(int64_t start_t, int64_t end_t)
{
	this->start_t = start_t;
	this->end_t = end_t;
}

template<class T>
void WindowLib::BasicWindow<T>::init()
{
	auto byte_it = byte_state.begin();
	auto count_it = count.begin();
	for (auto it = cardinality.begin(); it != cardinality.end(); ++it)
	{
		it->clear();
		*byte_it = uint64_t(0);
		++byte_it;
		*count_it = uint64_t(0);
		++count_it;
	}
}
#endif // !BASIC_WINDOW_H_