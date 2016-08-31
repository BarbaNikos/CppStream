#pragma once
#include <vector>
#include <unordered_set>

#ifndef BASIC_WINDOW_H_
#define BASIC_WINDOW_H_

template<class T>
class BasicWindow
{
public:
	BasicWindow(const std::vector<uint16_t> tasks, __int64 start_t, __int64 end_t);
	~BasicWindow();
	void set_time(__int64 start_t, __int64 end_t);
	void init();
	__int64 start_t;
	__int64 end_t;
	std::vector<uint16_t> tasks;
	std::vector<std::unordered_set<T>> cardinality;
	std::vector<uint64_t> byte_state;
	std::vector<uint64_t> count;
};
#endif // !BASIC_WINDOW_H_

template<class T>
inline BasicWindow<T>::BasicWindow(const std::vector<uint16_t> tasks, __int64 start_t, __int64 end_t) :
	byte_state(tasks.size(), uint64_t(0)), count(tasks.size(), uint64_t(0)), cardinality(tasks.size(), std::unordered_set<T>())
{
	this->start_t = start_t;
	this->end_t = end_t;
	this->tasks = tasks;
}

template<class T>
inline BasicWindow<T>::~BasicWindow()
{
	for (size_t i = 0; i < tasks.size(); ++i)
	{
		cardinality[i].clear();
	}
}

template<class T>
inline void BasicWindow<T>::set_time(__int64 start_t, __int64 end_t)
{
	this->start_t = start_t;
	this->end_t = end_t;
}

template<class T>
inline void BasicWindow<T>::init()
{
	byte_state.clear();
	count.clear();
	for (size_t i = 0; i < cardinality.size(); ++i)
	{
		cardinality[i].clear();
	}
}
