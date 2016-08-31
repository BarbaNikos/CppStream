#pragma once
#include <vector>
#include <unordered_set>

#ifndef BASIC_WINDOW_H_
#define BASIC_WINDOW_H_

template<class T>
class BasicWindow
{
public:
	BasicWindow(size_t task_number, __int64 start_t, __int64 end_t);
	~BasicWindow();
	void set_time(__int64 start_t, __int64 end_t);
	void init();
	__int64 start_t;
	__int64 end_t;
	std::vector<std::unordered_set<T>> cardinality;
	std::vector<uint64_t> byte_state;
	std::vector<uint64_t> count;
};
#endif // !BASIC_WINDOW_H_

template<class T>
inline BasicWindow<T>::BasicWindow(size_t task_number, __int64 start_t, __int64 end_t) :
	byte_state(task_number, uint64_t(0)), count(task_number, uint64_t(0)), cardinality(task_number, std::unordered_set<T>())
{
	this->start_t = start_t;
	this->end_t = end_t;
}

template<class T>
inline BasicWindow<T>::~BasicWindow()
{
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
