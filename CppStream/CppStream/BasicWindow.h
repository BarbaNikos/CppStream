#pragma once
#include <array>
#include <unordered_set>

#ifndef BASIC_WINDOW_H_
#define BASIC_WINDOW_H_

template<class T, std::size_t N>
class BasicWindow
{
public:
	BasicWindow(const std::array<__int16, N>& tasks, __int64 start_t, __int64 end_t);
	~BasicWindow();
private:
	__int64 start_t;
	__int64 end_t;
	std::array<__int16, N> tasks;
	std::array<std::unordered_set<T>, N> cardinality;
	std::array<__int64, N> byte_state;
	std::array<__int64, N> count;
};
#endif // !BASIC_WINDOW_H_

template<class T, std::size_t N>
inline BasicWindow<T, N>::BasicWindow(const std::array<__int16, N>& tasks, __int64 start_t, __int64 end_t)
{
	this->start_t = start_t;
	this->end_t = end_t;
	this->tasks = tasks;
}

template<class T, std::size_t N>
inline BasicWindow<T, N>::~BasicWindow()
{
	for (size_t i = 0; i < N; ++i)
	{
		cardinality[i].clear();
	}
}
