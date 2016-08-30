#pragma once
#include "BasicWindow.h"

#ifndef WINDOW_LOAD_H_
#define WINDOW_LOAD_H_
template <class T, std::size_t B, std::size_t N>
class WindowLoad
{
public:
	WindowLoad(uint64_t window, uint64_t slide, const std::array<uint16_t, N> tasks);
	~WindowLoad();
	void add(uint64_t time_t, void* key, uint16_t task);
	void clear_buffer();
	void remove_window();
private:
	std::array<BasicWindow, B> ring_buffer;
	uint64_t slide;
	uint64_t window;
	uint16_t buffer_size;
	std::array<uint16_t, N> tasks;
	std::array<uint64_t, N> count;
	std::array<uint64_t, N> state;
	std::array<std::unordered_set, N> cardinality;
};
#endif // !WINDOW_LOAD_H_

template<class T, std::size_t B, std::size_t N>
inline WindowLoad<T, B, N>::WindowLoad(uint64_t window, uint64_t slide, const std::array<uint16_t, N> tasks)
{
	this->slide = slide;
	this->tasks = tasks;
	count = { 0 };
	state = { 0 };
}

template<class T, std::size_t B, std::size_t N>
inline WindowLoad<T, B, N>::~WindowLoad()
{
}
