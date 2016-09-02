#pragma once
#include "BasicWindow.h"
#include "CircularBuffer.h"

#include <vector>
#include <limits>
#include <algorithm>

#ifndef WINDOW_LOAD_H_
#define WINDOW_LOAD_H_
template <class T>
class WindowLoad
{
public:
	WindowLoad(uint64_t window, uint64_t slide, const std::vector<uint16_t>& tasks, size_t buffer_size);
	~WindowLoad();
	void add(__int64 time_t, T key, size_t key_length, uint16_t task_index);
	uint64_t get_max_cardinality();
	uint64_t get_min_cardinality();
	uint64_t get_cardinality(uint16_t task_index);
	uint64_t get_count(uint16_t task_index);
	uint64_t get_max_count();
	uint64_t get_min_count();
private:
	void remove_last_window();
	size_t task_number;
	CircularBuffer<T> ring_buffer;
	uint64_t slide;
	uint64_t window;
	size_t buffer_size;
	std::vector<uint16_t> tasks;
	std::vector<uint64_t> count;
	std::vector<uint64_t> state;
	std::vector<std::unordered_set<T>> cardinality;
};
#endif // !WINDOW_LOAD_H_

template<class T>
inline WindowLoad<T>::WindowLoad(uint64_t window, uint64_t slide, const std::vector<uint16_t>& tasks, size_t buffer_size) : 
	ring_buffer(tasks.size(), buffer_size), tasks(tasks), count(tasks.size(), uint64_t(0)), state(tasks.size(), uint64_t(0)), 
	cardinality(tasks.size(), std::unordered_set<T>())
{
	this->task_number = tasks.size();
	this->slide = slide;
	this->tasks = tasks;
	this->buffer_size = buffer_size;
}

template<class T>
inline WindowLoad<T>::~WindowLoad()
{
}

template<class T>
inline void WindowLoad<T>::add(__int64 time_t, T key, size_t key_length, uint16_t task_index)
{
	std::vector<BasicWindow<T>>& buffer_ref = ring_buffer.get_buffer();
	uint16_t buffer_head = this->ring_buffer.get_head();
	if (ring_buffer.is_empty())
	{
		buffer_ref[buffer_head].set_time(time_t, time_t + slide);
		buffer_ref[buffer_head].count[task_index] += 1;
		buffer_ref[buffer_head].cardinality[task_index].insert(key);
		buffer_ref[buffer_head].byte_state[task_index] += key_length;

		cardinality[task_index].insert(key);
		count[task_index] += 1;
		state[task_index] += key_length;

		ring_buffer.increase_size();
	}
	else
	{
		if (buffer_ref[buffer_head].start_t <= time_t && time_t <= buffer_ref[buffer_head].end_t)
		{
			buffer_ref[buffer_head].count[task_index] += 1;
			count[task_index] += 1;
			// TODO: Need to think what happens with varying cardinalities of different operations
			auto search = buffer_ref[buffer_head].cardinality[task_index].find(key);
			if (search == buffer_ref[buffer_head].cardinality[task_index].end())
			{
				buffer_ref[buffer_head].cardinality[task_index].insert(key);
				buffer_ref[buffer_head].byte_state[task_index] += key_length;
			}
		}
		else
		{
			if (ring_buffer.is_full())
			{
				remove_last_window();
			}
			ring_buffer.progress_head();
			
			buffer_head = ring_buffer.get_head();

			buffer_ref[buffer_head].set_time(time_t, time_t + slide);
			buffer_ref[buffer_head].init();
			buffer_ref[buffer_head].count[task_index] += 1;
			buffer_ref[buffer_head].cardinality[task_index].insert(key);
			buffer_ref[buffer_head].byte_state[task_index] += key_length;
			
			cardinality[task_index].insert(key);
			count[task_index] += 1;
			state[task_index] += key_length;

			ring_buffer.increase_size();
		}
	}
}

template<class T>
inline uint64_t WindowLoad<T>::get_max_cardinality()
{
	uint64_t max_value = std::numeric_limits<uint64_t>::min();
	for (size_t i = 0; i < cardinality.size(); ++i)
	{
		if (cardinality[i].size() > max_value)
		{
			max_value = cardinality[i].size();
		}
	}
	return max_value;
}

template<class T>
inline uint64_t WindowLoad<T>::get_min_cardinality()
{
	uint64_t min_value = std::numeric_limits<uint64_t>::max();
	for (size_t i = 0; i < cardinality.size(); ++i)
	{
		if (cardinality[i].size() < min_value)
		{
			min_value = cardinality[i].size();
		}
	}
	return min_value;
}

template<class T>
inline uint64_t WindowLoad<T>::get_cardinality(uint16_t task_index)
{
	return cardinality[task_index].size();
}

template<class T>
inline uint64_t WindowLoad<T>::get_count(uint16_t task_index)
{
	return count[task_index];
}

template<class T>
inline uint64_t WindowLoad<T>::get_max_count()
{
	return *std::max_element(count.begin(), count.end());
}

template<class T>
inline uint64_t WindowLoad<T>::get_min_count()
{
	return *std::min_element(count.begin(), count.end());
}

template<class T>
inline void WindowLoad<T>::remove_last_window()
{
	BasicWindow<T>* last_window = ring_buffer.peek_tail();
	if (last_window == nullptr)
	{
		return;
	}
	else
	{
		std::vector<BasicWindow<T>>& buffer_ref = ring_buffer.get_buffer();
		uint16_t buffer_head = ring_buffer.get_head();
		uint16_t window_it = ring_buffer.get_tail();
		do 
		{
			window_it = window_it < buffer_ref.size() - 1 ? window_it + 1 : 0;
			for (size_t i = 0; i < task_number; ++i)
			{
				std::unordered_set<T>& task_keys = buffer_ref[window_it].cardinality[i];
				for (std::unordered_set<T>::const_iterator it = task_keys.begin(); it != task_keys.end(); ++it)
				{
					auto pos = last_window->cardinality[i].find(*it);
					if (pos != last_window->cardinality[i].end())
					{
						last_window->cardinality[i].erase(pos);
					}
				}
			}
		} while (window_it != buffer_head);
		for (size_t i = 0; i < tasks.size(); ++i)
		{
			count[i] -= last_window->count[i];
			state[i] -= last_window->byte_state[i];
			for (auto it = last_window->cardinality[i].begin(); it != last_window->cardinality[i].begin(); ++it)
			{
				cardinality[i].erase(*it);
			}
		}
	}
}
