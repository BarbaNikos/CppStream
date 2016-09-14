#pragma once
#include <iostream>
#include <cstdlib>
#include <cinttypes>
#include <vector>

#include "BasicWindow.h"

#ifndef CIRCULAR_BUFFER_H_
#define CIRCULAR_BUFFER_H_

template <class T>
class CircularBuffer
{
public:
	CircularBuffer(size_t task_number, size_t buffer_size);
	~CircularBuffer();
	void increase_size();
	void decrease_size();
	void progress_head();
	void progress_tail();
	bool is_empty();
	bool is_full();
	size_t get_size();
	WindowLib::BasicWindow<T>* peek_tail();
	WindowLib::BasicWindow<T>** get_buffer();
	uint16_t get_head();
	uint16_t get_tail();
private:
	WindowLib::BasicWindow<T>** _buffer;
	uint16_t head;
	uint16_t tail;
	size_t size;
	size_t buffer_size;
};

template<class T>
inline CircularBuffer<T>::CircularBuffer(size_t task_number, size_t buffer_size)
{
	_buffer = new WindowLib::BasicWindow<T>*[buffer_size];
	for (size_t i = 0; i < buffer_size; ++i)
	{
		_buffer[i] = new WindowLib::BasicWindow<T>(task_number, int64_t(0), int64_t(0));
	}
	this->buffer_size = buffer_size;
	head = uint16_t(0);
	tail = uint16_t(0);
	size = size_t(0);
}

template<class T>
inline CircularBuffer<T>::~CircularBuffer()
{
	for (size_t i = 0; i < buffer_size; ++i)
	{
		delete _buffer[i];
	}
	delete[] _buffer;
}

template<class T>
inline void CircularBuffer<T>::increase_size()
{
	if (size < buffer_size)
	{
		size += 1;
	}
}

template<class T>
inline void CircularBuffer<T>::decrease_size()
{
	if (size > 0)
	{
		size -= 1;
	}
}

template<class T>
inline void CircularBuffer<T>::progress_head()
{
	head = head == buffer_size - 1 ? 0 : head + 1;
}

template<class T>
inline void CircularBuffer<T>::progress_tail()
{
	tail = tail == buffer_size - 1 ? size_t(0) : tail + 1;
}

template<class T>
inline bool CircularBuffer<T>::is_empty()
{
	return size == size_t(0);
}

template<class T>
inline bool CircularBuffer<T>::is_full()
{
	return size == buffer_size;
}

template<class T>
inline size_t CircularBuffer<T>::get_size()
{
	return size;
}

template<class T>
inline WindowLib::BasicWindow<T>* CircularBuffer<T>::peek_tail()
{
	if (is_empty())
	{
		return nullptr;
	}
	else
	{
		return _buffer[tail];
	}
}

template<class T>
inline WindowLib::BasicWindow<T>** CircularBuffer<T>::get_buffer()
{
	return _buffer;
}

template<class T>
inline uint16_t CircularBuffer<T>::get_head()
{
	return head;
}

template<class T>
inline uint16_t CircularBuffer<T>::get_tail()
{
	return tail;
}
#endif // !CIRCULAR_BUFFER_H_
