#pragma once

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
	void progress_head();
	bool is_empty();
	bool is_full();
	BasicWindow<T>* peek_tail();
	BasicWindow<T>* pop_tail();
	void print_contents();
	std::vector<BasicWindow<T>>& get_buffer();
	uint16_t get_head();
	uint16_t get_tail();
private:
	std::vector<BasicWindow<T>> buffer;
	uint16_t head;
	uint16_t tail;
	size_t size;
};
#endif // !CIRCULAR_BUFFER_H_

template<class T>
inline CircularBuffer<T>::CircularBuffer(size_t task_number, size_t buffer_size) :
	buffer(buffer_size, BasicWindow<T>(task_number, __int64(0), __int64(0)))
{
	this->head = uint16_t(0);
	this->tail = uint16_t(0);
	this->size = size_t(0);
}

template<class T>
inline CircularBuffer<T>::~CircularBuffer()
{
	// nothing to clean
}

template<class T>
inline void CircularBuffer<T>::progress_head()
{
	if (size < buffer.size() - 1)
	{
		head = head == buffer.size() - 1 ? 0 : head + 1;
		size++;
	}
}

template<class T>
inline bool CircularBuffer<T>::is_empty()
{
	return size == size_t(0);
}

template<class T>
inline bool CircularBuffer<T>::is_full()
{
	return size == buffer.size();
}

template<class T>
inline BasicWindow<T>* CircularBuffer<T>::peek_tail()
{
	if (is_empty())
	{
		return nullptr;
	}
	else
	{
		return &(buffer[tail]);
	}
}

template<class T>
inline BasicWindow<T>* CircularBuffer<T>::pop_tail()
{
	if (is_empty())
	{
		return nullptr;
	}
	else
	{
		BasicWindow<T>* element = &(buffer[tail]);
		size -= 1;
		tail = tail == buffer.size() - 1 ? size_t(0) : tail + 1;
		return element;
	}
}

template<class T>
inline void CircularBuffer<T>::print_contents()
{
	if (size != 0)
	{
		std::cout << "Contents: ";
		uint16_t tmp = tail; 
		do
		{
			std::cout << buffer[tmp] << " ";
			tmp = tmp >= buffer.size() - 1 ? 0 : tmp + 1;
		} while (tmp != head);
		std::cout << std::endl;
	}
}

template<class T>
inline std::vector<BasicWindow<T>>& CircularBuffer<T>::get_buffer()
{
	return buffer;
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
