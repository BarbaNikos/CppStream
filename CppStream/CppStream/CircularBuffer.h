#pragma once

#include <array>
#ifndef CIRCULAR_BUFFER_H_
#define CIRCULAR_BUFFER_H_
template <class T, std::size_t N>
class CircularBuffer
{
public:
	CircularBuffer();
	~CircularBuffer();
	bool put(const T& element);
	bool is_empty();
	bool is_full();
	T* peek_tail();
	T* pop_tail();
	void print_contents();
private:
	std::array<T, N> buffer;
	uint16_t head;
	uint16_t tail;
	size_t size;
};
#endif // !CIRCULAR_BUFFER_H_

template<class T, std::size_t N>
inline CircularBuffer<T, N>::CircularBuffer()
{
	this->head = uint16_t(0);
	this->tail = uint16_t(0);
	this->size = size_t(0);
}

template<class T, std::size_t N>
inline CircularBuffer<T, N>::~CircularBuffer()
{
}

template<class T, std::size_t N>
inline bool CircularBuffer<T, N>::put(const T & element)
{
	buffer[head] = element;
	if (this->is_full())
	{
		return false;
	}
	else
	{
		buffer[head] = element;
		size++;
		head = head == buffer.size() - 1 ? 0 : head + 1;
		return true;
	}
}

template<class T, std::size_t N>
inline bool CircularBuffer<T, N>::is_empty()
{
	return size == size_t(0);
}

template<class T, std::size_t N>
inline bool CircularBuffer<T, N>::is_full()
{
	return size == buffer.size();
}

template<class T, std::size_t N>
inline T* CircularBuffer<T, N>::peek_tail()
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

template<class T, std::size_t N>
inline T* CircularBuffer<T, N>::pop_tail()
{
	if (is_empty())
	{
		return nullptr;
	}
	else
	{
		T& element = buffer[tail];
		size -= 1;
		tail = tail == buffer.size() - 1 ? size_t(0) : tail + 1;
		return &element;
	}
}

template<class T, std::size_t N>
inline void CircularBuffer<T, N>::print_contents()
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
