#include <iostream>
#include <vector>
#include <ctime>
#include <cinttypes>

#include "MurmurHash3.h"
#include "WindowLoad.h"

#ifndef WINDOW_PARTITIONER_H_
#define WINDOW_PARTITIONER_H_
template <class T>
class WindowPartitioner
{
public:
	WindowPartitioner(uint64_t window, uint64_t slide, const std::vector<uint16_t> tasks, size_t buffer_size);
	~WindowPartitioner();
	int16_t partition_next(time_t timestamp, const T& key, size_t key_len);
private:
	std::vector<uint16_t> tasks;
	WindowLoad<T> windowLoad;
};
#endif // !WINDOW_PARTITIONER_H_

template <class T>
inline WindowPartitioner<T>::WindowPartitioner(uint64_t window, uint64_t slide, const std::vector<uint16_t> tasks, size_t buffer_size) : 
	tasks(tasks), windowLoad(window, slide, tasks, buffer_size)
{
}

template<class T>
inline WindowPartitioner<T>::~WindowPartitioner()
{
}

template <class T>
inline int16_t WindowPartitioner<T>::partition_next(time_t timestamp, const T& key, size_t key_len)
{
	uint32_t first_choice, second_choice;
	MurmurHash3_x86_32((void*) &key, key_len, 13, &first_choice);
	MurmurHash3_x86_32((void*) &key, key_len, 17, &second_choice);
	first_choice = first_choice % tasks.size();
	second_choice = second_choice % tasks.size();
	// sag policy
	auto first_card = windowLoad.get_cardinality(first_choice);
	auto second_card = windowLoad.get_cardinality(second_choice);
	uint16_t selected_choice = first_card > second_card ? second_choice : first_choice;
	windowLoad.add(timestamp, key, key_len, selected_choice);
	return 0;
}
