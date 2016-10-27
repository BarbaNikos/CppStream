#pragma once
#include <iostream>
#include <vector>
#include <sstream>
#include <cstdio>
#include <exception>

#ifdef _WIN32
#include <Windows.h>
#include <tchar.h>
#include <strsafe.h>
#else // _WIN32
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#endif // !_WIN32


#ifndef GOOGLE_CLUSTER_MONITOR_UTIL_H_
#define GOOGLE_CLUSTER_MONITOR_UTIL_H_
namespace Experiment
{
	namespace GoogleClusterMonitor
	{
		class Util
		{
			public:
			static bool ends_with(std::string const & value, std::string const & ending);
			static void get_files(std::vector<std::string>& file_list, const std::string& directory);
		};

		typedef struct task_event_str
		{
			task_event_str() {}
			task_event_str(const std::string& task_event_info) 
			{
				deserealize(task_event_info);
			}
			task_event_str(const task_event_str& o)
			{
				timestamp = o.timestamp;
				missing_info = o.missing_info;
				job_id = o.job_id;
				task_index = o.task_index;
				machine_id = o.machine_id;
				event_type = o.event_type;
				user_name = o.user_name;
				scheduling_class = o.scheduling_class;
				priority = o.priority;
				cpu_request = o.cpu_request;
				ram_request = o.ram_request;
				disk_request = o.disk_request;
				constraints = o.constraints;
			}
			task_event_str& operator= (const task_event_str& o)
			{
				if (this != &o)
				{
					timestamp = o.timestamp;
					missing_info = o.missing_info;
					job_id = o.job_id;
					task_index = o.task_index;
					machine_id = o.machine_id;
					event_type = o.event_type;
					user_name = o.user_name;
					scheduling_class = o.scheduling_class;
					priority = o.priority;
					cpu_request = o.cpu_request;
					ram_request = o.ram_request;
					disk_request = o.disk_request;
					constraints = o.constraints;
				}
				return *this;
			}
			void deserealize(const std::string& serial_task_event)
			{
				int iter_count = 0;
				std::stringstream str_stream(serial_task_event);
				std::string token;
				while (getline(str_stream, token, ','))
				{
					switch (iter_count)
					{
					case 0:
						timestamp = token.length() > 0 ? std::stoll(token) : 0l;
						break;
					case 1:
						missing_info = token.length() > 0 ? std::stoi(token) : 0;
						break;
					case 2:
						job_id = token.length() > 0 ? std::stoll(token) : 0;
						break;
					case 3:
						task_index = token.length() > 0 ? std::stoll(token) : 0;
						break;
					case 4:
						machine_id = token.length() > 0 ? std::stoll(token) : 0;
						break;
					case 5:
						event_type = token.length() > 0 ? std::stoi(token) : 0;
						break;
					case 6:
						user_name = token;
						break;
					case 7:
						scheduling_class = token.length() > 0 ? std::stoi(token) : 0;
						break;
					case 8:
						priority = token.length() > 0 ? std::stoi(token) : 0;
						break;
					case 9:
						cpu_request = token.length() > 0 ? std::stof(token) : 0;
						break;
					case 10:
						ram_request = token.length() > 0 ? std::stof(token) : 0;
						break;
					case 11:
						disk_request = token.length() > 0 ? std::stof(token) : 0;
						break;
					case 12:
						constraints = token.length() > 0 ? std::stoi(token) : 0;
					default:
						break;
					}
					iter_count++;
				}
			}

			long timestamp; // required 0
			short missing_info; // not-required 1
			long job_id; // required 2
			long task_index; // required 3
			long machine_id; // not-required 4
			short event_type; // required 5
			std::string user_name; // not required hashed 6
			int scheduling_class; // not required 7 (category based on saber paper definition)
			int priority; // required 8
			float cpu_request; // not required 9 
			float ram_request; // not required 10
			float disk_request; // not required 11
			bool constraints; // not required 12
		}task_event;
	}
}
#endif // !GOOGLE_CLUSTER_MONITOR_UTIL_H_

inline bool Experiment::GoogleClusterMonitor::Util::ends_with(std::string const & value, std::string const & ending)
{
	if (ending.size() > value.size())
	{
		return false;
	}
	return std::equal(ending.rbegin(), ending.rend(), value.rbegin());
}

inline void Experiment::GoogleClusterMonitor::Util::get_files(std::vector<std::string>& file_list, const std::string& directory)
{
	#ifdef _WIN32
	HANDLE dir;
	WIN32_FIND_DATA file_data;
	if ((dir = FindFirstFile((directory + "/*").c_str(), &file_data)) == INVALID_HANDLE_VALUE)
		return; /* No files found */
	do {
		const std::string file_name = file_data.cFileName;
		const std::string full_file_name = directory + "/" + file_name;
		const bool is_directory = (file_data.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0;
		if (file_name[0] == '.')
			continue;
		if (is_directory)
			continue;
		file_list.push_back(full_file_name);
	} while (FindNextFile(dir, &file_data));
	FindClose(dir);
	#else // _WIN32
	DIR *dir;
	struct dirent* ent;
	struct stat st;
	dir = opendir(directory.c_str());
	while ((ent = readdir(dir)) != NULL)
	{
		const std::string file_name = ent->d_name;
		const std::string full_file_name = directory + "/" + file_name;
		if (file_name[0] == '.')
			continue;
		if (stat(full_file_name.c_str(), &st) == -1)
			continue;
		const bool is_directory = (st.st_mode & S_IFDIR) != 0;
		if (is_directory)
			continue;
		file_list.push_back(full_file_name);
	}
	closedir(dir);
	#endif
}