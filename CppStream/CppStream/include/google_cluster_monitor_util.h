#pragma once
#include <iostream>
#include <vector>
#include <sstream>

#ifndef GOOGLE_CLUSTER_MONITOR_UTIL_H_
#define GOOGLE_CLUSTER_MONITOR_UTIL_H_
namespace Experiment
{
	namespace GoogleClusterMonitor
	{
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
			int scheduling_class; // not required 7
			int priority; // required 8
			float cpu_request; // not required 9 
			float ram_request; // not required 10
			float disk_request; // not required 11
			bool constraints; // not required 12
		}task_event;
	}
}
#endif // !GOOGLE_CLUSTER_MONITOR_UTIL_H_
