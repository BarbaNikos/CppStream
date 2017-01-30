#include <iostream>
#include <vector>

#ifndef NON_NEGATIVE_STREAMGEN_H_
#include "../include/nonnegative_streamgen.h"
#endif

int main(int argc, char** argv)
{
	std::vector<int> stream;
	try
	{
		//StreamGenerator::generate_normal_stream(100000, 1000, stream);
		StreamGenerator::generate_lognormal_stream(100000, 1000, stream);
	} catch(const std::exception& e)
	{
		std::cout << e.what();
	}
	
	StreamGenerator::generate_histogram(stream);
	return 0;
}