#include <iostream>
#include <vector>

#ifndef NON_NEGATIVE_STREAMGEN_H_
#include "../include/nonnegative_streamgen.h"
#endif

int main(int argc, char** argv)
{
	std::vector<uint64_t> stream;
	StreamGenerator::generate_lognormal_stream(10, 5, stream);
	return 0;
}