CC = /opt/rh/devtoolset-2/root/usr/bin/g++				# Compiler to use
FLAGS = -std=c++11 -O2 -g -Wall						# -g for debug, -O2 for optimize, -Wall additional messages
INCLUDE_FLAGS = -I ./CppStream/include/					# include headers
LIBS = -lpthread -lm							# additional libraries
OBJS = bit_trick_utils.o cardinality_estimation_utils.o \
	main.o murmurhash_3.o

all: ${OBJS}
	${CC} ${FLAGS} ${INCLUDES} ${OBJS} -o target_bin ${LIBS}

#cag_partitioner.o: ./CppStream/src/cag_partitioner.cpp ./CppStream/include/cag_partitioner.h
#	${CC} ${FLAGS} -c ./CppStream/src/cag_partitioner.cpp ${INCLUDE_FLAGS}

main.o: ./CppStream/src/main.cpp
	${CC} ${FLAGS} -c ./CppStream/src/main.cpp ${INCLUDE_FLAGS}

CardinalityEstimator.o: ./CppStream/src/CardinalityEstimator.cpp ./CppStream/include/cardinality_estimation_utils.h
	${CC} ${FLAGS} -c ./CppStream/src/cardinality_estimation_utils.cpp ${INCLUDE_FLAGS}

MurmurHash3.o: ./CppStream/src/MurmurHash3.cpp ./CppStream/include/murmurhash_3.h
	$(CC) $(FLAGS) -c ./CppStream/src/murmurhash_3.cpp ${INCLUDE_FLAGS}

BitTrickBox.o: ./CppStream/src/BitTrickBox.cpp ./CppStream/include/bit_trick_utils.h
	$(CC) $(FLAGS) -c ./CppStream/src/bit_trick_utils.cpp ${INCLUDE_FLAGS}

clean:
	rm -rf *.o
	rm target_bin
