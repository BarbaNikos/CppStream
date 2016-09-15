CC = /opt/rh/devtoolset-2/root/usr/bin/g++				# Compiler to use
FLAGS = -std=c++11 -O2 -g -Wall						# -g for debug, -O2 for optimize, -Wall additional messages
INCLUDE_FLAGS = -I ./CppStream/include/					# include headers
LIBS = -lpthread -lm							# additional libraries
OBJS = BitTrickBox.o cag_partitioner.o CardinalityEstimator.o \
	debs_challenge_util.o main.o MurmurHash3.o

all: ${OBJS}
	${CC} ${FLAGS} ${INCLUDES} ${OBJS} -o target_bin ${LIBS}

cag_partitioner.o: ./CppStream/src/cag_partitioner.cpp ./CppStream/include/cag_partitioner.h
	${CC} ${FLAGS} -c ./CppStream/src/cag_partitioner.cpp ${INCLUDE_FLAGS}

main.o: ./CppStream/src/main.cpp
	${CC} ${FLAGS} -c ./CppStream/src/main.cpp ${INCLUDE_FLAGS}

CardinalityEstimator.o: ./CppStream/src/CardinalityEstimator.cpp ./CppStream/include/CardinalityEstimator.h
	${CC} ${FLAGS} -c ./CppStream/src/CardinalityEstimator.cpp ${INCLUDE_FLAGS}

debs_challenge_util.o: ./CppStream/src/debs_challenge_util.cpp ./CppStream/include/debs_challenge_util.h
	${CC} ${FLAGS} -c ./CppStream/src/debs_challenge_util.cpp ${INCLUDE_FLAGS}

MurmurHash3.o: ./CppStream/src/MurmurHash3.cpp ./CppStream/include/MurmurHash3.h
	$(CC) $(FLAGS) -c ./CppStream/src/MurmurHash3.cpp ${INCLUDE_FLAGS}

BitTrickBox.o: ./CppStream/src/BitTrickBox.cpp ./CppStream/include/BitTrickBox.h
	$(CC) $(FLAGS) -c ./CppStream/src/BitTrickBox.cpp ${INCLUDE_FLAGS}

clean:
	rm -rf *.o
	rm target_bin
