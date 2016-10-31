CC = /opt/rh/devtoolset-2/root/usr/bin/g++				# Compiler to use
#CC = g++
FLAGS = -std=c++11 -g -O3 -Wall						# -g for debug, -O2 for optimize, -Wall additional messages
INCLUDE_FLAGS = -I ./CppStream/include/					# include headers
LIBS = -lpthread -lm							# additional libraries
OBJS = bit_trick_utils.o cardinality_estimation_utils.o murmurhash_3.o \
debs_cell_coordinate_util.o c_hll.o debs_query_lib.o google_cluster_monitor_query.o \
tpch_query_lib.o main.o

all: ${OBJS}
	${CC} ${FLAGS} ${OBJS} -o target_bin ${LIBS}

main.o: ./CppStream/src/main.cpp
	${CC} ${FLAGS} -c ./CppStream/src/main.cpp ${INCLUDE_FLAGS}

tpch_query_lib.o: ./CppStream/src/tpch_query_lib.cpp
	${CC} ${FLAGS} -c ./CppStream/src/tpch_query_lib.cpp ${INCLUDE_FLAGS}

google_cluster_monitor_query.o: ./CppStream/src/google_cluster_monitor_query.cpp
	${CC} ${FLAGS} -c ./CppStream/src/google_cluster_monitor_query.cpp ${INCLUDE_FLAGS}

debs_query_lib.o: ./CppStream/src/debs_query_lib.cpp
	${CC} ${FLAGS} -c ./CppStream/src/debs_query_lib.cpp ${INCLUDE_FLAGS}

c_hll.o: ./CppStream/src/c_hll.c
	${CC} ${FLAGS} -c ./CppStream/src/c_hll.c ${INCLUDE_FLAGS}

debs_cell_coordinate_util.o: ./CppStream/src/debs_cell_coordinate_util.cpp
	${CC} ${FLAGS} -c ./CppStream/src/debs_cell_coordinate_util.cpp ${INCLUDE_FLAGS}

cardinality_estimation_utils.o: ./CppStream/src/cardinality_estimation_utils.cpp ./CppStream/include/cardinality_estimation_utils.h
	${CC} ${FLAGS} -c ./CppStream/src/cardinality_estimation_utils.cpp ${INCLUDE_FLAGS}

murmurhash_3.o: ./CppStream/src/murmurhash_3.cpp ./CppStream/include/murmurhash_3.h
	$(CC) $(FLAGS) -c ./CppStream/src/murmurhash_3.cpp ${INCLUDE_FLAGS}

bit_trick_utils.o: ./CppStream/src/bit_trick_utils.cpp ./CppStream/include/bit_trick_utils.h
	$(CC) $(FLAGS) -c ./CppStream/src/bit_trick_utils.cpp ${INCLUDE_FLAGS}

clean:
	rm -rf *.o
	rm target_bin
