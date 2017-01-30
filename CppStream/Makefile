include Makefile.inc

DIRS = bit_util hll stream_partition_lib googletest unit_test debs gcm tpch

INCLUDE_FLAGS = -I ./CppStream/include/					# include headers

OBJS = murmurhash_3.o debs_cell_coordinate_util.o c_hll.o debs_query_lib.o \
google_cluster_monitor_query.o tpch_query_lib.o main.o

bit_util_lib: bit_util/src/bit_util.cpp
	${CC} ${FLAGS} -c bit_util/src/bit_util.cpp 
all: ${OBJS}
	${CC} ${FLAGS} ${OBJS} -o part_main ${LIBS}

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

murmurhash_3.o: ./CppStream/src/murmurhash_3.cpp ./CppStream/include/murmurhash_3.h
	$(CC) $(FLAGS) -c ./CppStream/src/murmurhash_3.cpp ${INCLUDE_FLAGS}

bit_trick_utils.o: ./CppStream/src/bit_trick_utils.cpp ./CppStream/include/bit_trick_utils.h
	$(CC) $(FLAGS) -c ./CppStream/src/bit_trick_utils.cpp ${INCLUDE_FLAGS}

clean:
	rm -rf *.o
	rm part_main
