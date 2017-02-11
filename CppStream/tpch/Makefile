include ../Makefile.inc

EXE = tpch_main
TPCH_OBJS = tpch_query_lib.o main.o
SRC_DIR = src/
BIT_UTIL_DIR = ../bit_util/
BIT_UTIL_INCLUDE_DIR = $(BIT_UTIL_DIR)include/
BIT_UTIL_LIB = $(BIT_UTIL_DIR)libbitutil.a
#HLL_DIR = ../hll/
#HLL_INCLUDE_DIR = $(HLL_DIR)include/
#HLL_LIB = $(HLL_DIR)libhll.a
STREAM_PART_DIR = ../stream_partition_lib/
STREAM_PART_INCLUDE_DIR = $(STREAM_PART_DIR)include/
STREAM_PART_LIB = $(STREAM_PART_DIR)libstreampart.a

INCLUDE_HEADERS = $(BIT_UTIL_INCLUDE_DIR) $(STREAM_PART_INCLUDE_DIR)
INC_PARAMS = $(foreach d, $(INCLUDE_HEADERS), -I$d)

all: $(EXE)

$(EXE): $(TPCH_OBJS)
	$(CC) $(FLAGS) $(LDFLAGS) -o $(EXE) $(TPCH_OBJS) $(STREAM_PART_LIB) $(BIT_UTIL_LIB) $(LIBS) $(INC_PARAMS)

main.o: $(SRC_DIR)main.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)main.cpp $(INC_PARAMS)

tpch_query_lib.o: $(SRC_DIR)tpch_query_lib.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)tpch_query_lib.cpp $(INC_PARAMS)

clean:
	rm -rf $(TPCH_OBJS)
	rm -rf $(EXE)
