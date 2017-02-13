include ../Makefile.inc

EXE = debs_main
DEBS_OBJS = debs_cell_coordinate_util.o debs_query_lib.o main.o
SRC_DIR = src
BIT_UTIL_DIR = ../bit_util
BIT_UTIL_INCLUDE_DIR = $(BIT_UTIL_DIR)/include
BIT_UTIL_LIB = $(BIT_UTIL_DIR)/libbitutil.a
HLL_DIR = ../hll
HLL_INCLUDE_DIR = $(HLL_DIR)/include
HLL_LIB = $(HLL_DIR)/libhll.a
STREAM_PART_DIR = ../stream_partition_lib
STREAM_PART_INCLUDE_DIR = $(STREAM_PART_DIR)/include
STREAM_PART_LIB = $(STREAM_PART_DIR)/libstreampart.a

INCLUDE_HEADERS = $(BIT_UTIL_INCLUDE_DIR) $(HLL_INCLUDE_DIR) $(STREAM_PART_INCLUDE_DIR)
INC_PARAMS = $(foreach d, $(INCLUDE_HEADERS), -I$d)

all: $(EXE)

$(EXE): $(DEBS_OBJS)
	$(CC) $(FLAGS) -o $(EXE) $(DEBS_OBJS) $(STREAM_PART_LIB) $(HLL_LIB) $(BIT_UTIL_LIB) $(LIBS) $(INC_PARAMS)

main.o: $(SRC_DIR)/main.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)/main.cpp $(INC_PARAMS)

debs_query_lib.o : $(SRC_DIR)/debs_query_lib.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)/debs_query_lib.cpp $(INC_PARAMS)

debs_cell_coordinate_util.o : $(SRC_DIR)/debs_cell_coordinate_util.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)/debs_cell_coordinate_util.cpp $(INC_PARAMS)

clean:
	rm -rf $(DEBS_OBJS)
	rm -rf $(EXE)
