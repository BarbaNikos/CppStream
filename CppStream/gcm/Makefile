include ../Makefile.inc

EXE = gcm_main
GCM_OBJS = google_cluster_monitor_query.o main.o
SRC_DIR = src
BIT_UTIL_DIR = ../bit_util
BIT_UTIL_INCLUDE_DIR = $(BIT_UTIL_DIR)/include
BIT_UTIL_LIB = $(BIT_UTIL_DIR)/libbitutil.a
STREAM_PART_DIR = ../stream_partition_lib
STREAM_PART_INCLUDE_DIR = $(STREAM_PART_DIR)/include
STREAM_PART_LIB = $(STREAM_PART_DIR)/libstreampart.a

INCLUDE_HEADERS = $(BIT_UTIL_INCLUDE_DIR) $(STREAM_PART_INCLUDE_DIR)
INC_PARAMS =  $(foreach d, $(INCLUDE_HEADERS), -I$d)

all: $(EXE)

$(EXE): $(GCM_OBJS)
	$(CC) $(FLAGS) -o $(EXE) $(GCM_OBJS) $(STREAM_PART_LIB) $(BIT_UTIL_LIB) $(LIBS) $(INC_PARAMS)

main.o: $(SRC_DIR)/main.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)/main.cpp $(INC_PARAMS)

google_cluster_monitor_query.o : $(SRC_DIR)/google_cluster_monitor_query.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)/google_cluster_monitor_query.cpp $(INC_PARAMS)

clean:
	rm -rf $(GCM_OBJS)
	rm -rf $(EXE)
