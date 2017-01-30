include ../Makefile.inc

STREAM_PART_LIB = libstreampart.a
STREAM_PART_OBJS = murmurhash_3.o
SRC_DIR = src/
BIT_UTIL_DIR = ../bit_util/
BIT_UTIL_INCLUDE_DIR = $(BIT_UTIL_DIR)include/
BIT_UTIL_LIB = $(BIT_UTIL_DIR)libbitutil.a
HLL_DIR = ../hll/
HLL_INCLUDE_DIR = $(HLL_DIR)include/
HLL_LIB = $(HLL_DIR)libhll.a

INCLUDE_HEADERS = $(BIT_UTIL_INCLUDE_DIR) $(HLL_INCLUDE_DIR)
INC_PARAMS = $(foreach d, $(INCLUDE_HEADERS), -I$d)

all: $(STREAM_PART_LIB)

$(STREAM_PART_LIB): $(STREAM_PART_OBJS)
	$(AR) rcs $(STREAM_PART_LIB) $(HLL_LIB) $(BIT_UTIL_LIB) $(STREAM_PART_OBJS)

murmurhash_3.o: $(SRC_DIR)murmurhash_3.cpp $(BIT_UTIL_INCLUDE_DIR) $(HLL_INClUDE_DIR)
	$(CC) $(FLAGS) -c $(SRC_DIR)murmurhash_3.cpp $(INC_PARAMS)

clean: 
	rm -rf $(STREAM_PART_OBJS)
	rm -rf $(STREAM_PART_LIB)
