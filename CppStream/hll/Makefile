include ../Makefile.inc

HLL_LIB = libhll.a
HLL_OBJS = c_hll.o
SRC_DIR = src/

BIT_UTIL_DIR = ../bit_util/
BIT_UTIL_INCLUDE_DIR = $(BIT_UTIL_DIR)include/
BIT_UTIL_LIB = $(BIT_UTIL_DIR)libbitutil.a

all: $(HLL_LIB)

$(HLL_LIB): $(HLL_OBJS)
	$(AR) rcs $(HLL_LIB) $(BIT_UTIL_LIB) $(HLL_OBJS)

c_hll.o : $(SRC_DIR)c_hll.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)c_hll.cpp -I$(BIT_UTIL_INCLUDE_DIR)

clean:
	rm -rf $(HLL_OBJS)
	rm -rf $(HLL_LIB)
