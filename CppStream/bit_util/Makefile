include ../Makefile.inc

BIT_UTIL_OBJS = bit_util.o

BIT_UTIL_LIB = libbitutil.a

SRC_DIR = src/

all: $(BIT_UTIL_LIB)

$(BIT_UTIL_LIB): $(BIT_UTIL_OBJS)
	$(AR) rcs $(BIT_UTIL_LIB) $(BIT_UTIL_OBJS)

bit_util.o: $(SRC_DIR)bit_util.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)bit_util.cpp

clean: 
	rm -rf $(BIT_UTIL_OBJS) 
	rm -rf $(BIT_UTIL_LIB)
