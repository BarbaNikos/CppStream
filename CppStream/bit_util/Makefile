include ../Makefile.inc

SRC_DIR: src/
INCLUDE_DIR: include/

OBJS: bit_util.o
HEADER_FILES: include/bit_util.h

BIT_LIB: libbitutil.a

all: $(BIT_LIB)

$(BIT_LIB): $(OBJS)
	ar rcs $@ $^

bit_util.o: $(SRC_DIR)bit_util.cpp
	$(CC) $(FLAGS) -c $(SRC_DIR)bit_util.cpp