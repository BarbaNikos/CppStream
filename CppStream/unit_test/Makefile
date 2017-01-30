include ../Makefile.inc

GTEST_DIR = ../googletest/googletest

USER_DIR = src

CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11

CXXFLAGS += -g -Wall -Wextra -pthread

TESTS = unit_test

GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
		$(GTEST_DIR)/include/gtest/internal/*.h

BIT_UTIL_HEADERS = ../bit_util/include

HLL_HEADERS = ../hll/include

all : $(TESTS)

clean : 
	rm -f $(TESTS) gtest.a gtest_main.a *.o

# Builds gtest.a and gtest_main.a.

# Usually you shouldn't tweak such internal variables, indicated by a
# trailing _.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

# For simplicity and to avoid depending on Google Test's
# implementation details, the dependencies specified below are
# conservative and not optimized.  This is fine as Google Test
# compiles fast and for ordinary users its source rarely changes.
gtest-all.o : $(GTEST_SRCS_)
	$(CC) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CC) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

# user code

bit_util.o : ../bit_util/src/bit_util.cpp
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c ../bit_util/src/bit_util.cpp

bit_trick_utils_test.o : $(USER_DIR)/bit_trick_utils_test.cpp $(GTEST_HEADERS)
	$(CC) $(CPPFLAGS) -I$(BIT_UTIL_HEADERS) $(CXXFLAGS) -c $(USER_DIR)/bit_trick_utils_test.cpp

c_hll.o : ../hll/src/c_hll.cpp 
	$(CC) $(CPPFLAGS) -I$(BIT_UTIL_HEADERS) $(CXXFLAGS) -c ../hll/src/c_hll.cpp

hll_unit_test.o : $(USER_DIR)/hll_unit_test.cpp
	$(CC) $(CPPFLAGS) -I$(BIT_UTIL_HEADERS) -I$(HLL_HEADERS) $(CXXFLAGS) -c $(USER_DIR)/hll_unit_test.cpp

unit_test : bit_util.o bit_trick_utils_test.o c_hll.o hll_unit_test.o gtest_main.a
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -lpthread $^ -o $@
