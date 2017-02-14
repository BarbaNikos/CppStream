include Makefile.inc

DIRS = bit_util hll stream_partition_lib tpch debs gcm

all: bit_util hll stream_partition_lib tpch debs gcm unit_test

.PHONY: bit_util 
bit_util: 
	make -C bit_util/

.PHONY: hll
hll:
	make -C hll/

.PHONY: stream_partition_lib
stream_partition_lib:
	make -C stream_partition_lib/

.PHONY: tpch
tpch: 
	make -C tpch/

.PHONY: debs
debs:
	make -C debs/

.PHONY: gcm
gcm:
	make -C gcm/

.PHONY: unit_test
unit_test:
	make -C unit_test/

clean:
	make clean -C bit_util/
	make clean -C hll/
	make clean -C stream_partition_lib/
	make clean -C tpch/
	make clean -C debs/
	make clean -C gcm/
	make clean -C unit_test/
