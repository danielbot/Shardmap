.PHONY: all clean

optbase=-fPIC -Wno-sign-compare # -Wno-address-of-packed-member

opt=-O3 $(optbase)

ifdef DEBUG
opt=-g -O0 -DDEBUG $(optbase)
endif

obj = utility.o pmem.o bigmap.o options.o shardmap.o

all: shardmap bigmap.o
	@: # quiet make when nothing to do

shardmap: Makefile debug.h shardmap.h main.cc shardmap.so
	g++ $(opt) -Wall -Wno-unused-function -Wno-narrowing main.cc ./shardmap.so -lbacktrace -oshardmap

shardmap.so: Makefile $(obj)
	g++ $(opt) -shared $(obj) -o shardmap.so

shardmap.o: Makefile debug.h recops.h recops.c shardmap.h shardmap.cc
	g++ $(opt) -Wall -c -Wno-unused-function -Wno-narrowing -std=gnu++17 shardmap.cc -oshardmap.o

bigmap.o: Makefile debug.h bigmap.c bigmap.h
	gcc $(opt) -Wall -c bigmap.c

pmem.o: Makefile debug.h pmem.h pmem.c
	gcc $(opt) -Wall -Wno-unused-function -c pmem.c

options.o: Makefile debug.h options.h options.c
	gcc $(opt) -Wall -c options.c

utility.o: Makefile debug.h shardmap.h bt.c utility.c
	gcc $(opt) -Wall -Wno-unused-function -c utility.c

clean:
	rm -f shardmap *.o *.so a.out
