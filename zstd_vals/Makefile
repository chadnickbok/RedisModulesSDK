#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../rmutil
endif

ifndef ZSTD_INCLUDE_DIR
	ZSTD_INCLUDE_DIR=../zstd/lib
endif

ifndef ZSTD_LIBDIR
	ZSTD_LIBDIR=../zstd/lib
endif

# Compile flags for linux / osx
SHOBJ_CFLAGS ?=  -fno-common -g -ggdb
SHOBJ_LDFLAGS ?= -shared -Bsymbolic
CFLAGS = -I$(RM_INCLUDE_DIR) -I$(ZSTD_INCLUDE_DIR) -Wall -g -fPIC -lc -lm -O2 -std=gnu99
CC=gcc

all: zstd_vals.so 

zstd_vals.so: zstd_vals.o
	$(LD) -o $@ zstd_vals.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc -L$(ZSTD_LIBDIR) -lzstd

clean:
	rm -rf *.xo *.so *.o

