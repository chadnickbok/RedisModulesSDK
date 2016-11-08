#set environment variable RM_INCLUDE_DIR to the location of redismodule.h
ifndef RM_INCLUDE_DIR
	RM_INCLUDE_DIR=../
endif

ifndef RMUTIL_LIBDIR
	RMUTIL_LIBDIR=../rmutil
endif

# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
SHOBJ_CFLAGS ?=  -fno-common
SHOBJ_LDFLAGS ?= -shared
CFLAGS = -I$(RM_INCLUDE_DIR) -Wall -g -fPIC -lm
CC=gcc

all: module.so 

module.so: module.o
	$(LD) -o $@ module.o $(SHOBJ_LDFLAGS) $(LIBS) -L$(RMUTIL_LIBDIR) -lrmutil -lc 

clean:
	rm -rf *.xo *.so *.o

