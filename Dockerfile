FROM ubuntu:xenial

RUN apt-get -y update && \
  apt-get -y upgrade && \
  apt-get -y install build-essential git tcl-dev valgrind vim

RUN mkdir /build
WORKDIR /build

RUN git clone https://github.com/antirez/redis.git
WORKDIR /build/redis
RUN make CFLAGS="-g"

WORKDIR /build
RUN git clone https://github.com/facebook/zstd.git

WORKDIR /build/zstd
RUN make && make install && ldconfig

COPY redismodule.h /build/redismodule.h
COPY ./rmutil /build/rmutil
WORKDIR /build/rmutil
RUN make

COPY ./zstd_vals /build/zstd_vals
WORKDIR /build/zstd_vals
RUN make

EXPOSE 6379
WORKDIR /build/redis
CMD ["./src/redis-server", "--protected-mode", "no", "--loadmodule", "/build/zstd_vals/zstd_vals.so"]

