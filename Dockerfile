FROM ubuntu:xenial

RUN apt-get -y update && apt-get -y upgrade
RUN apt-get -y install build-essential git tcl-dev

RUN mkdir /build
WORKDIR /build

RUN git clone https://github.com/antirez/redis.git
WORKDIR /build/redis

RUN make

COPY . /build/
WORKDIR /build/rmutil
RUN make
WORKDIR /build/example
RUN make

WORKDIR /build/redis
CMD ["./src/redis-server", "--loadmodule", "/build/example/module.so"]

