CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -O2 -I../include-c -fno-omit-frame-pointer -fno-inline -g -fopenmp
# CFLAGS = -Wall -Wextra -std=c11 -O3 -I../include-c
LDFLAGS = -lm -fopenmp

SRCS = host.c graph_io.c algorithms.c utils.c
OBJS = $(SRCS:.c=.o)
TARGET = run_graph_algorithms

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	rm -f $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET) measurement bench_*.data
