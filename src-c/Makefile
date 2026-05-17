CC = gcc
#CFLAGS = -Wall -Wextra -fopenmp -O2 -I../include-c -fno-inline -fno-omit-frame-pointer -g
CFLAGS = -Wall -Wextra -fopenmp -O3 -I../include-c
LDFLAGS = -lm

SRCS = host.c cxl_utils.c device.c ipc.c shared_memory_utils.c pagerank.c sssp.c cc.c read_graph.c sorting.c
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
	rm -f $(OBJS) $(TARGET)
