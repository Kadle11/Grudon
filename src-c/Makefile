CC = gcc
CFLAGS = -Wall -Wextra -O3 -I../include-c
LDFLAGS = -lm

SRCS = host.c cxl_utils.c device.c pagerank.c read_graph.c sorting.c
OBJS = $(SRCS:.c=.o)
TARGET = run_pagerank

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	rm -f $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

run: $(TARGET)
	./$(TARGET) soc-LiveJournal1/soc-LiveJournal1.mtx

clean:
	rm -f $(OBJS) $(TARGET)
