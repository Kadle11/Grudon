CC      = gcc
CFLAGS  = -O3 -march=native -Wall -Wextra
LDFLAGS =

DRIVER_SRC = driver.c cli.c graph_generator.c csr_sweep_utils.c csv_writer.c \
             csr_sweep_push_st.c csc_sweep_pull_mt.c

all: driver

# Driver links all modules; -fopenmp is required for the parallel pull kernel.
driver: $(DRIVER_SRC)
	$(CC) $(CFLAGS) -fopenmp -o $@ $(DRIVER_SRC) $(LDFLAGS)

clean:
	rm -f driver

.PHONY: all clean
