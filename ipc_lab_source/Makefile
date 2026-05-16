CC=gcc
CFLAGS=-O2 -Wall -Wextra -std=c11
TARGET=ipc_lab

all: $(TARGET)

$(TARGET): ipc_lab.c
	$(CC) $(CFLAGS) -o $(TARGET) ipc_lab.c

clean:
	rm -f $(TARGET)
	rm -f /tmp/ipc_fifo_c2s /tmp/ipc_fifo_s2c /tmp/ipc_unix_stream.sock
