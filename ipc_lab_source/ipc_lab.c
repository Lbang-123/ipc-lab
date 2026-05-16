/*
 * ipc_lab.c - Linux IPC experiment demo and benchmark
 *
 * Supported IPC methods:
 *   1) FIFO named pipes
 *   2) System V message queue
 *   3) System V shared memory + semaphore
 *   4) Unix domain stream socket
 *
 * Build: gcc -O2 -Wall -Wextra -std=c11 -o ipc_lab ipc_lab.c
 * Usage:
 *   ./ipc_lab server <fifo|msgq|shm|unix>
 *   ./ipc_lab client <fifo|msgq|shm|unix> [message]
 *   ./ipc_lab bench  <fifo|msgq|shm|unix>
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#define FIFO_C2S "/tmp/ipc_fifo_c2s"
#define FIFO_S2C "/tmp/ipc_fifo_s2c"
#define UNIX_SOCK_PATH "/tmp/ipc_unix_stream.sock"
#define MSG_KEY 0x12345
#define SHM_KEY 0x23456
#define SEM_KEY 0x23457
#define MAX_PAYLOAD (1024 * 1024)
#define MSG_CHUNK 4096
#define BENCH_ITERS 1000

#define CMD_DATA 1u
#define CMD_QUIT 2u

typedef struct {
    uint32_t cmd;
    uint32_t size;
} frame_hdr_t;

typedef struct ipc_client ipc_client_t;

typedef struct {
    const char *name;
    int (*server_loop)(void);
    int (*client_open)(ipc_client_t *client);
    int (*roundtrip)(ipc_client_t *client, const void *send_buf, size_t send_len,
                     void *recv_buf, size_t recv_cap, size_t *recv_len);
    int (*send_quit)(ipc_client_t *client);
    void (*client_close)(ipc_client_t *client);
} ipc_ops_t;

struct ipc_client {
    const ipc_ops_t *ops;
    int fd_in;
    int fd_out;
    int msgqid;
    int shmid;
    int semid;
    void *shmaddr;
    pid_t pid;
};

static volatile sig_atomic_t g_stop = 0;
static int g_msgqid = -1;
static int g_shmid = -1;
static int g_semid = -1;
static void *g_shmaddr = NULL;

static void on_signal(int signo) {
    (void)signo;
    g_stop = 1;
}

static void fatal_perror(const char *msg) {
    perror(msg);
}

static int write_all(int fd, const void *buf, size_t n) {
    const char *p = (const char *)buf;
    while (n > 0) {
        ssize_t ret = write(fd, p, n);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (ret == 0) return -1;
        p += ret;
        n -= (size_t)ret;
    }
    return 0;
}

static int read_all(int fd, void *buf, size_t n) {
    char *p = (char *)buf;
    while (n > 0) {
        ssize_t ret = read(fd, p, n);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (ret == 0) return -2; /* peer closed */
        p += ret;
        n -= (size_t)ret;
    }
    return 0;
}

static double now_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static void fill_pattern(unsigned char *buf, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        buf[i] = (unsigned char)((i * 31u + 7u) & 0xffu);
    }
}

static int is_printable_text(const unsigned char *buf, size_t len) {
    if (len == 0 || len > 128) return 0;
    for (size_t i = 0; i < len; ++i) {
        if (buf[i] < 32 || buf[i] > 126) return 0;
    }
    return 1;
}

static int send_frame(int fd, uint32_t cmd, const void *buf, size_t len) {
    if (len > UINT32_MAX) {
        errno = EINVAL;
        return -1;
    }
    frame_hdr_t hdr;
    hdr.cmd = cmd;
    hdr.size = (uint32_t)len;
    if (write_all(fd, &hdr, sizeof(hdr)) < 0) return -1;
    if (len > 0 && write_all(fd, buf, len) < 0) return -1;
    return 0;
}

static int recv_frame(int fd, uint32_t *cmd, void *buf, size_t cap, size_t *len) {
    frame_hdr_t hdr;
    int ret = read_all(fd, &hdr, sizeof(hdr));
    if (ret < 0) return ret;
    if (hdr.size > cap) {
        errno = EMSGSIZE;
        return -1;
    }
    if (hdr.size > 0) {
        ret = read_all(fd, buf, hdr.size);
        if (ret < 0) return ret;
    }
    *cmd = hdr.cmd;
    *len = hdr.size;
    return 0;
}

/* ============================ FIFO ============================ */
static int fifo_client_open(ipc_client_t *client) {
    client->fd_out = open(FIFO_C2S, O_RDWR);
    if (client->fd_out < 0) {
        fatal_perror("open fifo c2s");
        return -1;
    }
    client->fd_in = open(FIFO_S2C, O_RDWR);
    if (client->fd_in < 0) {
        fatal_perror("open fifo s2c");
        close(client->fd_out);
        return -1;
    }
    return 0;
}

static int fifo_roundtrip(ipc_client_t *client, const void *send_buf, size_t send_len,
                          void *recv_buf, size_t recv_cap, size_t *recv_len) {
    uint32_t cmd = 0;
    if (send_frame(client->fd_out, CMD_DATA, send_buf, send_len) < 0) return -1;
    if (recv_frame(client->fd_in, &cmd, recv_buf, recv_cap, recv_len) < 0) return -1;
    return (cmd == CMD_DATA) ? 0 : -1;
}

static int fifo_send_quit(ipc_client_t *client) {
    return send_frame(client->fd_out, CMD_QUIT, NULL, 0);
}

static void fifo_client_close(ipc_client_t *client) {
    if (client->fd_in >= 0) close(client->fd_in);
    if (client->fd_out >= 0) close(client->fd_out);
}

static int fifo_server_loop(void) {
    unsigned char *buf = (unsigned char *)malloc(MAX_PAYLOAD);
    if (!buf) {
        fatal_perror("malloc");
        return -1;
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    unlink(FIFO_C2S);
    unlink(FIFO_S2C);
    if (mkfifo(FIFO_C2S, 0666) < 0 && errno != EEXIST) {
        fatal_perror("mkfifo c2s");
        free(buf);
        return -1;
    }
    if (mkfifo(FIFO_S2C, 0666) < 0 && errno != EEXIST) {
        fatal_perror("mkfifo s2c");
        unlink(FIFO_C2S);
        free(buf);
        return -1;
    }
    int fd_in = open(FIFO_C2S, O_RDWR);
    int fd_out = open(FIFO_S2C, O_RDWR);
    if (fd_in < 0 || fd_out < 0) {
        fatal_perror("open fifo server");
        if (fd_in >= 0) close(fd_in);
        if (fd_out >= 0) close(fd_out);
        unlink(FIFO_C2S);
        unlink(FIFO_S2C);
        free(buf);
        return -1;
    }
    printf("FIFO server started.\n");
    while (!g_stop) {
        uint32_t cmd = 0;
        size_t len = 0;
        int ret = recv_frame(fd_in, &cmd, buf, MAX_PAYLOAD, &len);
        if (ret == -2) continue;
        if (ret < 0) {
            fatal_perror("fifo recv_frame");
            break;
        }
        if (cmd == CMD_QUIT) break;
        if (cmd == CMD_DATA) {
            if (is_printable_text(buf, len)) printf("FIFO server receives: %.*s\n", (int)len, buf);
            if (send_frame(fd_out, CMD_DATA, buf, len) < 0) {
                fatal_perror("fifo send_frame");
                break;
            }
        }
    }
    close(fd_in);
    close(fd_out);
    unlink(FIFO_C2S);
    unlink(FIFO_S2C);
    free(buf);
    printf("FIFO server stopped.\n");
    return 0;
}

/* ======================== Unix Domain Socket ======================== */
static int unix_client_open(ipc_client_t *client) {
    client->fd_in = socket(AF_UNIX, SOCK_STREAM, 0);
    client->fd_out = client->fd_in;
    if (client->fd_in < 0) {
        fatal_perror("socket");
        return -1;
    }
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, UNIX_SOCK_PATH, sizeof(addr.sun_path) - 1);
    for (int i = 0; i < 50; ++i) {
        if (connect(client->fd_in, (struct sockaddr *)&addr, sizeof(addr)) == 0) {
            return 0;
        }
        usleep(100000);
    }
    fatal_perror("connect unix socket");
    close(client->fd_in);
    client->fd_in = -1;
    client->fd_out = -1;
    return -1;
}

static int unix_roundtrip(ipc_client_t *client, const void *send_buf, size_t send_len,
                          void *recv_buf, size_t recv_cap, size_t *recv_len) {
    uint32_t cmd = 0;
    if (send_frame(client->fd_out, CMD_DATA, send_buf, send_len) < 0) return -1;
    if (recv_frame(client->fd_in, &cmd, recv_buf, recv_cap, recv_len) < 0) return -1;
    return (cmd == CMD_DATA) ? 0 : -1;
}

static int unix_send_quit(ipc_client_t *client) {
    return send_frame(client->fd_out, CMD_QUIT, NULL, 0);
}

static void unix_client_close(ipc_client_t *client) {
    if (client->fd_in >= 0) close(client->fd_in);
    client->fd_in = -1;
    client->fd_out = -1;
}

static int unix_handle_conn(int connfd, unsigned char *buf) {
    while (!g_stop) {
        uint32_t cmd = 0;
        size_t len = 0;
        int ret = recv_frame(connfd, &cmd, buf, MAX_PAYLOAD, &len);
        if (ret == -2) return 0;
        if (ret < 0) return -1;
        if (cmd == CMD_QUIT) return 1;
        if (cmd == CMD_DATA) {
            if (is_printable_text(buf, len)) printf("Unix socket server receives: %.*s\n", (int)len, buf);
            if (send_frame(connfd, CMD_DATA, buf, len) < 0) return -1;
        }
    }
    return 1;
}

static int unix_server_loop(void) {
    unsigned char *buf = (unsigned char *)malloc(MAX_PAYLOAD);
    if (!buf) {
        fatal_perror("malloc");
        return -1;
    }
    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    int listenfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (listenfd < 0) {
        fatal_perror("socket");
        free(buf);
        return -1;
    }
    unlink(UNIX_SOCK_PATH);
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, UNIX_SOCK_PATH, sizeof(addr.sun_path) - 1);
    if (bind(listenfd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fatal_perror("bind");
        close(listenfd);
        unlink(UNIX_SOCK_PATH);
        free(buf);
        return -1;
    }
    if (listen(listenfd, 8) < 0) {
        fatal_perror("listen");
        close(listenfd);
        unlink(UNIX_SOCK_PATH);
        free(buf);
        return -1;
    }
    printf("Unix domain socket server started.\n");
    while (!g_stop) {
        int connfd = accept(listenfd, NULL, NULL);
        if (connfd < 0) {
            if (errno == EINTR) continue;
            fatal_perror("accept");
            break;
        }
        int ret = unix_handle_conn(connfd, buf);
        close(connfd);
        if (ret == 1) break;
        if (ret < 0) fatal_perror("unix handle conn");
    }
    close(listenfd);
    unlink(UNIX_SOCK_PATH);
    free(buf);
    printf("Unix domain socket server stopped.\n");
    return 0;
}

/* ======================== System V Message Queue ======================== */
typedef struct {
    long mtype;
    pid_t pid;
    uint32_t cmd;
    uint32_t total_len;
    uint32_t seq;
    uint32_t chunks;
    uint32_t chunk_len;
    char data[MSG_CHUNK];
} msg_packet_t;

static size_t msg_payload_size(size_t chunk_len) {
    return offsetof(msg_packet_t, data) - sizeof(long) + chunk_len;
}

static int msgq_send_buffer(int msgqid, long mtype, pid_t pid, uint32_t cmd,
                            const void *buf, size_t len) {
    if (len > UINT32_MAX) {
        errno = EINVAL;
        return -1;
    }
    uint32_t chunks = (uint32_t)((len + MSG_CHUNK - 1) / MSG_CHUNK);
    if (chunks == 0) chunks = 1;
    const char *p = (const char *)buf;
    for (uint32_t i = 0; i < chunks; ++i) {
        size_t off = (size_t)i * MSG_CHUNK;
        size_t left = (len > off) ? (len - off) : 0;
        size_t chunk_len = left > MSG_CHUNK ? MSG_CHUNK : left;
        msg_packet_t pkt;
        memset(&pkt, 0, sizeof(pkt));
        pkt.mtype = mtype;
        pkt.pid = pid;
        pkt.cmd = cmd;
        pkt.total_len = (uint32_t)len;
        pkt.seq = i;
        pkt.chunks = chunks;
        pkt.chunk_len = (uint32_t)chunk_len;
        if (chunk_len > 0) memcpy(pkt.data, p + off, chunk_len);
        if (msgsnd(msgqid, &pkt, msg_payload_size(chunk_len), 0) < 0) {
            return -1;
        }
    }
    return 0;
}

static int msgq_recv_buffer(int msgqid, long mtype, uint32_t *cmd, pid_t *from_pid,
                            void *buf, size_t cap, size_t *len) {
    msg_packet_t pkt;
    ssize_t ret = msgrcv(msgqid, &pkt, sizeof(pkt) - sizeof(long), mtype, 0);
    if (ret < 0) return -1;
    (void)ret;
    if (pkt.total_len > cap || pkt.chunk_len > MSG_CHUNK) {
        errno = EMSGSIZE;
        return -1;
    }
    *cmd = pkt.cmd;
    *from_pid = pkt.pid;
    *len = pkt.total_len;
    if (pkt.chunk_len > 0) memcpy((char *)buf, pkt.data, pkt.chunk_len);
    for (uint32_t expected = 1; expected < pkt.chunks; ++expected) {
        ret = msgrcv(msgqid, &pkt, sizeof(pkt) - sizeof(long), mtype, 0);
        if (ret < 0) return -1;
        if (pkt.seq != expected || pkt.chunk_len > MSG_CHUNK) {
            errno = EPROTO;
            return -1;
        }
        size_t off = (size_t)pkt.seq * MSG_CHUNK;
        if (off + pkt.chunk_len > cap) {
            errno = EMSGSIZE;
            return -1;
        }
        if (pkt.chunk_len > 0) memcpy((char *)buf + off, pkt.data, pkt.chunk_len);
    }
    return 0;
}

static void msgq_cleanup(void) {
    if (g_msgqid >= 0) {
        msgctl(g_msgqid, IPC_RMID, NULL);
        g_msgqid = -1;
    }
}

static void msgq_signal(int signo) {
    (void)signo;
    msgq_cleanup();
    _exit(0);
}

static int msgq_client_open(ipc_client_t *client) {
    client->pid = getpid();
    client->msgqid = msgget(MSG_KEY, IPC_CREAT | 0666);
    if (client->msgqid < 0) {
        fatal_perror("msgget");
        return -1;
    }
    return 0;
}

static int msgq_roundtrip(ipc_client_t *client, const void *send_buf, size_t send_len,
                          void *recv_buf, size_t recv_cap, size_t *recv_len) {
    uint32_t cmd = 0;
    pid_t from_pid = 0;
    if (msgq_send_buffer(client->msgqid, 1L, client->pid, CMD_DATA, send_buf, send_len) < 0) {
        return -1;
    }
    if (msgq_recv_buffer(client->msgqid, (long)client->pid, &cmd, &from_pid,
                         recv_buf, recv_cap, recv_len) < 0) {
        return -1;
    }
    (void)from_pid;
    return (cmd == CMD_DATA) ? 0 : -1;
}

static int msgq_send_quit(ipc_client_t *client) {
    return msgq_send_buffer(client->msgqid, 1L, client->pid, CMD_QUIT, NULL, 0);
}

static void msgq_client_close(ipc_client_t *client) {
    (void)client;
}

static int msgq_server_loop(void) {
    unsigned char *buf = (unsigned char *)malloc(MAX_PAYLOAD);
    if (!buf) {
        fatal_perror("malloc");
        return -1;
    }
    g_msgqid = msgget(MSG_KEY, IPC_CREAT | 0666);
    if (g_msgqid < 0) {
        fatal_perror("msgget");
        free(buf);
        return -1;
    }
    signal(SIGINT, msgq_signal);
    signal(SIGTERM, msgq_signal);
    signal(SIGHUP, msgq_signal);
    printf("Message queue server started. msgqid=%d\n", g_msgqid);
    while (1) {
        uint32_t cmd = 0;
        pid_t client_pid = 0;
        size_t len = 0;
        if (msgq_recv_buffer(g_msgqid, 1L, &cmd, &client_pid, buf, MAX_PAYLOAD, &len) < 0) {
            if (errno == EINTR) continue;
            fatal_perror("msgrcv");
            break;
        }
        if (cmd == CMD_QUIT) break;
        if (len == sizeof(pid_t)) {
            pid_t received_pid;
            memcpy(&received_pid, buf, sizeof(pid_t));
            printf("Server receives a message from %d!\n", (int)received_pid);
            pid_t server_pid = getpid();
            if (msgq_send_buffer(g_msgqid, (long)client_pid, server_pid, CMD_DATA,
                                 &server_pid, sizeof(server_pid)) < 0) {
                fatal_perror("msgsnd");
                break;
            }
        } else {
            if (msgq_send_buffer(g_msgqid, (long)client_pid, getpid(), CMD_DATA,
                                 buf, len) < 0) {
                fatal_perror("msgsnd");
                break;
            }
        }
    }
    msgq_cleanup();
    free(buf);
    printf("Message queue server stopped.\n");
    return 0;
}

/* ======================== Shared Memory + Semaphore ======================== */
typedef struct {
    uint32_t cmd;
    uint32_t size;
    unsigned char data[MAX_PAYLOAD];
} shm_block_t;

union semun {
    int val;
    struct semid_ds *buf;
    unsigned short *array;
};

static int sem_do(int semid, unsigned short sem_num, short op) {
    struct sembuf sb;
    sb.sem_num = sem_num;
    sb.sem_op = op;
    sb.sem_flg = 0;
    while (semop(semid, &sb, 1) < 0) {
        if (errno == EINTR) continue;
        return -1;
    }
    return 0;
}

static int sem_set_value(int semid, int sem_num, int value) {
    union semun arg;
    arg.val = value;
    return semctl(semid, sem_num, SETVAL, arg);
}

static void shm_cleanup(void) {
    if (g_shmaddr && g_shmaddr != (void *)-1) {
        shmdt(g_shmaddr);
        g_shmaddr = NULL;
    }
    if (g_shmid >= 0) {
        shmctl(g_shmid, IPC_RMID, NULL);
        g_shmid = -1;
    }
    if (g_semid >= 0) {
        semctl(g_semid, 0, IPC_RMID);
        g_semid = -1;
    }
}

static void shm_signal(int signo) {
    (void)signo;
    shm_cleanup();
    _exit(0);
}

static int shm_client_open(ipc_client_t *client) {
    client->shmid = shmget(SHM_KEY, sizeof(shm_block_t), 0666);
    if (client->shmid < 0) {
        fatal_perror("shmget client; please start server first");
        return -1;
    }
    client->semid = semget(SEM_KEY, 2, 0666);
    if (client->semid < 0) {
        fatal_perror("semget client; please start server first");
        return -1;
    }
    client->shmaddr = shmat(client->shmid, NULL, 0);
    if (client->shmaddr == (void *)-1) {
        fatal_perror("shmat client");
        client->shmaddr = NULL;
        return -1;
    }
    return 0;
}

static int shm_roundtrip(ipc_client_t *client, const void *send_buf, size_t send_len,
                         void *recv_buf, size_t recv_cap, size_t *recv_len) {
    if (send_len > MAX_PAYLOAD || recv_cap < send_len) {
        errno = EMSGSIZE;
        return -1;
    }
    shm_block_t *block = (shm_block_t *)client->shmaddr;
    block->cmd = CMD_DATA;
    block->size = (uint32_t)send_len;
    if (send_len > 0) memcpy(block->data, send_buf, send_len);
    if (sem_do(client->semid, 0, 1) < 0) return -1;  /* request ready */
    if (sem_do(client->semid, 1, -1) < 0) return -1; /* response ready */
    if (block->size > recv_cap) {
        errno = EMSGSIZE;
        return -1;
    }
    *recv_len = block->size;
    if (*recv_len > 0) memcpy(recv_buf, block->data, *recv_len);
    return 0;
}

static int shm_send_quit(ipc_client_t *client) {
    shm_block_t *block = (shm_block_t *)client->shmaddr;
    block->cmd = CMD_QUIT;
    block->size = 0;
    return sem_do(client->semid, 0, 1);
}

static void shm_client_close(ipc_client_t *client) {
    if (client->shmaddr && client->shmaddr != (void *)-1) shmdt(client->shmaddr);
    client->shmaddr = NULL;
}

static int shm_server_loop(void) {
    g_shmid = shmget(SHM_KEY, sizeof(shm_block_t), IPC_CREAT | 0666);
    if (g_shmid < 0) {
        fatal_perror("shmget server");
        return -1;
    }
    g_semid = semget(SEM_KEY, 2, IPC_CREAT | 0666);
    if (g_semid < 0) {
        fatal_perror("semget server");
        shm_cleanup();
        return -1;
    }
    if (sem_set_value(g_semid, 0, 0) < 0 || sem_set_value(g_semid, 1, 0) < 0) {
        fatal_perror("semctl SETVAL");
        shm_cleanup();
        return -1;
    }
    g_shmaddr = shmat(g_shmid, NULL, 0);
    if (g_shmaddr == (void *)-1) {
        fatal_perror("shmat server");
        g_shmaddr = NULL;
        shm_cleanup();
        return -1;
    }
    signal(SIGINT, shm_signal);
    signal(SIGTERM, shm_signal);
    printf("Shared memory server started.\n");
    shm_block_t *block = (shm_block_t *)g_shmaddr;
    while (1) {
        if (sem_do(g_semid, 0, -1) < 0) {
            fatal_perror("semop wait request");
            break;
        }
        if (block->cmd == CMD_QUIT) break;
        if (block->cmd == CMD_DATA) {
            if (is_printable_text(block->data, block->size)) printf("Shared memory server receives: %.*s\n", (int)block->size, block->data);
            /* Echo: data already stays in shared memory. */
            if (sem_do(g_semid, 1, 1) < 0) {
                fatal_perror("semop post response");
                break;
            }
        }
    }
    shm_cleanup();
    printf("Shared memory server stopped.\n");
    return 0;
}

/* ======================== Common commands ======================== */
static const ipc_ops_t OPS[] = {
    {"fifo", fifo_server_loop, fifo_client_open, fifo_roundtrip, fifo_send_quit, fifo_client_close},
    {"msgq", msgq_server_loop, msgq_client_open, msgq_roundtrip, msgq_send_quit, msgq_client_close},
    {"shm", shm_server_loop, shm_client_open, shm_roundtrip, shm_send_quit, shm_client_close},
    {"unix", unix_server_loop, unix_client_open, unix_roundtrip, unix_send_quit, unix_client_close},
};

static const ipc_ops_t *find_ops(const char *name) {
    for (size_t i = 0; i < sizeof(OPS) / sizeof(OPS[0]); ++i) {
        if (strcmp(OPS[i].name, name) == 0) return &OPS[i];
    }
    return NULL;
}

static void usage(const char *prog) {
    fprintf(stderr,
            "Usage:\n"
            "  %s server <fifo|msgq|shm|unix>\n"
            "  %s client <fifo|msgq|shm|unix> [message]\n"
            "  %s bench  <fifo|msgq|shm|unix>\n",
            prog, prog, prog);
}

static int run_client(const ipc_ops_t *ops, const char *message) {
    ipc_client_t client;
    memset(&client, 0, sizeof(client));
    client.ops = ops;
    client.fd_in = -1;
    client.fd_out = -1;
    client.msgqid = -1;
    client.shmid = -1;
    client.semid = -1;
    if (ops->client_open(&client) < 0) return 1;

    unsigned char recv_buf[MAX_PAYLOAD];
    size_t recv_len = 0;

    if (strcmp(ops->name, "msgq") == 0 && message == NULL) {
        pid_t pid = getpid();
        if (ops->roundtrip(&client, &pid, sizeof(pid), recv_buf, sizeof(recv_buf), &recv_len) < 0) {
            fatal_perror("client roundtrip");
            ops->client_close(&client);
            return 1;
        }
        if (recv_len == sizeof(pid_t)) {
            pid_t server_pid;
            memcpy(&server_pid, recv_buf, sizeof(server_pid));
            printf("Client receives a message from %d!\n", (int)server_pid);
        }
    } else {
        const char *text = message ? message : "Hello IPC";
        if (ops->roundtrip(&client, text, strlen(text), recv_buf, sizeof(recv_buf), &recv_len) < 0) {
            fatal_perror("client roundtrip");
            ops->client_close(&client);
            return 1;
        }
        printf("Client receives echo: %.*s\n", (int)recv_len, recv_buf);
    }

    ops->client_close(&client);
    return 0;
}

static int run_bench(const ipc_ops_t *ops) {
    ipc_client_t client;
    memset(&client, 0, sizeof(client));
    client.ops = ops;
    client.fd_in = -1;
    client.fd_out = -1;
    client.msgqid = -1;
    client.shmid = -1;
    client.semid = -1;
    if (ops->client_open(&client) < 0) return 1;

    unsigned char *send_buf = (unsigned char *)malloc(MAX_PAYLOAD);
    unsigned char *recv_buf = (unsigned char *)malloc(MAX_PAYLOAD);
    if (!send_buf || !recv_buf) {
        fatal_perror("malloc bench buffer");
        free(send_buf);
        free(recv_buf);
        ops->client_close(&client);
        return 1;
    }
    fill_pattern(send_buf, MAX_PAYLOAD);

    const size_t sizes[] = {1, 64, 1024, 64 * 1024, 1024 * 1024};
    printf("IPC method: %s, iterations: %d\n", ops->name, BENCH_ITERS);
    printf("%12s %18s %18s\n", "Size", "AvgLatency(us)", "Throughput(MB/s)");

    for (size_t si = 0; si < sizeof(sizes) / sizeof(sizes[0]); ++si) {
        size_t n = sizes[si];
        double start = now_seconds();
        for (int i = 0; i < BENCH_ITERS; ++i) {
            size_t recv_len = 0;
            if (ops->roundtrip(&client, send_buf, n, recv_buf, MAX_PAYLOAD, &recv_len) < 0) {
                fatal_perror("bench roundtrip");
                free(send_buf);
                free(recv_buf);
                ops->client_close(&client);
                return 1;
            }
            if (recv_len != n || memcmp(send_buf, recv_buf, n) != 0) {
                fprintf(stderr, "data verify failed at size=%zu iter=%d\n", n, i);
                free(send_buf);
                free(recv_buf);
                ops->client_close(&client);
                return 1;
            }
        }
        double elapsed = now_seconds() - start;
        double avg_us = elapsed * 1000000.0 / BENCH_ITERS;
        double total_mb = (double)n * 2.0 * BENCH_ITERS / (1024.0 * 1024.0);
        double mbps = total_mb / elapsed;
        printf("%12zu %18.3f %18.3f\n", n, avg_us, mbps);
    }

    (void)ops->send_quit(&client);
    ops->client_close(&client);
    free(send_buf);
    free(recv_buf);
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        usage(argv[0]);
        return 1;
    }
    const char *cmd = argv[1];
    const char *method = argv[2];
    const ipc_ops_t *ops = find_ops(method);
    if (!ops) {
        fprintf(stderr, "Unknown IPC method: %s\n", method);
        usage(argv[0]);
        return 1;
    }
    if (strcmp(cmd, "server") == 0) {
        return ops->server_loop() == 0 ? 0 : 1;
    }
    if (strcmp(cmd, "client") == 0) {
        const char *message = (argc >= 4) ? argv[3] : NULL;
        return run_client(ops, message);
    }
    if (strcmp(cmd, "bench") == 0) {
        return run_bench(ops);
    }
    usage(argv[0]);
    return 1;
}
