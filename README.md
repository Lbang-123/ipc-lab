# Linux IPC 实验项目

本项目实现 4 种 Linux 进程间通信方式：命名管道 FIFO、System V 消息队列、System V 共享内存 + 信号量、Unix 域流式套接字。

## 编译

```bash
make
```

## 运行演示

打开两个终端。

终端 1：

```bash
./ipc_lab server fifo
```

终端 2：

```bash
./ipc_lab client fifo "hello"
```

把 `fifo` 替换成 `msgq`、`shm`、`unix` 即可测试其他通信方式。

## 性能测试

终端 1：

```bash
./ipc_lab server fifo
```

终端 2：

```bash
./ipc_lab bench fifo
```

程序会测试 1B、64B、1KB、64KB、1MB 在 1000 次往返通信下的平均延迟和吞吐量。
