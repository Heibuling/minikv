# minikv

[![ci](https://github.com/Heibuling/minikv/actions/workflows/ci.yml/badge.svg)](https://github.com/Heibuling/minikv/actions/workflows/ci.yml)

**中文** · [English](README.en.md) — 你正在读中文版。

一个纯标准库写成的 LSM 风格键值存储引擎，代码可审计。单进程、不联网、没有后台
compaction 线程。它的目的是被阅读、被测试、被追问——而不是拿去跑任何重要的东西。

三组能在同一台机器上复现的结果：

- **批量写快约 34 倍**：`sync="always"` 下，一个 `batch()` 装 50 次写，摊到 7.7 us/op；
  而单条 `put` 是 258 us/op——一批只付一次 fsync，而不是五十次。
- **热读只比 SQLite 慢 2.3 倍**：5 000 次点查 0.015 s，原来 0.058 s，
  因为一个 4 KiB 数据块能服务约 34 次查询。
- **已确认的写一条不丢**：140 个崩溃注入点，0 丢失。

```python
from minikv import Store

with Store("data") as store:
    store.put(b"hello", b"world")
    print(store.get(b"hello"))          # b'world'
    for key, value in store.scan(b"h", b"i", limit=10):
        print(key, value)
```

```
$ pip install -e .
$ minikv --db ./data put hello world
$ minikv --db ./data fsck          # 回读每一个数据块和每一条 WAL 记录
```

## 它怎么工作

一次写先落到预写日志（WAL），再进入内存中的有序表。内存表到 4 MiB 时被冻结，写成
不可变的段文件，把当前存活的段集合记进带校验和的 MANIFEST，然后才删除被它取代的
WAL 文件。一次读先查内存表，再从新到旧查每个段：靠段上记录的键区间、布隆过滤器和
稀疏索引，大多数文件根本不会被打开。`compact()` 把所有段合并成一个并丢弃墓碑；持有
`snapshot()` 会让它读到的段留在盘上，所以长扫描不会被中途破坏。磁盘格式是显式的小
端字节布局，每条记录、每个数据块、索引和布隆过滤器都有 CRC32，没有任何地方用 pickle。

## 保证

| 保证 | 机制 | 测试 |
| --- | --- | --- |
| `sync="always"`：已确认的写在进程被杀后仍然存在 | 返回前 fsync 这条 WAL 记录 | 140 个 kill 点，0 丢失 |
| WAL 尾部的半写记录只截断，绝不当作记录解码 | 每条记录 CRC + 长度上界 | `test_corruption.py` |
| 段内容损坏会报错，绝不返回错误数据 | 解码前先校验数据块的 CRC | 单字节翻转扫描 |
| 一批要么全部生效、要么全部不生效 | 整批装进一条 WAL 记录 | `test_engine.py` |
| compaction 不会暴露写了一半的文件 | 写 .tmp、fsync、rename、MANIFEST | `test_compaction.py` |

## 不保证什么

| 不保证 | 原因 |
| --- | --- |
| `sync="never"` 能扛机器崩溃 | 何时回写由操作系统决定 |
| `sync="interval"` 断电最多丢一个间隔 | 默认间隔是 100 ms |
| 能承受 WAL 中间某个*长度*字段损坏 | 与半写尾部无法区分，文件在该处被截断 |
| 持有快照时能回收磁盘 | 最后一个读者放手后文件才会被删 |

键上限 64 KiB、值上限 16 MiB，越界会在写入任何字节前抛 `ValueError`；空键是合法的；
只允许一个进程打开一个目录。

## 耐久性三档

| 档位 | fsync | 进程被杀 | 机器崩溃 |
| --- | --- | --- | --- |
| `always`（默认） | 每次返回之前 | 安全 | 安全 |
| `interval` | 每 100 ms | 安全 | 最多丢 100 ms |
| `never` | 从不 | 安全 | 可能丢尾部 |

Windows 进程无法对目录做 fsync，所以段文件的 rename 只和 NTFS 本身一样耐久，
`stats()` 会把这一点报出来。

## 基准（本机、5 000 次操作、3 次取中位数）

| 场景 | minikv | sqlite3 | 慢多少 |
| --- | --- | --- | --- |
| 顺序写 | 0.081 s | 0.006 s | 14.8x |
| 点查命中 | 0.015 s | 0.007 s | 2.3x |
| 点查命中（热集） | 0.012 s | 0.006 s | 2.1x |
| 点查未命中 | 0.006 s | 0.004 s | 1.5x |
| 全表扫描 | 0.006 s | 0.002 s | 2.9x |
| 重启（5 000 条 WAL 记录） | 0.008 s | 0.001 s | 7.0x |
| compaction（76 个段） | 0.074 s | 仅合并本身：0.026 s | -- |

SQLite 是有页缓存和多年调优的 C 库；差距来自解释器、每条记录一个 Python 对象、
以及每次写一次系统调用。`flush()` 就是 checkpoint，能把重启从 0.008 s 降到 0.004 s。

## 什么时候不该用它

- 你需要耐久性、并发或者有人支持：用标准库里的 `sqlite3`，这些它都做得更好。
- 你需要多个写者或多个进程，或者值超过 16 MiB。
- 你希望 compaction 或崩溃恢复自动完成：这两件事都是手动的。

## 设计与文档

- DESIGN.md —— 为什么是 LSM、fsync 的代价、并发模型，以及每一条都带测量的取舍。
- BENCHMARKS.md —— 方法、数字、三轮优化。
- docs/crash-consistency.md、docs/format.md —— 不变量与字节布局。
- CONTRIBUTING.md、SECURITY.md、CHANGELOG.md

## 开发

```
python -m pip install pytest pytest-cov hypothesis mypy ruff
ruff check . && mypy       # 两者都是严格模式，配置在 pyproject.toml
pytest --cov=minikv        # 覆盖率门限 85%
```

CI 在 Linux、macOS、Windows 上运行 ruff、mypy、测试与覆盖率门限，覆盖 Python
3.9、3.12、3.13。MIT 许可。