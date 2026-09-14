# Lock-Free MPMC Queue

Implementation of a lock-free MPMC queue in C++17, along with tests to verify corectness and a benchmark.
I Built this project for self-learning, as a deep-dive into high performance concurrent data structure design.

## Benchmarks

Measured on a 8 hardware thread machine, equal producer/consumer counts, compared against
an `std::queue` with a mutex of the same capacity.

**Throughput (ops/sec, median of 5 runs):**
| Producers/Consumers | Lock-free | Mutex-based |
|---|---|---|
| 1 / 1 | 4.85M | 2.11M |
| 2 / 2 | 4.37M | 2.59M |
| 4 / 4 | 4.42M | 2.05M |
| 8 / 8 | 2.47M | 2.05M |

lockfree queue leads at every thread count but throughput falls off psat 4/4, since my machine only has
8 hw threads so 8*8 threads competing leads to this dropoff. The retry loops in `try_push` and `try_pop`
currently spin without backoff which costs more than a blocking mutex (since parked thread hands its core back immediately).

**Latency (1 prod / 1 consumer, median of 5 runs, 100k samples):**
| Percentile | Lock-free | Mutex-based |
|---|---|---|
| p50 | 58 ns | 285 ns |
| p99 | 129 ns | 1,902 ns |
| p999 | 136–156 ns | 3,755–4,545 ns |

lockfree queue's tail latency stays close to its median (roughly 2.7x from p50 to p999),
while mutex-based queue's tail balloons (roughly 16x).

## Building and running

Build both test and benchmark binaries
```bash
make
```

Build individual targets
```bash
make test
make benchmark
```

Build and run
```bash
make run-test
make run-benchmark
```

## Known limitations / future work

- No backoff strategy. Retry loops spin unconditionally, backoff would probably recover throughput past oversubscription point seen above.
- Bounded only. An unbounded version would require safe memory reclaiming (hazard pointers or epoch-based reclamation maybe).
- Visualizer. A simple graph or something that visualizes CSV data.
