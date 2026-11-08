# Local measurements: 2026-11-09

Exploratory measurements on a live Linux desktop, AMD Ryzen 7 7800X3D (8 cores / 16 threads),
OpenJDK 21.0.13.0.2+1, JMH 1.37, Netty 4.2.18.Final, Pulsar 0.3.0-SNAPSHOT.
One benchmark thread, two JVM forks, three one-second warmup iterations and five one-second
measurement iterations per fork. Fork heap: `-Xms256m -Xmx256m`. GC profiler enabled.
Broadcast forks additionally use `-Dio.netty.leakDetection.level=disabled`.

The following time errors are the 99.9% confidence intervals reported by JMH. Allocations are JVM heap
bytes per operation measured by the GC profiler; they do not measure native/direct-buffer memory.
These runs do not control desktop load or CPU frequency and are not network throughput claims.

## Broadcast fanout

One operation encodes and distributes a packet containing a 768-character string and eight integers
into embedded channels, then drains/releases their outbound buffers. Socket I/O and length-prefix
encoding are excluded. `previous` uses per-recipient encoding with the same current serializer and
dependencies; `current` uses the optimized broadcast path. These are strategy comparisons, not a
full historical 0.2.0-BETA vs 0.3.0 end-to-end comparison.

| Recipients | Previous ns/broadcast | Current ns/broadcast | Point-estimate speedup | Previous heap B/broadcast | Current heap B/broadcast |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 279.9 ± 5.5 | 277.6 ± 15.2 | 1.01× | 896 | 896 |
| 16 | 4,974.3 ± 120.2 | 1,445.5 ± 43.8 | 3.44× | 14,336 | 3,216 |
| 128 | 41,741.2 ± 6,827.7 | 10,482.8 ± 207.5 | 3.98× | 114,689 | 20,240 |

For one recipient, the time estimates overlap and allocations are unchanged. At 16 and 128
recipients, the current path substantially lowers both time and heap allocation in this harness.
Explicit regression tests separately verify one codec invocation per broadcast, independent reader
indices and buffer release. They do not substitute for TCP load/latency testing.

## String decoding

One operation decodes a length-prefixed ASCII string. `previous` uses the former intermediate-array
custom-reader algorithm; `current` uses the current custom reader. Both return the decoded String.

| Bytes | Buffer | Previous ns/read | Current ns/read | Previous heap B/read | Current heap B/read |
| ---: | :--- | ---: | ---: | ---: | ---: |
| 32 | heap | 13.2 ± 0.6 | 10.9 ± 0.3 | 120 | 72 |
| 32 | direct | 13.9 ± 0.8 | 17.3 ± 3.6 | 120 | 120 |
| 1024 | heap | 138.9 ± 10.3 | 68.7 ± 8.2 | 2,104 | 1,064 |
| 1024 | direct | 145.3 ± 38.8 | 167.1 ± 69.2 | 2,104 | 2,104 |

Heap-backed reads avoid one intermediate array. Direct-buffer allocation is unchanged in these
ordinary JMH worker threads, and the direct-buffer timing intervals overlap: no general direct-buffer
speedup is claimed. The benchmark does not cover Unicode payloads or Netty event-loop scheduling.

## Reproduce and inspect

Run the commands in [README.md](README.md). The broadcast run used the default recipient parameters;
strings used `-p size=32,1024`. Both used `-prof gc -rf json -rff <result file>`.

- [Raw broadcast JSON](results/2026-11-09-broadcast.json)
- [Raw string JSON](results/2026-11-09-strings.json)
