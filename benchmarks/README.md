# Pulsar JMH benchmarks

Build the library locally, then the standalone harness:

```bash
mvn -Prelease clean install
mvn -f benchmarks/pom.xml clean package
java -jar benchmarks/target/benchmarks.jar -prof gc -rf json -rff benchmarks/target/results.json
```

JMH runs warmup/measurement iterations in forked JVMs. Defaults use two forks; a quick exploratory run
can override them, for example:

```bash
java -jar benchmarks/target/benchmarks.jar BroadcastBenchmark -p recipients=1,16,128 -wi 2 -i 3 -w 1s -r 1s -f 1 -prof gc
java -jar benchmarks/target/benchmarks.jar StringDecodingBenchmark -p size=32,1024 -p storage=heap,direct -wi 2 -i 3 -w 1s -r 1s -f 1 -prof gc
```

`BroadcastBenchmark.previous` serializes separately for each embedded channel; `current` encodes once
and uses retained duplicates. Both use the same current serializer, dependency versions, payload and
channel implementation. Outbound buffers are drained and released within each measured invocation.
This isolates encoding/fanout and includes embedded-channel overhead. It excludes sockets, length-prefix
encoding, network congestion, backpressure and remote work.

`StringDecodingBenchmark.previous` retains the old custom-reader byte-array algorithm; `current` uses
the current reader. Both decode the same length-prefixed ASCII text. Heap/direct storage and text size
are parameters. This measures that specific read path, not whole-packet throughput or Unicode workloads.

Netty leak detection is disabled in the broadcast benchmark to measure ordinary production-style
costs; the test suite separately runs with paranoid detection. JMH defaults cap the fork's heap at
256 MiB for repeatable GC profiling. An exploratory single-fork run is useful evidence, but longer
multi-fork runs and representative TCP latency/load measurements are required for release claims.

Measured results and raw data from the local validation run are in [RESULTS.md](RESULTS.md).
