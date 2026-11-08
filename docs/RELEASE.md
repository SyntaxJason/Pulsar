# Release preparation

The development version is **0.3.0-SNAPSHOT**. Protocol version remains **2**.
Publishing a Git branch does not deploy Maven artifacts or create a release tag or GitHub release.

## Build and verify

Use Java 21+ and Maven. From the repository root:

```bash
mvn -Prelease clean install
mvn -f examples/pom.xml clean verify
mvn -f benchmarks/pom.xml clean package
```

The release profile attaches source and Javadoc jars. It does not deploy artifacts or sign them.
The main jar and source jar have a fixed archive timestamp from `project.build.outputTimestamp`.
Update that property deliberately when cutting a release. Byte-for-byte comparisons require the same
JDK/toolchain; Javadoc output can differ between JDK versions.

Tests include malformed/truncated frames, real length-prefix framing under fragmentation/coalescing,
frame size boundaries, failed-serialization buffer ownership, broadcast sharing, batch ordering and
flush counts, callback shutdown, startup failure cleanup, typed events, serialization, deterministic
cache expiry races, and real TCP packet/cache traffic. Netty leak detection is enabled in paranoid
mode for tests; explicit reference-count assertions cover the encoding failure paths.

`.github/workflows/verify.yml` prepares Java 21/25 builds on Linux and Windows. It also builds examples,
release jars, and the benchmark harness. It runs on pushes and pull requests and can also be started manually.

## Compatibility notes

- Existing `send`, `broadcast`, builder and cache APIs remain available.
- `Connection.sendAll` has a default implementation for other `Connection` implementations. The
  serialization-before-enqueueing and single-flush behavior belongs to Pulsar's Netty implementation.
- Ordinary protocol-v2 record serialization retains its wire format.
- A custom serializer registered for a built-in type now takes effect; the old implementation silently
  ignored those registrations. Configure both peers the same way when using this correction.
- Invalid TTLs and oversized outbound data now fail locally. Closed server caches reject mutations.
- Endpoints reject malformed frames before application packet/cache callbacks. Custom deserializers
  still execute while parsing.
- `close` from the endpoint's own callbacks initiates shutdown without blocking. Outside those threads
  it waits; use `closeAsync` when completion must be observed without blocking.
- Broadcast serializers run once per broadcast. They must not depend on being invoked per connection.

## Before declaring a stable release

1. Choose and add the project's license. This repository currently has no LICENSE file or Maven
   license metadata; this patch does not choose redistribution terms for the author.
2. Decide and document the supported deployment boundary. Authentication, TLS configuration, cache
   authorization, outbound backpressure limits, and per-peer subscription limits are not implemented.
3. Define cache concurrency guarantees. Compare-and-remove protects local expiry against replacing
   entries, but cross-thread mutation delivery and snapshot/live-update ordering lack sequence numbers.
   Serialize application cache operations if strict ordering is required. A stress-tested ordering
   design should precede advertising transactional or linearizable replication.
4. Add schema/version negotiation if differently deployed peer versions must interoperate safely.
   The protocol currently checks numeric IDs and protocol version, not record layout compatibility.
5. Add depth/work budgets and extended malformed-input fuzzing before accepting arbitrary untrusted
   serialized graphs. Frame and collection bounds are not a complete per-peer resource budget.
6. Complete public API Javadocs. The Javadoc jar builds, but the pre-existing API has missing-comment
   warnings. Verify the Windows/Linux CI matrix on GitHub before cutting a release.
7. Run representative TCP load/latency tests with the intended hardware, payloads, recipients and slow
   readers. The local microbenchmarks isolate CPU/allocation costs and do not establish network throughput.
8. Choose the release version, synchronize `pom.xml`, `Pulsar.VERSION`, examples/benchmarks, Maven README
   coordinates and archive timestamp, then configure signing/deployment for the intended repository.

## Performance measurements

See [the benchmark harness](../benchmarks/README.md). It keeps JMH dependencies out of the library's
runtime dependency graph. Preserve the raw JSON, JVM version and flags with any published result.

## Local validation on 2026-11-09

- Java 21, 25 and 26 on Linux: 60 tests each; zero failures, errors or skips.
- Java 21/25 release builds and Java 26 release verification: successful, including source/Javadoc jars.
- Examples: clean verification against the locally installed snapshot succeeded on Java 21.
- Standalone JMH harness: clean package and both benchmark suites succeeded on Java 21.
- Main, source and Javadoc jars: identical SHA-256 hashes across two clean JDK 26 packages. The second
  package skipped tests specifically for the archive comparison, after the complete test run passed.
- POM XML/version consistency, CI YAML syntax/matrix and Git whitespace checks passed.
- Javadoc generation reports missing API comment warnings; Windows and remote GitHub Actions remain
  unverified locally. This is release preparation, not a completed stable-release certification.

### Reproduced JDK 26 artifact hashes

- `pulsar-0.3.0-SNAPSHOT.jar`: `aa382cb9730210e9ff33900199e858e0c0714d7a486803900a0994239f0a2203`
- `pulsar-0.3.0-SNAPSHOT-sources.jar`: `c321c1bef75e4594d10f75b8304732656f69b417da18e17a6a56f8e7c1a534d1`
- `pulsar-0.3.0-SNAPSHOT-javadoc.jar`: `317eeeac51364a4037ba7bb7ffcf3708971ad1180970fcbb639b6a8821dd4c2c`
