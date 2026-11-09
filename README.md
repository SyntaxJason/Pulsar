# Pulsar

Pulsar is an intuitive Java networking library powered by Netty. Application code should feel closer to Bukkit events than to a low-level networking framework while the runtime keeps the hot path small and predictable.

## Development: 0.3.1-SNAPSHOT

The current development version is `0.3.1-SNAPSHOT`. This snapshot is not published by this
repository's build. Install it locally using the Maven instructions below; examples and benchmarks
use the same locally installed version.

This development version adds:

- optional application-protocol detection on a shared TCP port, with custom handler pipelines;
- `Connection.sendAll(...)` and `PulsarClient.sendAll(...)` for ordered batches with one flush;
- `PulsarClient.closeAsync()` and `PulsarServer.closeAsync()` for observable asynchronous shutdown;
- one serialization per server broadcast, shared through independently indexed Netty buffers;
- frame validation before packet events or cache mutations are applied;
- consistent outbound size limits and buffer cleanup when serialization fails;
- cache TTL overflow protection and safe expiry when a value is replaced concurrently;
- custom serializers for built-in types, and correct listener changes before the event registry freezes;
- automated regression/TCP tests, a CI matrix, and a standalone JMH benchmark harness.

Netty is pinned to `4.2.18.Final`, a [bug-fix and security release](https://netty.io/news/2026/09/09/4-2-18-Final.html).
See [release preparation](docs/RELEASE.md) for validation commands and remaining release decisions.

## Maven

Install the current snapshot from the repository root:

```sh
mvn -Prelease clean install
```

Then add it to your project:

```xml
<dependency>
    <groupId>de.syntaxjason</groupId>
    <artifactId>pulsar</artifactId>
    <version>0.3.1-SNAPSHOT</version>
</dependency>
```

## Requirements

- Java 21+
- Maven
- Netty 4.2.x

## Bootstrap first, reflection-free runtime

Pulsar uses a strict bootstrap/freeze model.

Before the network starts, Pulsar:

1. registers packets, serializers and listeners,
2. inspects reflective metadata once,
3. compiles record codec plans and event invokers,
4. validates every registered packet type,
5. freezes the registries,
6. starts the Netty runtime.

After the freeze, packet serialization and event dispatch do not call `Method.invoke()`, `Constructor.newInstance()`, `Class.forName()` or scan record components. Record accessors and constructors are prepared as `MethodHandle` call sites during bootstrap.

Runtime registration that would require a new reflective scan is intentionally rejected. Register everything on the builder before `start()` / `connect()`.

## Packets

A packet is still just a record:

```java
import de.syntaxjason.pulsar.packet.Packet;

public record ChatPacket(
        String sender,
        String message
) implements Packet {
}
```

Register packets before the endpoint starts:

```java
PulsarServer server = Pulsar.server()
        .port(8080)
        .packets(ChatPacket.class, LocationPacket.class)
        .start();
```

A typed `PacketReceiveEvent<T>` listener also causes `T` to be registered automatically during bootstrap. Explicit `.packet(...)` / `.packets(...)` is still recommended for packets that are only sent and never received by a typed listener.

### Stable packet names

By default, the logical packet id is the class name. `@PacketType` can provide a stable logical name:

```java
@PacketType("chat")
public record ChatPacket(String sender, String message) implements Packet {
}
```

Protocol v2 does **not** put class names on the wire. Pulsar derives a deterministic 32-bit wire id from the logical packet id and validates collisions during bootstrap. Incoming network data can no longer trigger `Class.forName()`.

## Multiple application protocols on one TCP port

Register additional protocols on the server builder:

```java
PulsarServer server = Pulsar.server()
        .port(8080)
        .packets(ChatPacket.class)
        .protocol(new HttpStatusProtocol()) // adapter from the examples project
        .protocolDetectionTimeout(Duration.ofSeconds(5))
        .maxProtocolProbeBytes(64)
        .start();
```

This server accepts ordinary Pulsar clients and HTTP requests on the **same TCP port**. The examples
project includes `HttpStatusProtocol` (`GET` / `HEAD /health`) and a runnable `SharedPortServer`.
HTTP is an example adapter with its own Netty HTTP dependency; it is not an additional core runtime
dependency or a built-in HTTP routing framework.

Selection happens once per connection. The selector inspects a bounded prefix, installs the selected
protocol's pipeline, forwards the original bytes unchanged, and removes itself. Subsequent messages
on that connection go directly to the protocol's handlers. Pulsar's packet format stays at version 2.

Without `.protocol(...)`, the server installs the existing Pulsar pipeline directly. When multiplexing
is enabled, clients must send first: the server cannot choose a protocol or emit a Pulsar
`ConnectionOpenEvent` for a silent client. A normal Pulsar `connect()` still opens the client socket;
its first `send(...)` or cache subscription lets the server recognize it. Server-first greetings on a
shared port therefore require a client request first. Selection is not authentication.

Implement `ApplicationProtocol` for other application protocols, or use
`ApplicationProtocol.withPrefix(name, signature, pipeline -> ...)` for a fixed signature. Custom adapters
supply their own Netty handlers, framing, size limits and protocol error handling. Protocol names are
unique, `pulsar` is reserved, and ambiguous signatures are rejected instead of resolving by registration
order. The default detection deadline is 5 seconds with a 64-byte probe; both are configurable.

`ProtocolSelectedEvent` reports the chosen protocol once. `server.connections()` and broadcasts still
refer to Pulsar connections; `server.activeConnections()` counts all accepted TCP sockets, including
custom protocols and pending detection. Closing the server closes all of them.

See [the extension contract and example](docs/PROTOCOLS.md) for lifecycle, framing and limit details.

## Server

```java
PulsarServer server = Pulsar.server()
        .port(8080)
        .packets(ChatPacket.class)
        .listener(new ChatListener())
        .start();
```

Send to everyone:

```java
server.broadcast(new ChatPacket("Server", "Hello network!"));
```

## Client

```java
PulsarClient client = Pulsar.client()
        .address("127.0.0.1", 8080)
        .packets(ChatPacket.class)
        .listener(new ChatListener())
        .connect();

client.send(new ChatPacket("Jason", "Moin"));
```

`startAsync()` and `connectAsync()` execute the blocking bootstrap/start boundary on Java virtual threads instead of the common `ForkJoinPool`.

### Sending a batch

```java
client.sendAll(List.of(
        new ChatPacket("Jason", "First"),
        new ChatPacket("Jason", "Second")
));

// Also available on any Pulsar connection:
connection.sendAll(packets);
```

The Netty implementation encodes the entire batch before enqueueing it. If encoding or iteration fails,
no packet from that batch is sent and all temporary buffers are released. Successful batches retain
iteration order and use one event-loop task and one flush. Delivery still depends on the connection;
these void send methods do not acknowledge remote receipt. Keep batches bounded, because all encoded
frames remain in memory until enqueueing.

`server.broadcast(packet)` serializes once and shares the encoded content among the currently open
connections. Custom serializers should be deterministic and must not rely on invocation once per recipient.

## Bukkit-style events without reflective dispatch

```java
public final class ChatListener implements Listener {

    @EventHandler
    public void onChat(PacketReceiveEvent<ChatPacket> event) {
        ChatPacket packet = event.packet();
        System.out.println(packet.sender() + ": " + packet.message());
    }
}
```

Register listeners on the builder:

```java
PulsarServer server = Pulsar.server()
        .listener(new ChatListener())
        .start();
```

Pulsar scans `@EventHandler` methods once during bootstrap and turns them into prepared `MethodHandle` invokers. `fire()` only executes those invokers; it does not call `Method.invoke()`.

Because the event registry is frozen after startup, this is intentionally rejected:

```java
server.events().register(new ChatListener()); // registry is already frozen
```

## Automatic serialization

The bootstrap compiler supports:

- primitives and boxed primitives
- `String`
- `UUID`
- enums
- `byte[]`
- arrays
- `List<T>`
- `Set<T>`
- `Map<K, V>`
- `Optional<T>`
- nested Java records
- custom `TypeSerializer<T>` implementations

Records are analyzed recursively before networking starts. Accessors and canonical constructors are converted to runtime codec plans.

### Non-packet record models

Types that are not reachable from registered packets must be declared explicitly before startup. This is especially important for network-cache values:

```java
PulsarServer server = Pulsar.server()
        .serializable(PlayerData.class)
        .start();

NetworkCache<UUID, PlayerData> players = server.cache(
        "players",
        UUID.class,
        PlayerData.class
);
```

If a cache type was not compiled during bootstrap, Pulsar fails immediately with an explanation instead of performing reflection on the network thread.

### Custom serializers

Custom serializers still do not expose Netty's `ByteBuf`:

```java
PulsarServer server = Pulsar.server()
        .serializer(Location.class, new LocationSerializer())
        .start();
```

A registered custom serializer is compiled into the runtime serialization table during bootstrap.

## Network cache

The default cache lifetime is **FOREVER**. A value has no expiry unless a TTL is explicitly supplied.

```java
NetworkCache<UUID, PlayerData> players = server.cache(
        "players",
        UUID.class,
        PlayerData.class
);

players.put(uuid, data);
players.put(uuid, data, Duration.ofMinutes(10));
players.remove(uuid);
players.clear();
```

The current cache model remains server-authoritative:

1. a client subscribes to a named cache,
2. the server sends an initial snapshot,
3. writes are applied by the server,
4. the authoritative mutation is broadcast to subscribed clients,
5. server-side TTL expiry is propagated as a removal.

## Logging

Pulsar ships with a small logger abstraction and does not require SLF4J, Log4j or JUL.

Default console logging:

```java
Pulsar.server()
        .port(8080)
        .start();
```

Enable detailed bootstrap registration logs:

```java
Pulsar.server()
        .logLevel(LogLevel.DEBUG)
        .start();
```

Disable all Pulsar logging:

```java
Pulsar.server()
        .logging(false)
        .start();
```

Provide your own logger:

```java
Pulsar.server()
        .logger(new MyPulsarLogger())
        .start();
```

Implement:

```java
public final class MyPulsarLogger implements PulsarLogger {

    @Override
    public boolean enabled(LogLevel level, LogCategory category) {
        return true;
    }

    @Override
    public void log(
            LogLevel level,
            LogCategory category,
            String message,
            Throwable throwable
    ) {
        // Bridge to Bukkit, Velocity, SLF4J, Log4j, your cloud console, ...
    }
}
```

Available categories currently include bootstrap, registry, serialization, events, network, connections, packets, cache and performance.

## Bootstrap diagnostics

Every started endpoint exposes its immutable bootstrap report:

```java
BootstrapReport report = server.bootstrapReport();

System.out.println(report.packets());
System.out.println(report.eventHandlers());
System.out.println(report.serializableTypes());
System.out.println(report.totalMillis());
```

Typical DEBUG output:

```text
INFO  [Pulsar/BOOTSTRAP] Bootstrapping Pulsar 0.3.1-SNAPSHOT
DEBUG [Pulsar/EVENT] Compiled event handler ...
DEBUG [Pulsar/REGISTRY] Registered packet ...
DEBUG [Pulsar/SERIALIZATION] Compiled codec plan for ...
DEBUG [Pulsar/REGISTRY] Packet registry frozen ...
INFO  [Pulsar/BOOTSTRAP] Ready: ... runtime reflection disabled
```

## Lifecycle

Endpoints expose a `RuntimeState` and transition through the bootstrap/freeze/runtime lifecycle. The externally visible runtime normally starts in `FROZEN`, switches to `RUNNING` after Netty is connected/bound, and becomes `CLOSED` when the endpoint is closed.

### Closing endpoints

```java
CompletableFuture<Void> terminated = server.closeAsync();
terminated.thenRun(() -> System.out.println("Server terminated"));
```

`closeAsync()` initiates shutdown and completes after Netty's event loops terminate. `state()` becomes
`CLOSED` when shutdown starts. Calling it repeatedly observes the same shutdown; cancelling a returned
future does not cancel the underlying cleanup.

`close()` waits for termination when called outside the endpoint's own Netty threads. Inside its own
event callbacks it initiates shutdown and returns, so it does not wait on the thread executing the
callback. Do not call `join()` or `get()` on the shutdown future from such a callback.

Event handlers execute synchronously on the calling thread, usually a Netty event loop for received
packets and a scheduler thread for TTL expiry. Move blocking application work to your own executor.

## Protocol limits and deployment assumptions

A frame may occupy at most 16 MiB including its four-byte length prefix. The payload is therefore
limited to `16 * 1024 * 1024 - 4` bytes, including the Pulsar header. Oversized outbound frames fail
locally. Strings are limited to 1 MiB of UTF-8 data, collections to 1,000,000 elements, and byte arrays
to 16 MiB; the overall frame limit may impose a smaller usable array size.

TTL values must be at least one millisecond and fit in an absolute millisecond timestamp. Omitting a
TTL still means forever. Malformed frames close the connection before packet events, subscriptions,
snapshots, or cache mutations are applied. Custom deserializers run during parsing and should avoid
application side effects.

Pulsar currently uses plaintext TCP and does not authenticate peers or authorize individual cache
writes. Connected clients can submit changes to known server caches. Deploy within an appropriate
trusted boundary or an authenticated encrypted transport. Protocol v2 validates the version and packet
ID, but does not negotiate record schemas or serializer configuration; configure both peers consistently.
Concurrent cache operations are eventually replicated without sequence numbers or conflict resolution;
applications needing strict ordering across writer threads must serialize their cache operations.
Automatic reconnect, bounded outbound queues, TLS configuration and schema negotiation remain future work.

## Wire compatibility

`0.3.1-SNAPSHOT` uses Pulsar protocol version **2** and numeric packet wire ids. It is incompatible with
the legacy 0.1.x class-name packet format. Both endpoints must use protocol-v2-compatible builds with
consistent packet definitions and serializer configuration.
