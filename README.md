# Pulsar

Pulsar is an intuitive Java networking library powered by Netty. Application code should feel closer to Bukkit events than to a low-level networking framework while the runtime keeps the hot path small and predictable.

## Maven

```xml
<repository>
    <id>SyntaxJason-Beta</id>
    <url>https://repo.syntaxjason.de/beta</url>
</repository>

<dependency>
    <groupId>de.syntaxjason</groupId>
    <artifactId>pulsar</artifactId>
    <version>0.2.0-BETA</version>
</dependency>
```

## Requirements

- Java 21+
- Maven
- Netty 4.2.x

## 0.2.0-BETA: bootstrap first, reflection-free runtime

Pulsar 0.2.0-BETA introduces a strict bootstrap/freeze model.

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
INFO  [Pulsar/BOOTSTRAP] Bootstrapping Pulsar 0.2.0-BETA
DEBUG [Pulsar/EVENT] Compiled event handler ...
DEBUG [Pulsar/REGISTRY] Registered packet ...
DEBUG [Pulsar/SERIALIZATION] Compiled codec plan for ...
DEBUG [Pulsar/REGISTRY] Packet registry frozen ...
INFO  [Pulsar/BOOTSTRAP] Ready: ... runtime reflection disabled
```

## Lifecycle

Endpoints expose a `RuntimeState` and transition through the bootstrap/freeze/runtime lifecycle. The externally visible runtime normally starts in `FROZEN`, switches to `RUNNING` after Netty is connected/bound, and becomes `CLOSED` when the endpoint is closed.

## 0.2.0-BETA wire compatibility

0.2.0-BETA uses Pulsar protocol version **2** and numeric packet wire ids. It is intentionally not wire-compatible with the previous 0.1.x class-name packet format. Both sides of a connection must use 0.2.0-BETA or another protocol-v2-compatible build.
