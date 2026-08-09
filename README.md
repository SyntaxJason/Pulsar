# Pulsar

## Maven

```xml
<repository>
    <id>SyntaxJason-Beta</id>
    <url>https://www.syntaxjason.de/maven/beta</url>
</repository>

<dependency>
    <groupId>de.syntaxjason</groupId>
    <artifactId>pulsar</artifactId>
    <version>0.1.0-Beta</version>
</dependency>
```

Pulsar is a small, intuitive Java networking library powered by Netty.

The design goal is simple: application code should feel closer to Bukkit events than to a low-level networking framework. Netty stays under the hood; normal users work with records, listeners, connections and network caches.

## Requirements

- Java 21+
- Maven
- Netty 4.2.x (managed by Pulsar's Maven configuration)

## Packet in one record

```java
import de.syntaxjason.pulsar.packet.Packet;

public record ChatPacket(
        String sender,
        String message
) implements Packet {
}
```

No encoder, decoder or numeric packet id is required.

For normal class-name based packets, Pulsar automatically resolves and registers the packet type on receive. If a stable wire id is desired, use `@PacketType("chat")` and register that packet on both endpoints for now. A compile-time packet index is planned.

## Server

```java
import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.server.PulsarServer;

PulsarServer server = Pulsar.server()
        .port(8080)
        .start();
```

Send to everyone:

```java
server.broadcast(new ChatPacket("Server", "Hello network!"));
```

## Client

```java
import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.client.PulsarClient;

PulsarClient client = Pulsar.client()
        .address("127.0.0.1", 8080)
        .connect();

client.send(new ChatPacket("Jason", "Moin"));
```

## Bukkit-style events

```java
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;

public final class ChatListener implements Listener {

    @EventHandler
    public void onChat(PacketReceiveEvent<ChatPacket> event) {
        ChatPacket packet = event.packet();
        System.out.println(packet.sender() + ": " + packet.message());
    }
}
```

Register it:

```java
server.events().register(new ChatListener());
```

Connection lifecycle uses the same event system:

```java
@EventHandler
public void onConnect(ConnectionOpenEvent event) {
    System.out.println(event.connection().remoteAddress());
}
```

## Network cache

The default is **FOREVER**. A value has no expiry unless a TTL is explicitly supplied.

```java
public record PlayerData(
        UUID uuid,
        String name,
        long coins
) {
}
```

Create the same named cache on server and clients:

```java
NetworkCache<UUID, PlayerData> players = server.cache(
        "players",
        UUID.class,
        PlayerData.class
);
```

Normal map-like use:

```java
players.put(uuid, data);       // infinite until replaced/removed
PlayerData data = players.get(uuid);
players.remove(uuid);
players.clear();
```

Optional TTL:

```java
players.put(uuid, data, Duration.ofMinutes(10));
```

The current cache model is server-authoritative. Client-side mutations while disconnected are rejected instead of being silently lost on the next synchronization.

1. a client subscribes to a named cache,
2. the server sends an initial snapshot,
3. writes are applied by the server,
4. the authoritative mutation is broadcast to every subscribed client, including the origin,
5. TTL expiry is decided by the server and propagated as a removal.

Cache events also use the Pulsar event bus:

```java
@EventHandler
public void onCachePut(CachePutEvent<UUID, PlayerData> event) {
    System.out.println(event.cache() + " -> " + event.key());
}
```

## Automatic serialization

The initial binary serializer supports:

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
- custom serializers

Custom types do not expose Netty's `ByteBuf`:

```java
public final class LocationSerializer implements TypeSerializer<Location> {

    @Override
    public void write(SerializationWriter out, Location value) {
        out.writeString(value.world());
        out.writeDouble(value.x());
        out.writeDouble(value.y());
        out.writeDouble(value.z());
    }

    @Override
    public Location read(SerializationReader in) {
        return new Location(
                in.readString(),
                in.readDouble(),
                in.readDouble(),
                in.readDouble()
        );
    }
}
```

Register it when building an endpoint:

```java
Pulsar.server()
        .serializer(Location.class, new LocationSerializer())
        .port(8080)
        .start();
```

## Current v0.1 foundation

Already implemented:

- Netty TCP server/client
- 4-byte length-prefixed frames
- protocol magic + protocol version
- packet send/receive
- automatic record serialization/deserialization
- packet type auto-resolution
- `@PacketType` aliases
- Bukkit-style listener/event dispatch with priorities and cancellable events
- connection open/close events
- replicated `NetworkCache<K, V>`
- forever-by-default cache values
- optional cache TTL
- initial cache snapshots
- cache put/remove/clear/expire/sync events
- custom serializers without leaking `ByteBuf`
- frame and collection size validation

Next architectural steps:

- request/response API
- heartbeats and connection health
- automatic reconnect
- TLS
- authentication/handshake metadata
- native transport auto-selection (`io_uring` / epoll / kqueue / NIO)
- compile-time packet index for stable packet aliases without manual registration
- cache revisions/delta resync
- atomic cache operations / single-flight loaders
- `NetworkValue<T>` and `NetworkSet<T>`
- backpressure / write watermarks
- protocol compatibility negotiation

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the current wire format.

## Examples

A complete server/client example is included in [`examples/`](examples/). It covers:

- packet records
- Bukkit-style packet and connection listeners
- `NetworkCache<K, V>` with FOREVER-by-default entries
- explicit TTL values
- custom serializers
- server replies via `Connection#send(...)`

Build Pulsar first with `mvn clean install`, then build the examples from the `examples` directory.


## Threading note

Network-originated listeners currently execute on Netty I/O threads. Local cache mutations execute on the caller thread and TTL expiry events execute on Pulsar's cache-expiry thread. Do not block a Netty event handler with database or filesystem work. A configurable event executor is planned before the stable API.
