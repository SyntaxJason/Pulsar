# Pulsar examples

The examples are intentionally small and use only Pulsar's public API.

## Structure

- `shared/ChatPacket.java` — a packet as a plain Java record
- `shared/LocationSerializer.java` — custom serialization without exposing Netty `ByteBuf`
- `server/ServerExample.java` — starts a Pulsar server and creates a network cache
- `server/ServerListener.java` — Bukkit-style connection, packet and cache listeners
- `client/ClientExample.java` — connects, sends packets and mutates the network cache
- `client/ClientListener.java` — receives packets and cache updates

## Run

First install Pulsar from the repository root:

```bash
mvn clean install
```

Then build the examples:

```bash
cd examples
mvn clean package
```

Run `ServerExample` first and then `ClientExample` from your IDE.

## Cache lifetime

`cache.put(key, value)` has no expiry. The value lives forever until it is replaced, removed, or the whole cache is cleared.

A TTL only exists when explicitly supplied:

```java
cache.put(key, value, Duration.ofMinutes(5));
```
