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

## Shared-port protocols

`protocol/SharedPortServer.java` runs ordinary Pulsar packets and an HTTP/1.x health endpoint on
TCP port 8080. Start it from your IDE, then request:

```bash
curl http://127.0.0.1:8080/health
```

A normal Pulsar client can use `127.0.0.1:8080` at the same time; it must send a packet or subscribe to
a cache before the server can select its protocol. The HTTP adapter recognizes standard method
prefixes, handles GET/HEAD for `/health`, supports keep-alive, returns 404 for other routes and bounds
aggregated request bodies to 4096 bytes. It is a small adapter example, not a complete web framework.

The examples project declares `netty-codec-http` explicitly. Pulsar core does not bring it into an
application's runtime dependencies. `mvn -f examples/pom.xml verify` also tests the actual example adapter,
including a real HTTP request alongside a native Pulsar connection on the same port.
