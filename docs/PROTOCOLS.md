# Application protocols sharing a TCP port

Pulsar can recognize multiple application protocols on one listening socket. Each accepted connection
uses one protocol for its lifetime. Different connections can concurrently speak Pulsar, HTTP or a
custom protocol to the same host/port. This does not multiplex differently framed messages inside a
single stream, and does not add UDP, TLS negotiation or a new Pulsar packet format.

## Register an adapter

```java
ApplicationProtocol text = ApplicationProtocol.withPrefix(
        "text",
        "PING".getBytes(StandardCharsets.US_ASCII),
        pipeline -> {
            pipeline.addLast(new LineBasedFrameDecoder(1024));
            pipeline.addLast(new StringDecoder(StandardCharsets.US_ASCII));
            pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                @Override
                protected void channelRead0(ChannelHandlerContext ctx, String line) {
                    ctx.writeAndFlush(Unpooled.copiedBuffer("PONG " + line + "\n", StandardCharsets.US_ASCII));
                }
            });
        }
);

PulsarServer server = Pulsar.server()
        .port(8080)
        .packet(ChatPacket.class)
        .protocol(text)
        .protocol(new HttpStatusProtocol()) // examples module
        .maxProtocolProbeBytes(64)
        .protocolDetectionTimeout(Duration.ofSeconds(5))
        .start();
```

The text connection sends `PING first\n`. Its decoder receives the entire original stream, including
`PING`; later lines can have any prefix. Adapters define the rest of their protocol, including whether
the initial signature is data or should be consumed by a decoder. Netty classes in this snippet are
extension APIs; ordinary Pulsar packet/listener APIs remain unchanged.

## Detection contract

Implement `ApplicationProtocol` when a fixed prefix is insufficient:

- `name()` is a stable unique name matching `[a-z][a-z0-9._-]{0,63}`. `pulsar` is reserved; at most 32
  additional protocols can be registered.
- `detect(prefix)` runs on the receiving event loop. The instance is shared between connections and
  must be thread-safe. It receives only the currently available prefix, capped by the configured
  probe limit, with reader index zero. The probe is read-only and non-owning; do not keep it.
- Return `NEED_MORE_DATA` while bytes are insufficient, `NO_MATCH` once this cannot be your protocol,
  or `MATCH` once its signature is established. MATCH and NO_MATCH must be stable when more bytes arrive.
- All adapters are examined, including Pulsar's native length-prefix/magic signature. Selection waits
  until exactly one matches and none still need more data. Multiple matches close the connection;
  no registration-order priority is used. Avoid overlapping signatures.
- `configure(pipeline)` appends fresh handlers for the chosen connection. It runs exactly once, before
  downstream `channelActive`, then all buffered input is forwarded unchanged. Stateful Netty handlers
  must not be reused between connections. The selector removes itself after setup.
- Configuration/detection failures close the socket. A final fallback closes sockets for exceptions
  left unhandled by custom handlers. Adapters remain responsible for protocol-specific error responses,
  message/body/header limits, outbound flow control and buffer ownership.

The probe limit bounds bytes inspected, not the size of the first TCP read or an application frame.
A large first read with an identifiable prefix is passed to the chosen decoder. An undecided prefix
at the limit, an unknown signature or an expired detection deadline closes the socket. The default
limits are 64 bytes and 5 seconds; accepted configuration ranges are 8..65536 bytes and 1..60000 ms.
Post-selection idle/message deadlines belong to the adapter; the detection timeout is cancelled.

## Native Pulsar behavior

Without additional adapters, initialization bypasses detection and keeps the existing TCP-v2 behavior.
With additional adapters, the client must send first. In particular, a Pulsar `connect()` opens its
socket, but the server's Pulsar `ConnectionOpenEvent` occurs after the first recognizable input.
Do not wait for a server greeting before sending that first packet or cache subscription on a shared
port. A server cannot infer an application protocol from a silent TCP client.

`ProtocolSelectedEvent` is fired once for selected multiplexed connections, before the selected
pipeline receives `channelActive` or buffered messages. Its protocol name and remote address allow
logging/metrics without pretending every socket is a Pulsar packet connection.

`connections()` and `broadcast(...)` cover native Pulsar connections. `activeConnections()` covers all
accepted sockets, including pending detection and non-Pulsar adapters. Server shutdown closes them all.
Existing lifecycle, record serialization, packet IDs and network caches keep their v2 wire format.

## HTTP example

`examples/.../protocol/HttpStatusProtocol.java` provides an HTTP/1.x health endpoint through Netty's
HTTP codec, separate from Pulsar's core runtime dependencies. The adapter recognizes standard method
prefixes; GET/HEAD `/health` return success, other routes/methods return 404. It supports keep-alive,
bounds request bodies to 4096 bytes and closes malformed requests. It does not implement arbitrary
HTTP extension-method detection, WebSocket upgrades, HTTP/2, HTTPS or general application routing.
A custom adapter can install those pipelines where needed, with an appropriate distinguishing prefix.

Protocol detection is routing, not a trust or authorization boundary. Application-level permissions,
peer authentication, TLS and deployment limits still need to be chosen for the service using Pulsar.
