package de.syntaxjason.pulsar;

import de.syntaxjason.pulsar.client.PulsarClient;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.connection.ConnectionCloseEvent;
import de.syntaxjason.pulsar.event.connection.ProtocolSelectedEvent;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.protocol.ApplicationProtocol;
import de.syntaxjason.pulsar.server.PulsarServer;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.string.StringDecoder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class SharedPortIntegrationTest {
    record Echo(String text) implements Packet { }

    @Test
    void pulsarHttpAndCustomTextShareOnePortAndShutdownClosesEverySocket() throws Exception {
        BlockingQueue<String> selections = new LinkedBlockingQueue<>();
        CompletableFuture<Echo> reply = new CompletableFuture<>();
        CompletableFuture<Void> clientClosed = new CompletableFuture<>();
        Listener serverEvents = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Echo> event) { event.connection().send(event.packet()); }
            @EventHandler public void selected(ProtocolSelectedEvent event) { selections.add(event.protocol()); }
        };
        Listener clientEvents = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Echo> event) { reply.complete(event.packet()); }
            @EventHandler public void closed(ConnectionCloseEvent event) { clientClosed.complete(null); }
        };
        PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false)
                .protocol(http()).protocol(text()).listener(serverEvents).start();
        PulsarClient client = null;
        try (Socket http = socket(server.port()); Socket text = socket(server.port())) {
            client = Pulsar.client().address("127.0.0.1", server.port()).logging(false).listener(clientEvents).connect();
            client.send(new Echo("native"));
            assertEquals(new Echo("native"), reply.get(5, TimeUnit.SECONDS));

            text.getOutputStream().write("PING first\n".getBytes(StandardCharsets.US_ASCII));
            var textReader = new BufferedReader(new InputStreamReader(text.getInputStream(), StandardCharsets.US_ASCII));
            assertEquals("PONG PING first", textReader.readLine());
            // The signature is checked once; subsequent frames need not begin with PING.
            text.getOutputStream().write("second\n".getBytes(StandardCharsets.US_ASCII));
            assertEquals("PONG second", textReader.readLine());

            var httpReader = new BufferedReader(new InputStreamReader(http.getInputStream(), StandardCharsets.US_ASCII));
            for (int i = 0; i < 2; i++) {
                http.getOutputStream().write("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
                assertEquals("HTTP/1.1 200 OK", httpReader.readLine());
                String header;
                do { header = httpReader.readLine(); assertNotNull(header); } while (!header.isEmpty());
                assertEquals('o', httpReader.read());
                assertEquals('k', httpReader.read());
            }
            Set<String> selected = new HashSet<>();
            for (int i = 0; i < 3; i++) selected.add(selections.poll(5, TimeUnit.SECONDS));
            assertEquals(Set.of("http", "text", "pulsar"), selected);
            assertTrue(selections.isEmpty());
            assertEquals(3, server.activeConnections());
            assertEquals(1, server.connections().size());

            server.closeAsync().get(5, TimeUnit.SECONDS);
            assertEquals(-1, httpReader.read());
            assertEquals(-1, textReader.read());
            clientClosed.get(5, TimeUnit.SECONDS);
            assertFalse(client.connected());
            assertEquals(0, server.activeConnections());
        } finally {
            CompletableFuture.allOf(server.closeAsync(), client == null
                    ? CompletableFuture.completedFuture(null) : client.closeAsync()).get(6, TimeUnit.SECONDS);
        }
    }

    @Test
    void idleClientIsClosedByProtocolDetectionDeadline() throws Exception {
        try (PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false)
                .protocol(text()).protocolDetectionTimeout(Duration.ofMillis(50)).start();
             Socket client = socket(server.port())) {
            client.getOutputStream().write('P');
            assertEquals(-1, client.getInputStream().read());
            assertTrue(server.connections().isEmpty());
        }
    }

    private static Socket socket(int port) throws Exception {
        Socket socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(5000);
        return socket;
    }

    private static ApplicationProtocol text() {
        return ApplicationProtocol.withPrefix("text", "PING".getBytes(StandardCharsets.US_ASCII), pipeline -> {
            pipeline.addLast(new LineBasedFrameDecoder(1024));
            pipeline.addLast(new StringDecoder(StandardCharsets.US_ASCII));
            pipeline.addLast(new SimpleChannelInboundHandler<String>() {
                @Override protected void channelRead0(ChannelHandlerContext ctx, String value) {
                    ctx.writeAndFlush(Unpooled.copiedBuffer("PONG " + value + "\n", StandardCharsets.US_ASCII));
                }
            });
        });
    }

    private static ApplicationProtocol http() {
        return ApplicationProtocol.withPrefix("http", "GET ".getBytes(StandardCharsets.US_ASCII), pipeline -> {
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpObjectAggregator(4096));
            pipeline.addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
                @Override protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
                    var response = new DefaultFullHttpResponse(request.protocolVersion(), HttpResponseStatus.OK,
                            Unpooled.copiedBuffer("ok", StandardCharsets.US_ASCII));
                    HttpUtil.setContentLength(response, 2);
                    HttpUtil.setKeepAlive(response, true);
                    ctx.writeAndFlush(response);
                }
            });
        });
    }
}
