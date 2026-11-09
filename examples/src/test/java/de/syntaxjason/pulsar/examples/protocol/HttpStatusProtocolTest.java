package de.syntaxjason.pulsar.examples.protocol;

import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.protocol.ProtocolDetection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class HttpStatusProtocolTest {
    record Echo(String value) implements Packet { }

    @ParameterizedTest
    @ValueSource(strings = {"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE", "CONNECT"})
    void detectsStandardHttpMethodsUnderFragmentation(String method) {
        HttpStatusProtocol protocol = new HttpStatusProtocol();
        ByteBuf prefix = Unpooled.copiedBuffer(method + " /health", StandardCharsets.US_ASCII);
        try {
            for (int count = 1; count <= method.length(); count++) {
                assertEquals(ProtocolDetection.NEED_MORE_DATA, protocol.detect(prefix.slice(0, count)));
            }
            assertEquals(ProtocolDetection.MATCH, protocol.detect(prefix));
        } finally { prefix.release(); }
    }

    @Test
    void getAndHeadUseCorrectBodyLengthsAndKeepAlive() {
        EmbeddedChannel channel = new EmbeddedChannel();
        new HttpStatusProtocol().configure(channel.pipeline());
        try {
            channel.writeInbound(request("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n"));
            String get = response(channel);
            assertTrue(get.startsWith("HTTP/1.1 200 OK\r\n"));
            assertTrue(get.toLowerCase().contains("content-length: 3\r\n"));
            assertTrue(get.endsWith("\r\n\r\nok\n"));
            assertTrue(channel.isActive());
            channel.writeInbound(request("HEAD /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"));
            String head = response(channel);
            assertTrue(head.startsWith("HTTP/1.1 200 OK\r\n"));
            assertTrue(head.toLowerCase().contains("content-length: 3\r\n"));
            assertTrue(head.endsWith("\r\n\r\n"));
            assertFalse(channel.isActive());
        } finally { channel.finishAndReleaseAll(); }
    }

    @ParameterizedTest
    @ValueSource(strings = {"GET /missing", "POST /health"})
    void unsupportedRoutesAndMethodsReturnNotFound(String requestLine) {
        EmbeddedChannel channel = new EmbeddedChannel();
        new HttpStatusProtocol().configure(channel.pipeline());
        try {
            channel.writeInbound(request(requestLine + " HTTP/1.1\r\nHost: localhost\r\n\r\n"));
            assertTrue(response(channel).startsWith("HTTP/1.1 404 Not Found\r\n"));
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    void malformedHeadersReturnBadRequestAndClose() {
        EmbeddedChannel channel = new EmbeddedChannel();
        new HttpStatusProtocol().configure(channel.pipeline());
        try {
            channel.writeInbound(request("GET /health HTTP/1.1\r\nBad Header: value\r\n\r\n"));
            assertTrue(response(channel).startsWith("HTTP/1.1 400 Bad Request\r\n"));
            assertFalse(channel.isActive());
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    void oversizedRequestIsRejectedBeforeApplicationResponse() {
        EmbeddedChannel channel = new EmbeddedChannel();
        new HttpStatusProtocol().configure(channel.pipeline());
        try {
            channel.writeInbound(request("POST /health HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5000\r\n\r\n"));
            assertTrue(response(channel).startsWith("HTTP/1.1 413 Request Entity Too Large\r\n"));
        } finally { channel.finishAndReleaseAll(); }
    }

    @Test
    @Timeout(15)
    void actualExampleAdapterAndNativePulsarRespondOnTheSamePort() throws Exception {
        CompletableFuture<Echo> echo = new CompletableFuture<>();
        try (var server = Pulsar.server().host("127.0.0.1").port(0).logging(false)
                .protocol(new HttpStatusProtocol()).listener(new Listener() {
                    @EventHandler public void receive(PacketReceiveEvent<Echo> event) { event.connection().send(event.packet()); }
                }).start();
             var client = Pulsar.client().address("127.0.0.1", server.port()).logging(false).listener(new Listener() {
                 @EventHandler public void receive(PacketReceiveEvent<Echo> event) { echo.complete(event.packet()); }
             }).connect();
             var http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                     .connectTimeout(Duration.ofSeconds(3)).build()) {
            client.send(new Echo("same port"));
            assertEquals(new Echo("same port"), echo.get(3, TimeUnit.SECONDS));
            var response = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/health"))
                    .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertEquals("ok\n", response.body());
        }
    }

    private static ByteBuf request(String text) { return Unpooled.copiedBuffer(text, StandardCharsets.US_ASCII); }

    private static String response(EmbeddedChannel channel) {
        StringBuilder text = new StringBuilder();
        ByteBuf buffer;
        while ((buffer = channel.readOutbound()) != null) {
            try { text.append(buffer.toString(StandardCharsets.US_ASCII)); }
            finally { buffer.release(); }
        }
        return text.toString();
    }
}
