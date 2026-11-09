package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.connection.ProtocolSelectedEvent;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.protocol.ApplicationProtocol;
import de.syntaxjason.pulsar.protocol.ProtocolDetection;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolMultiplexerTest {
    record Message(String text) implements Packet { }

    @Test
    void nativeFramesSurviveOneByteFragmentationAndSelectionIsRemoved() {
        List<String> events = new ArrayList<>();
        Listener listener = new Listener() {
            @EventHandler public void selected(ProtocolSelectedEvent event) { events.add(event.protocol()); }
            @EventHandler public void packet(PacketReceiveEvent<Message> event) { events.add(event.packet().text()); }
        };
        AtomicInteger probes = new AtomicInteger();
        ApplicationProtocol alternative = protocol("other", prefix -> {
            probes.incrementAndGet();
            return ProtocolDetection.NO_MATCH;
        });
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class), listener)) {
            EmbeddedChannel channel = channel(endpoint, alternative);
            ByteBuf first = wire(endpoint, channel, "first");
            ByteBuf second = wire(endpoint, channel, "second");
            try {
                assertNull(endpoint.connection);
                while (first.isReadable()) channel.writeInbound(first.readRetainedSlice(1));
                assertEquals(List.of("pulsar", "first"), events);
                assertNotNull(endpoint.connection);
                assertNull(channel.pipeline().get(ProtocolMultiplexer.class));
                int before = probes.get();
                channel.writeInbound(second.retain());
                assertEquals(List.of("pulsar", "first", "second"), events);
                assertEquals(before, probes.get());
                assertTrue(channel.isActive());
            } finally { first.release(); second.release(); channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void customProtocolGetsChannelActiveBeforeTheUntouchedBufferedPrefix() {
        List<String> events = new ArrayList<>();
        ApplicationProtocol custom = ApplicationProtocol.withPrefix("custom", bytes("PING"), pipeline ->
                pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                    @Override public void channelActive(ChannelHandlerContext ctx) { events.add("active"); }
                    @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf message) {
                        events.add(message.toString(StandardCharsets.US_ASCII));
                    }
                }));
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = channel(endpoint, custom);
            ByteBuf prefix = Unpooled.copiedBuffer("PI", StandardCharsets.US_ASCII);
            ByteBuf remainder = Unpooled.copiedBuffer("NG body", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(prefix);
                assertTrue(events.isEmpty());
                channel.writeInbound(remainder);
                assertEquals(List.of("active", "PING body"), events);
                assertNull(endpoint.connection);
                assertEquals(0, prefix.refCnt());
                assertEquals(0, remainder.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void detectorCannotConsumeMutateOrReleaseAnotherDetectorsProbe() {
        AtomicInteger checked = new AtomicInteger();
        ApplicationProtocol first = protocol("first", prefix -> {
            assertTrue(prefix.isReadOnly());
            assertThrows(java.nio.ReadOnlyBufferException.class, () -> prefix.setByte(0, 0));
            prefix.readByte();
            assertFalse(prefix.release());
            return ProtocolDetection.NO_MATCH;
        });
        ApplicationProtocol second = new ApplicationProtocol() {
            @Override public String name() { return "second"; }
            @Override public ProtocolDetection detect(ByteBuf prefix) {
                assertEquals(0, prefix.readerIndex());
                assertEquals('X', prefix.readUnsignedByte());
                checked.incrementAndGet();
                return ProtocolDetection.MATCH;
            }
            @Override public void configure(io.netty.channel.ChannelPipeline pipeline) {
                pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                    @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf message) {
                        assertEquals("XYZ", message.toString(StandardCharsets.US_ASCII));
                    }
                });
            }
        };
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint, List.of(first, second), 64, 5000));
            ByteBuf input = Unpooled.copiedBuffer("XYZ", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(input);
                assertEquals(1, checked.get());
                assertEquals(0, input.refCnt());
                assertTrue(channel.isActive());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void ambiguousProtocolsCloseWithoutConfiguringEither() {
        AtomicInteger configured = new AtomicInteger();
        var one = ApplicationProtocol.withPrefix("one", bytes("X"), pipeline -> configured.incrementAndGet());
        var two = ApplicationProtocol.withPrefix("two", bytes("X"), pipeline -> configured.incrementAndGet());
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint, List.of(one, two), 64, 5000));
            ByteBuf input = Unpooled.copiedBuffer("X", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(input);
                assertFalse(channel.isActive());
                assertEquals(0, configured.get());
                assertEquals(0, input.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void matchingShortSignatureWaitsForACompetingLongerSignature() {
        List<String> selected = new ArrayList<>();
        var shortOne = ApplicationProtocol.withPrefix("short", bytes("X"), pipeline -> selected.add("short"));
        var longOne = ApplicationProtocol.withPrefix("long", bytes("XY"), pipeline -> selected.add("long"));
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint, List.of(shortOne, longOne), 64, 5000));
            try {
                channel.writeInbound(Unpooled.copiedBuffer("X", StandardCharsets.US_ASCII));
                assertTrue(selected.isEmpty());
                assertTrue(channel.isActive());
                channel.writeInbound(Unpooled.copiedBuffer("Y", StandardCharsets.US_ASCII));
                assertFalse(channel.isActive());
                assertTrue(selected.isEmpty());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "throw", "unknown", "configure"})
    void detectorFailuresAndUnrecognizedPrefixesReleaseInput(String failure) {
        ApplicationProtocol alternative = new ApplicationProtocol() {
            @Override public String name() { return "test"; }
            @Override public ProtocolDetection detect(ByteBuf prefix) {
                if (failure.equals("null")) return null;
                if (failure.equals("throw")) throw new IllegalStateException("broken detector");
                return failure.equals("configure") ? ProtocolDetection.MATCH : ProtocolDetection.NO_MATCH;
            }
            @Override public void configure(io.netty.channel.ChannelPipeline pipeline) { throw new IllegalStateException("broken installer"); }
        };
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = channel(endpoint, alternative);
            ByteBuf input = Unpooled.copiedBuffer("unknown", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(input);
                assertFalse(channel.isActive());
                assertEquals(0, input.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void probeBudgetRejectsUndecidedProtocolEvenWithALargeFirstRead() {
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint,
                    List.of(protocol("pending", prefix -> {
                        assertEquals(8, prefix.readableBytes());
                        return ProtocolDetection.NEED_MORE_DATA;
                    })), 8, 5000));
            ByteBuf input = Unpooled.copiedBuffer("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(input);
                assertFalse(channel.isActive());
                assertEquals(0, input.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void detectionTimeoutReleasesPartialInputAndIsCancelledAfterSelection() {
        for (boolean select : List.of(false, true)) {
            try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
                var custom = ApplicationProtocol.withPrefix("custom", bytes("XY"), pipeline ->
                        pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                            @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf message) { }
                        }));
                EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint, List.of(custom), 64, 100));
                channel.freezeTime();
                ByteBuf input = Unpooled.copiedBuffer(select ? "XY" : "X", StandardCharsets.US_ASCII);
                try {
                    channel.writeInbound(input);
                    channel.advanceTimeBy(101, TimeUnit.MILLISECONDS);
                    channel.runScheduledPendingTasks();
                    assertEquals(select, channel.isActive());
                    assertEquals(0, input.refCnt());
                } finally { channel.finishAndReleaseAll(); }
            }
        }
    }

    @Test
    void nativeAndCustomSignatureCollisionDoesNotOpenAPulsarConnection() {
        AtomicInteger installs = new AtomicInteger();
        ApplicationProtocol custom = ApplicationProtocol.withPrefix("impostor", new byte[]{0}, pipeline -> installs.incrementAndGet());
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class))) {
            EmbeddedChannel channel = channel(endpoint, custom);
            ByteBuf frame = wire(endpoint, channel, "data");
            try {
                channel.writeInbound(frame);
                assertFalse(channel.isActive());
                assertNull(endpoint.connection);
                assertEquals(0, installs.get());
                assertEquals(0, frame.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"active", "read"})
    void unhandledCustomHandlerExceptionsCloseAndReleaseTheConnection(String phase) {
        ApplicationProtocol custom = ApplicationProtocol.withPrefix("broken", bytes("X"), pipeline ->
                pipeline.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                    @Override public void channelActive(ChannelHandlerContext ctx) {
                        if (phase.equals("active")) throw new IllegalStateException("active failed");
                    }
                    @Override protected void channelRead0(ChannelHandlerContext ctx, ByteBuf message) {
                        throw new IllegalStateException("read failed");
                    }
                }));
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = channel(endpoint, custom);
            ByteBuf input = Unpooled.copiedBuffer("X", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(input);
                assertFalse(channel.isActive());
                assertEquals(0, input.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void closingFromSelectionEventDoesNotActivateTheSelectedProtocol() {
        java.util.concurrent.atomic.AtomicReference<EmbeddedChannel> reference = new java.util.concurrent.atomic.AtomicReference<>();
        AtomicInteger activeEvents = new AtomicInteger();
        Listener listener = new Listener() {
            @EventHandler public void selected(ProtocolSelectedEvent event) { reference.get().close(); }
        };
        var custom = ApplicationProtocol.withPrefix("custom", bytes("X"), pipeline ->
                pipeline.addLast(new ChannelInboundHandlerAdapter() {
                    @Override public void channelActive(ChannelHandlerContext ctx) { activeEvents.incrementAndGet(); }
                }));
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(), listener)) {
            EmbeddedChannel channel = channel(endpoint, custom);
            reference.set(channel);
            ByteBuf input = Unpooled.copiedBuffer("X", StandardCharsets.US_ASCII);
            try {
                channel.writeInbound(input);
                assertFalse(channel.isActive());
                assertEquals(0, activeEvents.get());
                assertEquals(0, input.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    private static EmbeddedChannel channel(TestEndpoint endpoint, ApplicationProtocol protocol) {
        return new EmbeddedChannel(new PulsarChannelInitializer(endpoint, List.of(protocol), 64, 5000));
    }

    private static ByteBuf wire(TestEndpoint endpoint, EmbeddedChannel channel, String text) {
        ByteBuf payload = ProtocolWriter.encodePacket(new NettyConnection(channel, endpoint), new Message(text));
        try { return Unpooled.buffer().writeInt(payload.readableBytes()).writeBytes(payload); }
        finally { payload.release(); }
    }

    private static byte[] bytes(String value) { return value.getBytes(StandardCharsets.US_ASCII); }

    private static ApplicationProtocol protocol(String name, java.util.function.Function<ByteBuf, ProtocolDetection> detect) {
        return new ApplicationProtocol() {
            @Override public String name() { return name; }
            @Override public ProtocolDetection detect(ByteBuf prefix) { return detect.apply(prefix); }
            @Override public void configure(io.netty.channel.ChannelPipeline pipeline) { }
        };
    }
}
