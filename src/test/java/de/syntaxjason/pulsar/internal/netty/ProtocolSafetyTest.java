package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;
import de.syntaxjason.pulsar.serialization.SerializationReader;
import de.syntaxjason.pulsar.serialization.SerializationWriter;
import de.syntaxjason.pulsar.packet.Packet;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolSafetyTest {
    record Message(String text) implements Packet { }

    @Test
    void trailingBytesMustNotDispatchPacket() {
        AtomicInteger received = new AtomicInteger();
        Listener listener = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Message> event) { received.incrementAndGet(); }
        };
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class), listener)) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
            try {
                ByteBuf frame = frame(FrameType.PACKET);
                frame.writeInt(endpoint.packets.wireId(Message.class));
                endpoint.binarySerializer().write(frame, Message.class, new Message("hello"));
                frame.writeByte(99);
                channel.writeInbound(frame);
                assertEquals(0, received.get());
                assertFalse(channel.isActive());
                assertInstanceOf(ProtocolException.class, endpoint.failures.getFirst());
                assertEquals(0, frame.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void trailingBytesMustNotSubscribeOrMutate() {
        for (FrameType type : List.of(FrameType.CACHE_SUBSCRIBE, FrameType.CACHE_CLEAR)) {
            try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
                endpoint.caches.cache("test", String.class, String.class);
                EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
                try {
                    ByteBuf frame = frame(type);
                    endpoint.binarySerializer().write(frame, String.class, "test");
                    frame.writeByte(99);
                    channel.writeInbound(frame);
                    assertEquals(0, endpoint.subscriptions);
                    assertEquals(0, endpoint.mutations);
                    assertFalse(channel.isActive());
                } finally { channel.finishAndReleaseAll(); }
            }
        }
    }

    @Test
    void nullPacketMustBeRejectedBeforeDispatch() {
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class))) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
            try {
                ByteBuf frame = frame(FrameType.PACKET).writeInt(endpoint.packets.wireId(Message.class)).writeBoolean(false);
                channel.writeInbound(frame);
                assertFalse(channel.isActive());
                assertInstanceOf(ProtocolException.class, endpoint.failures.getFirst());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void failedSerializationReleasesAllocatedBuffer() {
        SerializerRegistry serializers = new SerializerRegistry();
        serializers.register(Message.class, new TypeSerializer<>() {
            @Override public void write(SerializationWriter writer, Message value) {
                writer.writeInt(123);
                throw new IllegalArgumentException("broken serializer");
            }
            @Override public Message read(SerializationReader reader) { return new Message(""); }
        });
        try (TestEndpoint endpoint = new TestEndpoint(true, serializers, List.of(Message.class))) {
            EmbeddedChannel channel = new EmbeddedChannel();
            TrackingAllocator allocator = new TrackingAllocator();
            channel.config().setAllocator(allocator);
            try {
                NettyConnection connection = new NettyConnection(channel, endpoint);
                assertThrows(RuntimeException.class, () -> connection.send(new Message("hello")));
                assertNotNull(allocator.last);
                assertEquals(0, allocator.last.refCnt());
                assertNull(channel.readOutbound());
            } finally {
                channel.finishAndReleaseAll();
                if (allocator.last != null && allocator.last.refCnt() > 0) allocator.last.release();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-2, 0, 1})
    void invalidSnapshotCannotReplaceExistingCache(int invalid) {
        try (TestEndpoint endpoint = new TestEndpoint(false, new SerializerRegistry(), List.of())) {
            var cache = endpoint.caches.cache("test", String.class, String.class);
            endpoint.caches.applySnapshot("test", List.of(
                    new de.syntaxjason.pulsar.internal.cache.CacheSnapshotEntry("old", "value", -1)));
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
            try {
                ByteBuf frame = frame(FrameType.CACHE_SNAPSHOT);
                endpoint.binarySerializer().write(frame, String.class, "test");
                frame.writeInt(1).writeLong(invalid == 1 ? -1 : invalid);
                endpoint.binarySerializer().write(frame, String.class, invalid == 1 ? null : "key");
                endpoint.binarySerializer().write(frame, String.class, "value");
                channel.writeInbound(frame);
                assertFalse(channel.isActive());
                assertEquals("value", cache.get("old"));
                assertEquals(1, cache.size());
                assertInstanceOf(ProtocolException.class, endpoint.failures.getFirst());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void trailingSnapshotCannotReplaceExistingCache() {
        try (TestEndpoint endpoint = new TestEndpoint(false, new SerializerRegistry(), List.of())) {
            var cache = endpoint.caches.cache("test", String.class, String.class);
            endpoint.caches.applySnapshot("test", List.of(
                    new de.syntaxjason.pulsar.internal.cache.CacheSnapshotEntry("old", "value", -1)));
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
            try {
                ByteBuf frame = frame(FrameType.CACHE_SNAPSHOT);
                endpoint.binarySerializer().write(frame, String.class, "test");
                frame.writeInt(0).writeByte(99);
                channel.writeInbound(frame);
                assertFalse(channel.isActive());
                assertEquals("value", cache.get("old"));
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
    void truncatedPacketsAreRejectedAndReleased(int length) {
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class))) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
            try {
                ByteBuf complete = frame(FrameType.PACKET).writeInt(endpoint.packets.wireId(Message.class));
                ByteBuf truncated = complete.copy(0, length);
                complete.release();
                channel.writeInbound(truncated);
                assertFalse(channel.isActive());
                assertEquals(0, truncated.refCnt());
                assertEquals(1, endpoint.failures.size());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void validPacketIsDispatchedExactlyOnce() {
        AtomicInteger received = new AtomicInteger();
        Listener listener = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Message> event) {
                assertEquals(new Message("hello"), event.packet());
                received.incrementAndGet();
            }
        };
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class), listener)) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarInboundHandler(endpoint));
            try {
                ByteBuf frame = ProtocolWriter.encodePacket(endpoint.connection, new Message("hello"));
                channel.writeInbound(frame);
                assertEquals(1, received.get());
                assertTrue(channel.isActive());
                assertTrue(endpoint.failures.isEmpty());
                assertEquals(0, frame.refCnt());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    private static ByteBuf frame(FrameType type) {
        return Unpooled.buffer().writeInt(ProtocolWriter.MAGIC).writeByte(ProtocolWriter.VERSION).writeByte(type.id());
    }

    private static final class TrackingAllocator extends AbstractByteBufAllocator {
        ByteBuf last;
        TrackingAllocator() { super(false); }
        @Override protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            return last = Unpooled.buffer(initialCapacity, maxCapacity);
        }
        @Override protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            return last = Unpooled.directBuffer(initialCapacity, maxCapacity);
        }
        @Override public boolean isDirectBufferPooled() { return false; }
    }
}
