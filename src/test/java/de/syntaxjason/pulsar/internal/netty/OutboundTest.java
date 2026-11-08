package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.serialization.SerializationReader;
import de.syntaxjason.pulsar.serialization.SerializationWriter;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class OutboundTest {
    record Message(String text) implements Packet { }

    @Test
    void batchPreservesOrderAndFlushesOnce() {
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class))) {
            FlushCounter flushes = new FlushCounter();
            EmbeddedChannel channel = new EmbeddedChannel(flushes);
            try {
                NettyConnection connection = new NettyConnection(channel, endpoint);
                connection.sendAll(List.of(new Message("one"), new Message("two"), new Message("three")));
                channel.runPendingTasks();
                assertEquals(1, flushes.count);
                for (String text : List.of("one", "two", "three")) {
                    ByteBuf frame = channel.readOutbound();
                    try {
                        frame.skipBytes(10);
                        assertEquals(new Message(text), endpoint.binarySerializer().read(frame, Message.class));
                        assertFalse(frame.isReadable());
                    } finally { frame.release(); }
                }
                assertNull(channel.readOutbound());
                connection.sendAll(List.of());
                assertEquals(1, flushes.count);
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void failedBatchWritesNothingAndReleasesAllBuffers() {
        AtomicInteger writes = new AtomicInteger();
        try (TestEndpoint endpoint = new TestEndpoint(true, serializers(writes), List.of(Message.class))) {
            FlushCounter flushes = new FlushCounter();
            EmbeddedChannel channel = new EmbeddedChannel(flushes);
            TrackingAllocator allocator = new TrackingAllocator();
            channel.config().setAllocator(allocator);
            try {
                NettyConnection connection = new NettyConnection(channel, endpoint);
                assertThrows(RuntimeException.class, () -> connection.sendAll(
                        List.of(new Message("one"), new Message("two"), new Message("fail"))));
                assertEquals(3, writes.get());
                assertEquals(0, flushes.count);
                assertNull(channel.readOutbound());
                assertEquals(3, allocator.allocated.size());
                allocator.allocated.forEach(buffer -> assertEquals(0, buffer.refCnt()));
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void broadcastSerializesOnceAndReadersAreIndependent() {
        AtomicInteger writes = new AtomicInteger();
        try (TestEndpoint endpoint = new TestEndpoint(true, serializers(writes), List.of(Message.class))) {
            EmbeddedChannel first = new EmbeddedChannel();
            EmbeddedChannel second = new EmbeddedChannel();
            EmbeddedChannel closed = new EmbeddedChannel();
            try {
                closed.close();
                NettyConnection.broadcast(List.of(new NettyConnection(first, endpoint),
                        new NettyConnection(second, endpoint), new NettyConnection(closed, endpoint)), new Message("hello"));
                assertEquals(1, writes.get());
                ByteBuf a = first.readOutbound();
                ByteBuf b = second.readOutbound();
                try {
                    a.skipBytes(10);
                    assertEquals(new Message("hello"), endpoint.binarySerializer().read(a, Message.class));
                    assertEquals(0, b.readerIndex());
                    a.release();
                    a = null;
                    b.skipBytes(10);
                    assertEquals(new Message("hello"), endpoint.binarySerializer().read(b, Message.class));
                    assertFalse(b.isReadable());
                } finally {
                    if (a != null) a.release();
                    b.release();
                }
                assertNull(closed.readOutbound());
                NettyConnection.broadcast(List.of(), new Message("hello"));
                assertEquals(1, writes.get());
            } finally {
                first.finishAndReleaseAll();
                second.finishAndReleaseAll();
                closed.finishAndReleaseAll();
            }
        }
    }

    @Test
    void oversizedOutboundFrameFailsLocallyAndReleasesBuffer() {
        record Payload(byte[] bytes) implements Packet { }
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Payload.class))) {
            EmbeddedChannel channel = new EmbeddedChannel();
            TrackingAllocator allocator = new TrackingAllocator();
            channel.config().setAllocator(allocator);
            try {
                NettyConnection connection = new NettyConnection(channel, endpoint);
                assertThrows(RuntimeException.class, () -> connection.send(new Payload(new byte[ProtocolWriter.MAX_FRAME_SIZE])));
                assertNull(channel.readOutbound());
                allocator.allocated.forEach(buffer -> assertEquals(0, buffer.refCnt()));
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    private static SerializerRegistry serializers(AtomicInteger writes) {
        return new SerializerRegistry().register(Message.class, new TypeSerializer<>() {
            @Override public void write(SerializationWriter writer, Message value) {
                writes.incrementAndGet();
                writer.writeString(value.text());
                if (value.text().equals("fail")) throw new IllegalArgumentException("test serialization failure");
            }
            @Override public Message read(SerializationReader reader) { return new Message(reader.readString()); }
        });
    }

    private static final class FlushCounter extends ChannelOutboundHandlerAdapter {
        int count;
        @Override public void flush(ChannelHandlerContext ctx) throws Exception {
            count++;
            super.flush(ctx);
        }
    }

    private static final class TrackingAllocator extends AbstractByteBufAllocator {
        final List<ByteBuf> allocated = new ArrayList<>();
        TrackingAllocator() { super(false); }
        @Override protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
            ByteBuf buffer = Unpooled.buffer(initialCapacity, maxCapacity);
            allocated.add(buffer);
            return buffer;
        }
        @Override protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
            ByteBuf buffer = Unpooled.directBuffer(initialCapacity, maxCapacity);
            allocated.add(buffer);
            return buffer;
        }
        @Override public boolean isDirectBufferPooled() { return false; }
    }
}
