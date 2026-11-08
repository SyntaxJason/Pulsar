package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolFramingTest {
    record Message(String text) implements Packet { }

    @Test
    void realPipelineHandlesOneByteFragmentsAndCoalescedFrames() {
        List<String> received = new ArrayList<>();
        Listener listener = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Message> event) { received.add(event.packet().text()); }
        };
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Message.class), listener)) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint));
            ByteBuf wire = wire(endpoint, "fragmented");
            try {
                while (wire.isReadable()) channel.writeInbound(wire.readRetainedSlice(1));
                assertEquals(List.of("fragmented"), received);
                ByteBuf coalesced = wire(endpoint, "two");
                ByteBuf third = wire(endpoint, "three");
                try { coalesced.writeBytes(third); } finally { third.release(); }
                channel.writeInbound(coalesced);
                assertEquals(List.of("fragmented", "two", "three"), received);
                assertTrue(channel.isActive());
                assertTrue(endpoint.failures.isEmpty());
            } finally { wire.release(); channel.finishAndReleaseAll(); }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, Integer.MAX_VALUE, ProtocolWriter.MAX_FRAME_SIZE})
    void invalidOrOversizedLengthPrefixClosesConnection(int length) {
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of())) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint));
            try {
                channel.writeInbound(Unpooled.buffer().writeInt(length));
                assertFalse(channel.isActive());
                assertEquals(1, endpoint.failures.size());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    @Test
    void outboundLimitIncludesTheLengthPrefix() {
        record Payload(byte[] data) implements Packet { }
        try (TestEndpoint endpoint = new TestEndpoint(true, new SerializerRegistry(), List.of(Payload.class))) {
            EmbeddedChannel channel = new EmbeddedChannel(new PulsarChannelInitializer(endpoint));
            try {
                // Header 6 + packet ID 4 + record presence 1 + byte[] presence 1 + array length 4.
                endpoint.connection.send(new Payload(new byte[ProtocolWriter.MAX_PAYLOAD_SIZE - 16]));
                ByteBuf prefix = channel.readOutbound();
                ByteBuf payload = channel.readOutbound();
                try {
                    assertEquals(ProtocolWriter.MAX_PAYLOAD_SIZE, prefix.readInt());
                    assertEquals(ProtocolWriter.MAX_PAYLOAD_SIZE, payload.readableBytes());
                } finally { prefix.release(); payload.release(); }
                assertThrows(RuntimeException.class,
                        () -> endpoint.connection.send(new Payload(new byte[ProtocolWriter.MAX_PAYLOAD_SIZE - 15])));
                assertNull(channel.readOutbound());
            } finally { channel.finishAndReleaseAll(); }
        }
    }

    private static ByteBuf wire(TestEndpoint endpoint, String text) {
        ByteBuf payload = ProtocolWriter.encodePacket(endpoint.connection, new Message(text));
        try { return Unpooled.buffer().writeInt(payload.readableBytes()).writeBytes(payload); }
        finally { payload.release(); }
    }
}
