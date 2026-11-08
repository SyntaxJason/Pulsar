package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.cache.CacheSnapshotEntry;
import de.syntaxjason.pulsar.internal.cache.NetworkCacheImpl;
import de.syntaxjason.pulsar.packet.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

final class ProtocolWriter {

    static final int MAGIC = 0x50554C53;
    static final int VERSION = 2;
    // The decoder's limit includes the four-byte length prefix.
    static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;
    static final int MAX_PAYLOAD_SIZE = MAX_FRAME_SIZE - Integer.BYTES;

    private ProtocolWriter() {
    }

    static void packet(NettyConnection connection, Packet packet) {
        connection.channel().writeAndFlush(encodePacket(connection, packet));
    }

    @SuppressWarnings("unchecked")
    static ByteBuf encodePacket(NettyConnection connection, Packet packet) {
        Objects.requireNonNull(packet, "packet");
        Class<? extends Packet> type = (Class<? extends Packet>) packet.getClass();
        int wireId = connection.endpoint().packetRegistry().wireId(type);
        ByteBuf buffer = frame(connection.channel(), FrameType.PACKET);
        try {
            buffer.writeInt(wireId);
            connection.endpoint().binarySerializer().write(buffer, type, packet);
            return buffer;
        } catch (Throwable failure) {
            buffer.release();
            throw failure;
        }
    }

    static void subscribe(NettyConnection connection, String cache) {
        writeFrame(connection, FrameType.CACHE_SUBSCRIBE, buffer ->
                connection.endpoint().binarySerializer().write(buffer, String.class, cache));
    }

    static void snapshot(NettyConnection connection, NetworkCacheImpl<?, ?> cache) {
        List<CacheSnapshotEntry> entries = cache.snapshotEntries();
        if (entries.size() > 1_000_000) {
            throw new IllegalArgumentException("Cache snapshot is too large: " + entries.size());
        }
        writeFrame(connection, FrameType.CACHE_SNAPSHOT, buffer -> {
            connection.endpoint().binarySerializer().write(buffer, String.class, cache.name());
            buffer.writeInt(entries.size());
            for (CacheSnapshotEntry entry : entries) {
                buffer.writeLong(entry.ttlMillis());
                connection.endpoint().binarySerializer().write(buffer, cache.keyType(), entry.key());
                connection.endpoint().binarySerializer().write(buffer, cache.valueType(), entry.value());
            }
        });
    }

    static void mutation(NettyConnection connection, CacheMutation mutation) {
        NetworkCacheImpl<?, ?> cache = connection.endpoint().cacheManager().raw(mutation.cache());
        if (cache == null) {
            throw new IllegalStateException("Unknown cache: " + mutation.cache());
        }
        if (mutation instanceof CacheMutation.Put put) {
            writeFrame(connection, FrameType.CACHE_PUT, buffer -> {
                connection.endpoint().binarySerializer().write(buffer, String.class, put.cache());
                buffer.writeLong(put.ttlMillis());
                connection.endpoint().binarySerializer().write(buffer, cache.keyType(), put.key());
                connection.endpoint().binarySerializer().write(buffer, cache.valueType(), put.value());
            });
        } else if (mutation instanceof CacheMutation.Remove remove) {
            writeFrame(connection, FrameType.CACHE_REMOVE, buffer -> {
                connection.endpoint().binarySerializer().write(buffer, String.class, remove.cache());
                connection.endpoint().binarySerializer().write(buffer, cache.keyType(), remove.key());
            });
        } else if (mutation instanceof CacheMutation.Clear clear) {
            writeFrame(connection, FrameType.CACHE_CLEAR, buffer ->
                    connection.endpoint().binarySerializer().write(buffer, String.class, clear.cache()));
        }
    }

    private static void writeFrame(NettyConnection connection, FrameType type, Consumer<ByteBuf> encoder) {
        ByteBuf buffer = frame(connection.channel(), type);
        try {
            encoder.accept(buffer);
        } catch (Throwable failure) {
            buffer.release();
            throw failure;
        }
        connection.channel().writeAndFlush(buffer);
    }

    private static ByteBuf frame(Channel channel, FrameType type) {
        ByteBuf buffer = channel.alloc().buffer(256, MAX_PAYLOAD_SIZE);
        buffer.writeInt(MAGIC);
        buffer.writeByte(VERSION);
        buffer.writeByte(type.id());
        return buffer;
    }
}
