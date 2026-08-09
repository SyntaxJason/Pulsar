package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.cache.CacheSnapshotEntry;
import de.syntaxjason.pulsar.internal.cache.NetworkCacheImpl;
import de.syntaxjason.pulsar.packet.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.List;

final class ProtocolWriter {

    static final int MAGIC = 0x50554C53;
    static final int VERSION = 1;
    static final int MAX_FRAME_SIZE = 16 * 1024 * 1024;

    private ProtocolWriter() {
    }

    @SuppressWarnings("unchecked")
    static void packet(NettyConnection connection, Packet packet) {
        Class<? extends Packet> type = (Class<? extends Packet>) packet.getClass();
        String id = connection.endpoint().packetRegistry().id(type);

        ByteBuf buffer = frame(connection.channel(), FrameType.PACKET);
        connection.endpoint().binarySerializer().write(buffer, String.class, id);
        connection.endpoint().binarySerializer().write(buffer, type, packet);
        connection.channel().writeAndFlush(buffer);
    }

    static void subscribe(NettyConnection connection, String cache) {
        ByteBuf buffer = frame(connection.channel(), FrameType.CACHE_SUBSCRIBE);
        connection.endpoint().binarySerializer().write(buffer, String.class, cache);
        connection.channel().writeAndFlush(buffer);
    }

    static void snapshot(NettyConnection connection, NetworkCacheImpl<?, ?> cache) {
        List<CacheSnapshotEntry> entries = cache.snapshotEntries();
        ByteBuf buffer = frame(connection.channel(), FrameType.CACHE_SNAPSHOT);
        connection.endpoint().binarySerializer().write(buffer, String.class, cache.name());
        buffer.writeInt(entries.size());

        for (CacheSnapshotEntry entry : entries) {
            buffer.writeLong(entry.ttlMillis());
            connection.endpoint().binarySerializer().write(buffer, cache.keyType(), entry.key());
            connection.endpoint().binarySerializer().write(buffer, cache.valueType(), entry.value());
        }

        connection.channel().writeAndFlush(buffer);
    }

    static void mutation(NettyConnection connection, CacheMutation mutation) {
        NetworkCacheImpl<?, ?> cache = connection.endpoint().cacheManager().raw(mutation.cache());
        if (cache == null && !(mutation instanceof CacheMutation.Clear)) {
            throw new IllegalStateException("Unknown cache: " + mutation.cache());
        }

        if (mutation instanceof CacheMutation.Put put) {
            ByteBuf buffer = frame(connection.channel(), FrameType.CACHE_PUT);
            connection.endpoint().binarySerializer().write(buffer, String.class, put.cache());
            buffer.writeLong(put.ttlMillis());
            connection.endpoint().binarySerializer().write(buffer, cache.keyType(), put.key());
            connection.endpoint().binarySerializer().write(buffer, cache.valueType(), put.value());
            connection.channel().writeAndFlush(buffer);
            return;
        }

        if (mutation instanceof CacheMutation.Remove remove) {
            ByteBuf buffer = frame(connection.channel(), FrameType.CACHE_REMOVE);
            connection.endpoint().binarySerializer().write(buffer, String.class, remove.cache());
            connection.endpoint().binarySerializer().write(buffer, cache.keyType(), remove.key());
            connection.channel().writeAndFlush(buffer);
            return;
        }

        if (mutation instanceof CacheMutation.Clear clear) {
            ByteBuf buffer = frame(connection.channel(), FrameType.CACHE_CLEAR);
            connection.endpoint().binarySerializer().write(buffer, String.class, clear.cache());
            connection.channel().writeAndFlush(buffer);
        }
    }

    private static ByteBuf frame(Channel channel, FrameType type) {
        ByteBuf buffer = channel.alloc().buffer();
        buffer.writeInt(MAGIC);
        buffer.writeByte(VERSION);
        buffer.writeByte(type.id());
        return buffer;
    }
}
