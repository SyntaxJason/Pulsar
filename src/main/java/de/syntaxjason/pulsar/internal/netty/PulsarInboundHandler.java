package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.cache.CacheSnapshotEntry;
import de.syntaxjason.pulsar.internal.cache.NetworkCacheImpl;
import de.syntaxjason.pulsar.packet.Packet;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.ArrayList;
import java.util.List;

final class PulsarInboundHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final InternalEndpoint endpoint;
    private NettyConnection connection;

    PulsarInboundHandler(InternalEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        connection = new NettyConnection(ctx.channel(), endpoint);
        endpoint.connected(connection);
        ctx.fireChannelActive();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buffer) {
        validateHeader(buffer);
        FrameType type = FrameType.byId(buffer.readUnsignedByte());

        switch (type) {
            case PACKET -> readPacket(buffer);
            case CACHE_SUBSCRIBE -> readSubscribe(buffer);
            case CACHE_SNAPSHOT -> readSnapshot(buffer);
            case CACHE_PUT -> readCachePut(buffer);
            case CACHE_REMOVE -> readCacheRemove(buffer);
            case CACHE_CLEAR -> readCacheClear(buffer);
        }

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (connection != null) {
            endpoint.disconnected(connection);
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (connection != null) {
            try {
                endpoint.exception(connection, cause);
            } finally {
                ctx.close();
            }
        } else {
            ctx.close();
        }
    }

    private void validateHeader(ByteBuf buffer) {
        if (buffer.readableBytes() < 6) {
            throw new ProtocolException("Frame is too small");
        }

        int magic = buffer.readInt();
        if (magic != ProtocolWriter.MAGIC) {
            throw new ProtocolException("Invalid Pulsar frame magic");
        }

        int version = buffer.readUnsignedByte();
        if (version != ProtocolWriter.VERSION) {
            throw new ProtocolException("Unsupported Pulsar protocol version: " + version);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void readPacket(ByteBuf buffer) {
        int wireId = buffer.readInt();
        Class<? extends Packet> type = endpoint.packetRegistry().type(wireId);
        if (type == null) {
            throw new ProtocolException(
                    "Unknown packet wire id " + Integer.toUnsignedString(wireId)
                            + ". Register the packet on both endpoints before start/connect."
            );
        }

        Packet packet = (Packet) endpoint.binarySerializer().read(buffer, type);
        if (packet == null) {
            throw new ProtocolException("Packet must not be null");
        }
        requireConsumed(buffer);
        endpoint.eventBus().fire(new PacketReceiveEvent(connection, packet));
    }

    private void readSubscribe(ByteBuf buffer) {
        if (!endpoint.server()) {
            throw new ProtocolException("Client endpoint received CACHE_SUBSCRIBE frame");
        }
        String cache = (String) endpoint.binarySerializer().read(buffer, String.class);
        if (cache == null || cache.isBlank() || cache.length() > 128) {
            throw new ProtocolException("Invalid cache subscription name");
        }
        requireConsumed(buffer);
        endpoint.cacheSubscribe(connection, cache);
    }

    private void readSnapshot(ByteBuf buffer) {
        if (endpoint.server()) {
            throw new ProtocolException("Server endpoint received CACHE_SNAPSHOT frame");
        }
        String name = (String) endpoint.binarySerializer().read(buffer, String.class);
        NetworkCacheImpl<?, ?> cache = requireCache(name);

        int size = buffer.readInt();
        if (size < 0 || size > 1_000_000 || size > buffer.readableBytes() / Long.BYTES) {
            throw new ProtocolException("Invalid cache snapshot size: " + size);
        }

        List<CacheSnapshotEntry> entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            long ttl = buffer.readLong();
            Object key = endpoint.binarySerializer().read(buffer, cache.keyType());
            Object value = endpoint.binarySerializer().read(buffer, cache.valueType());
            validateEntry(key, value, ttl);
            entries.add(new CacheSnapshotEntry(key, value, ttl));
        }

        requireConsumed(buffer);
        endpoint.cacheManager().applySnapshot(name, entries);
    }

    private void readCachePut(ByteBuf buffer) {
        String name = (String) endpoint.binarySerializer().read(buffer, String.class);
        long ttl = buffer.readLong();
        NetworkCacheImpl<?, ?> cache = requireCache(name);
        Object key = endpoint.binarySerializer().read(buffer, cache.keyType());
        Object value = endpoint.binarySerializer().read(buffer, cache.valueType());
        validateEntry(key, value, ttl);
        requireConsumed(buffer);
        endpoint.cacheMutation(connection, new CacheMutation.Put(name, key, value, ttl));
    }

    private void readCacheRemove(ByteBuf buffer) {
        String name = (String) endpoint.binarySerializer().read(buffer, String.class);
        NetworkCacheImpl<?, ?> cache = requireCache(name);
        Object key = endpoint.binarySerializer().read(buffer, cache.keyType());
        if (key == null) {
            throw new ProtocolException("Cache key must not be null");
        }
        requireConsumed(buffer);
        endpoint.cacheMutation(connection, new CacheMutation.Remove(name, key));
    }

    private void readCacheClear(ByteBuf buffer) {
        String name = (String) endpoint.binarySerializer().read(buffer, String.class);
        requireCache(name);
        requireConsumed(buffer);
        endpoint.cacheMutation(connection, new CacheMutation.Clear(name));
    }

    private static void requireConsumed(ByteBuf buffer) {
        if (buffer.isReadable()) {
            throw new ProtocolException("Trailing bytes after frame: " + buffer.readableBytes());
        }
    }

    private static void validateEntry(Object key, Object value, long ttl) {
        if (key == null || value == null) {
            throw new ProtocolException("Cache keys and values must not be null");
        }
        if (ttl != -1 && (ttl <= 0 || ttl > Long.MAX_VALUE - System.currentTimeMillis())) {
            throw new ProtocolException("Invalid cache TTL: " + ttl);
        }
    }

    private NetworkCacheImpl<?, ?> requireCache(String name) {
        if (name == null) {
            throw new ProtocolException("Cache name must not be null");
        }
        NetworkCacheImpl<?, ?> cache = endpoint.cacheManager().raw(name);
        if (cache == null) {
            throw new ProtocolException("Received mutation for unknown cache '" + name + "'");
        }
        return cache;
    }
}
