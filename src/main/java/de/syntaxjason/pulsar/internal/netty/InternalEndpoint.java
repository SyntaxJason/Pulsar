package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.internal.cache.CacheManager;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.packet.PacketRegistry;

public interface InternalEndpoint {

    boolean server();

    EventBus eventBus();

    PacketRegistry packetRegistry();

    BinarySerializer binarySerializer();

    CacheManager cacheManager();

    void connected(NettyConnection connection);

    void disconnected(NettyConnection connection);

    void cacheSubscribe(NettyConnection connection, String cache);

    void cacheMutation(NettyConnection connection, CacheMutation mutation);

    default void exception(NettyConnection connection, Throwable throwable) {
        System.getLogger("Pulsar").log(System.Logger.Level.ERROR,
                "Connection error from " + connection.remoteAddress(), throwable);
    }
}
