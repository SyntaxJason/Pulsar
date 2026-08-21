package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.internal.cache.CacheManager;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.PacketRegistry;

public interface InternalEndpoint {

    boolean server();

    EventBus eventBus();

    PacketRegistry packetRegistry();

    BinarySerializer binarySerializer();

    CacheManager cacheManager();

    PulsarLogger logger();

    void connected(NettyConnection connection);

    void disconnected(NettyConnection connection);

    void cacheSubscribe(NettyConnection connection, String cache);

    void cacheMutation(NettyConnection connection, CacheMutation mutation);

    default void exception(NettyConnection connection, Throwable throwable) {
        logger().error(
                LogCategory.CONNECTION,
                "Connection error from " + connection.remoteAddress(),
                throwable
        );
    }
}
