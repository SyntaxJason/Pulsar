package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.internal.bootstrap.BootstrapArtifacts;
import de.syntaxjason.pulsar.internal.bootstrap.PulsarBootstrap;
import de.syntaxjason.pulsar.internal.cache.CacheManager;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.logging.NoOpPulsarLogger;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;

import java.util.ArrayList;
import java.util.List;

final class TestEndpoint implements InternalEndpoint, AutoCloseable {
    final PacketRegistry packets = new PacketRegistry();
    final CacheManager caches;
    final BootstrapArtifacts bootstrap;
    final List<Throwable> failures = new ArrayList<>();
    NettyConnection connection;
    int subscriptions;
    int mutations;
    final boolean server;

    TestEndpoint(boolean server, SerializerRegistry serializers,
                 List<Class<? extends Packet>> types, Listener... listeners) {
        this.server = server;
        types.forEach(packets::register);
        bootstrap = PulsarBootstrap.prepare(packets, serializers, List.of(listeners), List.of(),
                NoOpPulsarLogger.INSTANCE);
        caches = new CacheManager(bootstrap.events(), server);
    }

    @Override public boolean server() { return server; }
    @Override public EventBus eventBus() { return bootstrap.events(); }
    @Override public PacketRegistry packetRegistry() { return packets; }
    @Override public BinarySerializer binarySerializer() { return bootstrap.serializer(); }
    @Override public CacheManager cacheManager() { return caches; }
    @Override public PulsarLogger logger() { return NoOpPulsarLogger.INSTANCE; }
    @Override public void connected(NettyConnection connection) { this.connection = connection; }
    @Override public void disconnected(NettyConnection connection) { }
    @Override public void cacheSubscribe(NettyConnection connection, String cache) { subscriptions++; }
    @Override public void cacheMutation(NettyConnection connection, CacheMutation mutation) { mutations++; }
    @Override public void exception(NettyConnection connection, Throwable throwable) { failures.add(throwable); }
    @Override public void close() { caches.close(); }
}
