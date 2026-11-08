package de.syntaxjason.pulsar.server;

import de.syntaxjason.pulsar.bootstrap.BootstrapReport;
import de.syntaxjason.pulsar.bootstrap.RuntimeState;
import de.syntaxjason.pulsar.cache.NetworkCache;
import de.syntaxjason.pulsar.connection.Connection;
import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.connection.ConnectionCloseEvent;
import de.syntaxjason.pulsar.event.connection.ConnectionOpenEvent;
import de.syntaxjason.pulsar.internal.cache.CacheManager;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.cache.NetworkCacheImpl;
import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import de.syntaxjason.pulsar.internal.netty.InternalEndpoint;
import de.syntaxjason.pulsar.internal.netty.EndpointShutdown;
import de.syntaxjason.pulsar.internal.netty.NettyConnection;
import de.syntaxjason.pulsar.internal.netty.ProtocolException;
import de.syntaxjason.pulsar.internal.netty.PulsarChannelInitializer;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class PulsarServer implements InternalEndpoint, AutoCloseable {

    private final String host;
    private final int port;
    private final SimpleEventBus events;
    private final PacketRegistry packets;
    private final SerializerRegistry serializers;
    private final BinarySerializer binarySerializer;
    private final BootstrapReport bootstrapReport;
    private final PulsarLogger logger;
    private final CacheManager caches;
    private final Map<UUID, NettyConnection> connections = new ConcurrentHashMap<>();
    private final Map<String, Set<NettyConnection>> cacheSubscribers = new ConcurrentHashMap<>();

    private CompletableFuture<Void> closeFuture;

    private volatile RuntimeState state = RuntimeState.FROZEN;
    private volatile EventLoopGroup bossGroup;
    private volatile EventLoopGroup workerGroup;
    private volatile Channel channel;

    PulsarServer(
            String host,
            int port,
            SimpleEventBus events,
            PacketRegistry packets,
            SerializerRegistry serializers,
            BinarySerializer binarySerializer,
            BootstrapReport bootstrapReport,
            PulsarLogger logger
    ) {
        this.host = host;
        this.port = port;
        this.events = events;
        this.packets = packets;
        this.serializers = serializers;
        this.binarySerializer = binarySerializer;
        this.bootstrapReport = bootstrapReport;
        this.logger = logger;
        this.caches = new CacheManager(events, true);

        caches.mutationSink(this::broadcastCacheMutation);
        caches.registrationSink(this::syncRegisteredCache);
    }

    void startInternal() {
        if (channel != null) {
            throw new IllegalStateException("Pulsar server is already started");
        }

        bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        workerGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            channel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new PulsarChannelInitializer(this))
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .bind(new InetSocketAddress(host, port))
                    .syncUninterruptibly()
                    .channel();
            synchronized (this) {
                if (state != RuntimeState.CLOSED) state = RuntimeState.RUNNING;
            }
            logger.info(LogCategory.NETWORK, "Server listening on " + host + ":" + port());
        } catch (RuntimeException exception) {
            close();
            throw exception;
        }
    }

    public EventBus events() {
        return events;
    }

    public PacketRegistry packets() {
        return packets;
    }

    public SerializerRegistry serializers() {
        return serializers;
    }

    public <K, V> NetworkCache<K, V> cache(String name, Class<K> keyType, Class<V> valueType) {
        ensureCompiled(keyType);
        ensureCompiled(valueType);
        return caches.cache(name, keyType, valueType);
    }

    public BootstrapReport bootstrapReport() {
        return bootstrapReport;
    }

    public RuntimeState state() {
        return state;
    }

    public Collection<Connection> connections() {
        return List.copyOf(connections.values());
    }

    public void broadcast(Packet packet) {
        NettyConnection.broadcast(connections.values(), packet);
    }

    public boolean running() {
        Channel current = channel;
        return current != null && current.isActive();
    }

    public int port() {
        Channel current = channel;
        if (current != null && current.localAddress() instanceof InetSocketAddress address) {
            return address.getPort();
        }
        return port;
    }

    @Override
    public boolean server() {
        return true;
    }

    @Override
    public EventBus eventBus() {
        return events;
    }

    @Override
    public PacketRegistry packetRegistry() {
        return packets;
    }

    @Override
    public BinarySerializer binarySerializer() {
        return binarySerializer;
    }

    @Override
    public CacheManager cacheManager() {
        return caches;
    }

    @Override
    public PulsarLogger logger() {
        return logger;
    }

    @Override
    public synchronized void connected(NettyConnection connection) {
        if (state == RuntimeState.CLOSED) {
            connection.close();
            return;
        }
        connections.put(connection.id(), connection);
        events.fire(new ConnectionOpenEvent(connection));
    }

    @Override
    public void disconnected(NettyConnection connection) {
        connections.remove(connection.id());
        cacheSubscribers.values().forEach(subscribers -> subscribers.remove(connection));
        events.fire(new ConnectionCloseEvent(connection));
    }

    @Override
    public void cacheSubscribe(NettyConnection connection, String cache) {
        cacheSubscribers.computeIfAbsent(cache, ignored -> ConcurrentHashMap.newKeySet()).add(connection);
        NetworkCacheImpl<?, ?> networkCache = caches.raw(cache);
        if (networkCache != null) {
            connection.sendSnapshot(networkCache);
        }
    }

    @Override
    public void cacheMutation(NettyConnection connection, CacheMutation mutation) {
        if (!caches.applyRemote(mutation)) {
            throw new ProtocolException("Mutation targets unknown server cache '" + mutation.cache() + "'");
        }
        broadcastCacheMutation(mutation);
    }

    /**
     * Starts shutdown and completes after the acceptor and worker event loops terminate.
     * Safe in event callbacks; repeated calls observe the same shutdown operation.
     */
    public synchronized CompletableFuture<Void> closeAsync() {
        if (closeFuture != null) return closeFuture.copy();
        closeFuture = new CompletableFuture<>();
        state = RuntimeState.CLOSED;

        Channel current = channel;
        channel = null;
        if (current != null) current.close();
        connections.values().forEach(NettyConnection::close);
        connections.clear();
        cacheSubscribers.clear();
        caches.close();

        CompletableFuture.allOf(
                EndpointShutdown.shutdown(workerGroup),
                EndpointShutdown.shutdown(bossGroup)
        ).whenComplete((ignored, failure) -> {
            if (failure == null) closeFuture.complete(null);
            else closeFuture.completeExceptionally(failure);
        });
        return closeFuture.copy();
    }

    /** Blocks for termination unless called from one of this server's event loops. */
    @Override
    public void close() {
        boolean onEventLoop = EndpointShutdown.inEventLoop(workerGroup)
                || EndpointShutdown.inEventLoop(bossGroup);
        CompletableFuture<Void> shutdown = closeAsync();
        if (!onEventLoop) shutdown.join();
    }

    private void ensureCompiled(Class<?> type) {
        if (binarySerializer.supports(type)) {
            return;
        }
        throw new IllegalStateException(
                "Cache type " + type.getName()
                        + " was not registered during bootstrap. Add .serializable("
                        + type.getSimpleName() + ".class) before start()."
        );
    }

    private void broadcastCacheMutation(CacheMutation mutation) {
        Set<NettyConnection> subscribers = cacheSubscribers.get(mutation.cache());
        if (subscribers == null) {
            return;
        }

        subscribers.forEach(connection -> {
            if (connection.open()) {
                connection.sendCacheMutation(mutation);
            }
        });
    }

    private void syncRegisteredCache(String name) {
        NetworkCacheImpl<?, ?> cache = caches.raw(name);
        Set<NettyConnection> subscribers = cacheSubscribers.get(name);
        if (cache == null || subscribers == null) {
            return;
        }
        subscribers.forEach(connection -> connection.sendSnapshot(cache));
    }
}
