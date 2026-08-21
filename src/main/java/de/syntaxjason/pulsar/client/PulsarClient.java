package de.syntaxjason.pulsar.client;

import de.syntaxjason.pulsar.bootstrap.BootstrapReport;
import de.syntaxjason.pulsar.bootstrap.RuntimeState;
import de.syntaxjason.pulsar.cache.NetworkCache;
import de.syntaxjason.pulsar.connection.Connection;
import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.connection.ConnectionCloseEvent;
import de.syntaxjason.pulsar.event.connection.ConnectionOpenEvent;
import de.syntaxjason.pulsar.internal.cache.CacheManager;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import de.syntaxjason.pulsar.internal.netty.InternalEndpoint;
import de.syntaxjason.pulsar.internal.netty.NettyConnection;
import de.syntaxjason.pulsar.internal.netty.ProtocolException;
import de.syntaxjason.pulsar.internal.netty.PulsarChannelInitializer;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

public final class PulsarClient implements InternalEndpoint, AutoCloseable {

    private final String host;
    private final int port;
    private final SimpleEventBus events;
    private final PacketRegistry packets;
    private final SerializerRegistry serializers;
    private final BinarySerializer binarySerializer;
    private final BootstrapReport bootstrapReport;
    private final PulsarLogger logger;
    private final CacheManager caches;

    private volatile RuntimeState state = RuntimeState.FROZEN;
    private volatile EventLoopGroup eventLoopGroup;
    private volatile NettyConnection connection;

    PulsarClient(
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
        this.caches = new CacheManager(events, false);

        caches.mutationSink(this::sendCacheMutation);
        caches.subscribeSink(this::subscribeCache);
    }

    void connectInternal() {
        if (eventLoopGroup != null) {
            throw new IllegalStateException("Pulsar client is already connected or connecting");
        }

        eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        try {
            new Bootstrap()
                    .group(eventLoopGroup)
                    .channel(NioSocketChannel.class)
                    .handler(new PulsarChannelInitializer(this))
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .connect(new InetSocketAddress(host, port))
                    .syncUninterruptibly();
            state = RuntimeState.RUNNING;
            logger.info(LogCategory.NETWORK, "Connected to " + host + ":" + port);
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

    public Connection connection() {
        return connection;
    }

    public void send(Packet packet) {
        NettyConnection current = requireConnection();
        current.send(packet);
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

    public boolean connected() {
        NettyConnection current = connection;
        return current != null && current.open();
    }

    @Override
    public boolean server() {
        return false;
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
    public void connected(NettyConnection connection) {
        this.connection = connection;
        caches.connected();
        events.fire(new ConnectionOpenEvent(connection));
    }

    @Override
    public void disconnected(NettyConnection connection) {
        if (this.connection == connection) {
            this.connection = null;
        }
        caches.disconnected();
        events.fire(new ConnectionCloseEvent(connection));
    }

    @Override
    public void cacheSubscribe(NettyConnection connection, String cache) {
        throw new ProtocolException("Client cannot receive cache subscriptions");
    }

    @Override
    public void cacheMutation(NettyConnection connection, CacheMutation mutation) {
        if (!caches.applyRemote(mutation)) {
            throw new ProtocolException("Mutation targets unknown client cache '" + mutation.cache() + "'");
        }
    }

    @Override
    public void close() {
        NettyConnection current = connection;
        connection = null;
        if (current != null) {
            current.close();
        }

        caches.disconnected();
        caches.close();

        EventLoopGroup group = eventLoopGroup;
        eventLoopGroup = null;
        if (group != null) {
            group.shutdownGracefully().syncUninterruptibly();
        }
        state = RuntimeState.CLOSED;
    }

    private void ensureCompiled(Class<?> type) {
        if (binarySerializer.supports(type)) {
            return;
        }
        throw new IllegalStateException(
                "Cache type " + type.getName()
                        + " was not registered during bootstrap. Add .serializable("
                        + type.getSimpleName() + ".class) before connect()."
        );
    }

    private void sendCacheMutation(CacheMutation mutation) {
        NettyConnection current = connection;
        if (current == null || !current.open()) {
            return;
        }
        current.sendCacheMutation(mutation);
    }

    private void subscribeCache(String cache) {
        NettyConnection current = connection;
        if (current == null || !current.open()) {
            return;
        }
        current.subscribe(cache);
    }

    private NettyConnection requireConnection() {
        NettyConnection current = connection;
        if (current == null || !current.open()) {
            throw new IllegalStateException("Pulsar client is not connected");
        }
        return current;
    }
}
