package de.syntaxjason.pulsar.client;

import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.internal.bootstrap.BootstrapArtifacts;
import de.syntaxjason.pulsar.internal.bootstrap.PulsarBootstrap;
import de.syntaxjason.pulsar.logging.ConsolePulsarLogger;
import de.syntaxjason.pulsar.logging.LogLevel;
import de.syntaxjason.pulsar.logging.NoOpPulsarLogger;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class PulsarClientBuilder {

    private String host = "127.0.0.1";
    private int port = 8080;
    private PulsarLogger logger = new ConsolePulsarLogger();
    private final PacketRegistry packets = new PacketRegistry(logger);
    private final SerializerRegistry serializers = new SerializerRegistry();
    private final List<Listener> listeners = new ArrayList<>();
    private final Set<Type> serializableTypes = new LinkedHashSet<>();

    public PulsarClientBuilder host(String host) {
        this.host = host;
        return this;
    }

    public PulsarClientBuilder port(int port) {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        this.port = port;
        return this;
    }

    public PulsarClientBuilder address(String host, int port) {
        return host(host).port(port);
    }

    public PulsarClientBuilder packet(Class<? extends Packet> packet) {
        packets.register(packet);
        return this;
    }

    @SafeVarargs
    public final PulsarClientBuilder packets(Class<? extends Packet>... packets) {
        this.packets.register(packets);
        return this;
    }

    public PulsarClientBuilder listener(Listener listener) {
        listeners.add(listener);
        return this;
    }

    public PulsarClientBuilder serializable(Type type) {
        serializableTypes.add(type);
        return this;
    }

    public PulsarClientBuilder serializable(Class<?> type) {
        return serializable((Type) type);
    }

    @SafeVarargs
    public final PulsarClientBuilder serializables(Class<?>... types) {
        for (Class<?> type : types) {
            serializable(type);
        }
        return this;
    }

    public <T> PulsarClientBuilder serializer(Class<T> type, TypeSerializer<T> serializer) {
        serializers.register(type, serializer);
        return this;
    }

    public PulsarClientBuilder logger(PulsarLogger logger) {
        this.logger = logger == null ? NoOpPulsarLogger.INSTANCE : logger;
        packets.logger(this.logger);
        return this;
    }

    public PulsarClientBuilder logging(boolean enabled) {
        return logger(enabled ? new ConsolePulsarLogger() : NoOpPulsarLogger.INSTANCE);
    }

    public PulsarClientBuilder logLevel(LogLevel level) {
        if (logger instanceof ConsolePulsarLogger console) {
            console.level(level);
            return this;
        }
        return logger(new ConsolePulsarLogger(level));
    }

    public PulsarClient connect() {
        BootstrapArtifacts bootstrap = PulsarBootstrap.prepare(
                packets,
                serializers,
                listeners,
                serializableTypes,
                logger
        );

        PulsarClient client = new PulsarClient(
                host,
                port,
                bootstrap.events(),
                packets,
                serializers,
                bootstrap.serializer(),
                bootstrap.report(),
                logger
        );
        client.connectInternal();
        return client;
    }

    public CompletableFuture<PulsarClient> connectAsync() {
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("pulsar-connect-", 0).factory()
        );
        CompletableFuture<PulsarClient> future = CompletableFuture.supplyAsync(this::connect, executor);
        future.whenComplete((ignored, throwable) -> executor.shutdown());
        return future;
    }
}
