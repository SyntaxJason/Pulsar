package de.syntaxjason.pulsar.server;

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

public final class PulsarServerBuilder {

    private String host = "0.0.0.0";
    private int port = 8080;
    private PulsarLogger logger = new ConsolePulsarLogger();
    private final PacketRegistry packets = new PacketRegistry(logger);
    private final SerializerRegistry serializers = new SerializerRegistry();
    private final List<Listener> listeners = new ArrayList<>();
    private final Set<Type> serializableTypes = new LinkedHashSet<>();

    public PulsarServerBuilder host(String host) {
        this.host = host;
        return this;
    }

    public PulsarServerBuilder port(int port) {
        if (port < 0 || port > 65_535) {
            throw new IllegalArgumentException("Invalid port: " + port);
        }
        this.port = port;
        return this;
    }

    public PulsarServerBuilder packet(Class<? extends Packet> packet) {
        packets.register(packet);
        return this;
    }

    @SafeVarargs
    public final PulsarServerBuilder packets(Class<? extends Packet>... packets) {
        this.packets.register(packets);
        return this;
    }

    public PulsarServerBuilder listener(Listener listener) {
        listeners.add(listener);
        return this;
    }

    public PulsarServerBuilder serializable(Type type) {
        serializableTypes.add(type);
        return this;
    }

    public PulsarServerBuilder serializable(Class<?> type) {
        return serializable((Type) type);
    }

    @SafeVarargs
    public final PulsarServerBuilder serializables(Class<?>... types) {
        for (Class<?> type : types) {
            serializable(type);
        }
        return this;
    }

    public <T> PulsarServerBuilder serializer(Class<T> type, TypeSerializer<T> serializer) {
        serializers.register(type, serializer);
        return this;
    }

    public PulsarServerBuilder logger(PulsarLogger logger) {
        this.logger = logger == null ? NoOpPulsarLogger.INSTANCE : logger;
        packets.logger(this.logger);
        return this;
    }

    public PulsarServerBuilder logging(boolean enabled) {
        return logger(enabled ? new ConsolePulsarLogger() : NoOpPulsarLogger.INSTANCE);
    }

    public PulsarServerBuilder logLevel(LogLevel level) {
        if (logger instanceof ConsolePulsarLogger console) {
            console.level(level);
            return this;
        }
        return logger(new ConsolePulsarLogger(level));
    }

    public PulsarServer start() {
        BootstrapArtifacts bootstrap = PulsarBootstrap.prepare(
                packets,
                serializers,
                listeners,
                serializableTypes,
                logger
        );

        PulsarServer server = new PulsarServer(
                host,
                port,
                bootstrap.events(),
                packets,
                serializers,
                bootstrap.serializer(),
                bootstrap.report(),
                logger
        );
        server.startInternal();
        return server;
    }

    public CompletableFuture<PulsarServer> startAsync() {
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("pulsar-start-", 0).factory()
        );
        CompletableFuture<PulsarServer> future = CompletableFuture.supplyAsync(this::start, executor);
        future.whenComplete((ignored, throwable) -> executor.shutdown());
        return future;
    }
}
