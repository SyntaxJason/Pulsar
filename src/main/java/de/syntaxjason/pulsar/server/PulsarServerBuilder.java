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
import de.syntaxjason.pulsar.protocol.ApplicationProtocol;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
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
    private final List<ApplicationProtocol> applicationProtocols = new ArrayList<>();
    private int maxProtocolProbeBytes = 64;
    private long protocolDetectionTimeoutMillis = 5000;

    /** Registers an additional, client-first TCP application protocol on this server's port. */
    public PulsarServerBuilder protocol(ApplicationProtocol protocol) {
        Objects.requireNonNull(protocol, "protocol");
        String name = protocol.name();
        if (name == null || !name.matches("[a-z][a-z0-9._-]{0,63}") || name.equals("pulsar")) {
            throw new IllegalArgumentException("Protocol name must match [a-z][a-z0-9._-]{0,63} and not be 'pulsar'");
        }
        if (applicationProtocols.stream().anyMatch(existing -> existing.name().equals(name))) {
            throw new IllegalArgumentException("Duplicate application protocol: " + name);
        }
        if (applicationProtocols.size() == 32) throw new IllegalArgumentException("At most 32 additional protocols are supported");
        applicationProtocols.add(protocol);
        return this;
    }

    /** Maximum inspected prefix, independent of the protocol's actual message size limit. */
    public PulsarServerBuilder maxProtocolProbeBytes(int bytes) {
        if (bytes < 8 || bytes > 65_536) throw new IllegalArgumentException("Probe limit must be 8..65536 bytes");
        maxProtocolProbeBytes = bytes;
        return this;
    }

    public PulsarServerBuilder protocolDetectionTimeout(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long millis;
        try { millis = timeout.toMillis(); }
        catch (ArithmeticException failure) { throw new IllegalArgumentException("Invalid protocol detection timeout", failure); }
        if (millis < 1 || millis > 60_000) throw new IllegalArgumentException("Detection timeout must be 1..60000 ms");
        protocolDetectionTimeoutMillis = millis;
        return this;
    }

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
        server.configureProtocols(applicationProtocols, maxProtocolProbeBytes, protocolDetectionTimeoutMillis);
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
