package de.syntaxjason.pulsar.server;

import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class PulsarServerBuilder {

    private String host = "0.0.0.0";
    private int port = 8080;
    private final PacketRegistry packets = new PacketRegistry();
    private final SerializerRegistry serializers = new SerializerRegistry();
    private final List<Listener> listeners = new ArrayList<>();

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

    public <T> PulsarServerBuilder serializer(Class<T> type, TypeSerializer<T> serializer) {
        serializers.register(type, serializer);
        return this;
    }

    public PulsarServer start() {
        SimpleEventBus events = new SimpleEventBus();
        listeners.forEach(events::register);

        PulsarServer server = new PulsarServer(host, port, events, packets, serializers);
        server.startInternal();
        return server;
    }

    public CompletableFuture<PulsarServer> startAsync() {
        return CompletableFuture.supplyAsync(this::start);
    }
}
