package de.syntaxjason.pulsar.client;

import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class PulsarClientBuilder {

    private String host = "127.0.0.1";
    private int port = 8080;
    private final PacketRegistry packets = new PacketRegistry();
    private final SerializerRegistry serializers = new SerializerRegistry();
    private final List<Listener> listeners = new ArrayList<>();

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

    public <T> PulsarClientBuilder serializer(Class<T> type, TypeSerializer<T> serializer) {
        serializers.register(type, serializer);
        return this;
    }

    public PulsarClient connect() {
        SimpleEventBus events = new SimpleEventBus();
        listeners.forEach(events::register);

        PulsarClient client = new PulsarClient(host, port, events, packets, serializers);
        client.connectInternal();
        return client;
    }

    public CompletableFuture<PulsarClient> connectAsync() {
        return CompletableFuture.supplyAsync(this::connect);
    }
}
