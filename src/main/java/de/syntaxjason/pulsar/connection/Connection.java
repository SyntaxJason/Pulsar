package de.syntaxjason.pulsar.connection;

import de.syntaxjason.pulsar.packet.Packet;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.Objects;

public interface Connection {

    UUID id();

    SocketAddress remoteAddress();

    boolean open();

    void send(Packet packet);

    /**
     * Sends a batch in iteration order. Pulsar's Netty implementation serializes the entire
     * batch before enqueueing any packet, then writes it with one flush on the event loop.
     * Keep batches bounded to limit temporary memory and event-loop work.
     */
    default void sendAll(Iterable<? extends Packet> packets) {
        Objects.requireNonNull(packets, "packets");
        for (Packet packet : packets) send(packet);
    }

    void close();
}
