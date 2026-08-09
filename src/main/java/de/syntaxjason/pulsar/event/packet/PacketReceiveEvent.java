package de.syntaxjason.pulsar.event.packet;

import de.syntaxjason.pulsar.connection.Connection;
import de.syntaxjason.pulsar.event.Cancellable;
import de.syntaxjason.pulsar.event.Event;
import de.syntaxjason.pulsar.packet.Packet;

public final class PacketReceiveEvent<T extends Packet> implements Event, Cancellable {

    private final Connection connection;
    private final T packet;
    private boolean cancelled;

    public PacketReceiveEvent(Connection connection, T packet) {
        this.connection = connection;
        this.packet = packet;
    }

    public Connection connection() {
        return connection;
    }

    public T packet() {
        return packet;
    }

    @Override
    public boolean cancelled() {
        return cancelled;
    }

    @Override
    public void cancelled(boolean cancelled) {
        this.cancelled = cancelled;
    }
}
