package de.syntaxjason.pulsar.event.connection;

import de.syntaxjason.pulsar.event.Event;

import java.net.SocketAddress;

/** Emitted once when a multiplexed TCP connection has selected an application protocol. */
public record ProtocolSelectedEvent(String protocol, SocketAddress remoteAddress) implements Event {
}
