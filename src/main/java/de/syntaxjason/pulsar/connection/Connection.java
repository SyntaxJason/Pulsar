package de.syntaxjason.pulsar.connection;

import de.syntaxjason.pulsar.packet.Packet;

import java.net.SocketAddress;
import java.util.UUID;

public interface Connection {

    UUID id();

    SocketAddress remoteAddress();

    boolean open();

    void send(Packet packet);

    void close();
}
