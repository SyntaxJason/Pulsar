package de.syntaxjason.pulsar.examples.shared;

import de.syntaxjason.pulsar.packet.Packet;

public record ChatPacket(
        String sender,
        String message
) implements Packet {
}
