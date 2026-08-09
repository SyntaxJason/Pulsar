package de.syntaxjason.pulsar.examples.shared;

import de.syntaxjason.pulsar.packet.Packet;

public record LocationPacket(
        String player,
        Location location
) implements Packet {
}
