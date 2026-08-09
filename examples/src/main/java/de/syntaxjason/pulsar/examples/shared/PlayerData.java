package de.syntaxjason.pulsar.examples.shared;

import java.util.UUID;

public record PlayerData(
        UUID uuid,
        String name,
        long coins
) {
}
