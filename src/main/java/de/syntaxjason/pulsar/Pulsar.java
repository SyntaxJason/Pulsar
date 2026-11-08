package de.syntaxjason.pulsar;

import de.syntaxjason.pulsar.client.PulsarClientBuilder;
import de.syntaxjason.pulsar.server.PulsarServerBuilder;

public final class Pulsar {

    public static final String VERSION = "0.3.0-SNAPSHOT";

    private Pulsar() {
    }

    public static PulsarServerBuilder server() {
        return new PulsarServerBuilder();
    }

    public static PulsarClientBuilder client() {
        return new PulsarClientBuilder();
    }
}
