package de.syntaxjason.pulsar.event.connection;

import de.syntaxjason.pulsar.connection.Connection;
import de.syntaxjason.pulsar.event.Event;

public record ConnectionCloseEvent(Connection connection) implements Event {
}
