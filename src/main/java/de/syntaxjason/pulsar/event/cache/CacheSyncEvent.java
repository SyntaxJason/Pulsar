package de.syntaxjason.pulsar.event.cache;

import de.syntaxjason.pulsar.event.Event;

public record CacheSyncEvent(String cache, int entries) implements Event {
}
