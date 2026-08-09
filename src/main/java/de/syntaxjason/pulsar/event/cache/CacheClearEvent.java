package de.syntaxjason.pulsar.event.cache;

import de.syntaxjason.pulsar.cache.CacheOrigin;
import de.syntaxjason.pulsar.event.Event;

public record CacheClearEvent(String cache, int removedEntries, CacheOrigin origin) implements Event {
}
