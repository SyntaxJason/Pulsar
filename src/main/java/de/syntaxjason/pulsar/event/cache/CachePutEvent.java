package de.syntaxjason.pulsar.event.cache;

import de.syntaxjason.pulsar.cache.CacheOrigin;
import de.syntaxjason.pulsar.event.Event;

public record CachePutEvent<K, V>(
        String cache,
        K key,
        V oldValue,
        V value,
        CacheOrigin origin
) implements Event {
}
