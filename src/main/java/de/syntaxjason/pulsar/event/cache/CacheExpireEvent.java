package de.syntaxjason.pulsar.event.cache;

import de.syntaxjason.pulsar.event.Event;

public record CacheExpireEvent<K, V>(String cache, K key, V value) implements Event {
}
