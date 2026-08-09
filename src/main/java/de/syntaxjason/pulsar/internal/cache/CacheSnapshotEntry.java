package de.syntaxjason.pulsar.internal.cache;

public record CacheSnapshotEntry(Object key, Object value, long ttlMillis) {
}
