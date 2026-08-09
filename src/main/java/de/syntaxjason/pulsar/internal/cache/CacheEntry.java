package de.syntaxjason.pulsar.internal.cache;

record CacheEntry<V>(V value, long expiresAtMillis, long generation) {

    boolean infinite() {
        return expiresAtMillis == 0L;
    }
}
