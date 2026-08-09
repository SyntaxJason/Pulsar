package de.syntaxjason.pulsar.internal.cache;

public sealed interface CacheMutation permits CacheMutation.Put, CacheMutation.Remove, CacheMutation.Clear {

    String cache();

    record Put(String cache, Object key, Object value, long ttlMillis) implements CacheMutation {
    }

    record Remove(String cache, Object key) implements CacheMutation {
    }

    record Clear(String cache) implements CacheMutation {
    }
}
