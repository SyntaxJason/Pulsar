package de.syntaxjason.pulsar.cache;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public interface NetworkCache<K, V> {

    String name();

    V get(K key);

    Optional<V> find(K key);

    boolean contains(K key);

    V put(K key, V value);

    V put(K key, V value, Duration ttl);

    V remove(K key);

    void clear();

    int size();

    Map<K, V> snapshot();

    CacheState state();
}
