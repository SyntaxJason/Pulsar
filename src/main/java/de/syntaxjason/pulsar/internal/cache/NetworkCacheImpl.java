package de.syntaxjason.pulsar.internal.cache;

import de.syntaxjason.pulsar.cache.CacheOrigin;
import de.syntaxjason.pulsar.cache.CacheState;
import de.syntaxjason.pulsar.cache.NetworkCache;
import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.cache.CacheClearEvent;
import de.syntaxjason.pulsar.event.cache.CacheExpireEvent;
import de.syntaxjason.pulsar.event.cache.CachePutEvent;
import de.syntaxjason.pulsar.event.cache.CacheRemoveEvent;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class NetworkCacheImpl<K, V> implements NetworkCache<K, V> {

    static final long FOREVER = -1L;

    private final String name;
    private final Class<K> keyType;
    private final Class<V> valueType;
    private final EventBus events;
    private final CacheManager manager;
    private final ScheduledExecutorService scheduler;
    private final boolean authoritative;
    private final Map<K, CacheEntry<V>> entries = new ConcurrentHashMap<>();
    private final AtomicLong generations = new AtomicLong();

    private volatile CacheState state;

    NetworkCacheImpl(
            String name,
            Class<K> keyType,
            Class<V> valueType,
            EventBus events,
            CacheManager manager,
            ScheduledExecutorService scheduler,
            boolean authoritative
    ) {
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;
        this.events = events;
        this.manager = manager;
        this.scheduler = scheduler;
        this.authoritative = authoritative;
        this.state = authoritative ? CacheState.SYNCHRONIZED : CacheState.OFFLINE;
    }

    @Override
    public String name() {
        return name;
    }

    public Class<K> keyType() {
        return keyType;
    }

    public Class<V> valueType() {
        return valueType;
    }

    @Override
    public V get(K key) {
        CacheEntry<V> entry = liveEntry(key);
        return entry == null ? null : entry.value();
    }

    @Override
    public Optional<V> find(K key) {
        return Optional.ofNullable(get(key));
    }

    @Override
    public boolean contains(K key) {
        return liveEntry(key) != null;
    }

    @Override
    public V put(K key, V value) {
        manager.ensureWritable();
        return putInternal(key, value, FOREVER, CacheOrigin.LOCAL, true);
    }

    @Override
    public V put(K key, V value, Duration ttl) {
        manager.ensureWritable();
        if (ttl == null || ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("TTL must be greater than zero");
        }
        long millis;
        try {
            millis = ttl.toMillis();
        } catch (ArithmeticException failure) {
            throw new IllegalArgumentException("TTL is too large", failure);
        }
        if (millis == 0) throw new IllegalArgumentException("TTL must be at least one millisecond");
        return putInternal(key, value, millis, CacheOrigin.LOCAL, true);
    }

    @Override
    public V remove(K key) {
        manager.ensureWritable();
        return removeInternal(key, CacheOrigin.LOCAL, true);
    }

    @Override
    public void clear() {
        manager.ensureWritable();
        clearInternal(CacheOrigin.LOCAL, true);
    }

    @Override
    public int size() {
        purgeExpiredLocally();
        return entries.size();
    }

    @Override
    public Map<K, V> snapshot() {
        purgeExpiredLocally();
        Map<K, V> values = new LinkedHashMap<>();
        entries.forEach((key, entry) -> values.put(key, entry.value()));
        return Map.copyOf(values);
    }

    @Override
    public CacheState state() {
        return state;
    }

    void state(CacheState state) {
        this.state = state;
    }

    void applyRemotePut(Object key, Object value, long ttlMillis) {
        putInternal(keyType.cast(key), valueType.cast(value), ttlMillis, CacheOrigin.REMOTE, false);
    }

    void applyRemoteRemove(Object key) {
        removeInternal(keyType.cast(key), CacheOrigin.REMOTE, false);
    }

    void applyRemoteClear() {
        clearInternal(CacheOrigin.REMOTE, false);
    }

    void applySnapshot(List<CacheSnapshotEntry> snapshot) {
        Map<K, CacheEntry<V>> replacement = new HashMap<>();
        long now = System.currentTimeMillis();

        for (CacheSnapshotEntry item : snapshot) {
            K key = keyType.cast(item.key());
            V value = valueType.cast(item.value());
            if (key == null || value == null) {
                throw new IllegalArgumentException("Cache keys and values must not be null");
            }
            long expiresAt = expiryAt(item.ttlMillis(), now);
            long generation = generations.incrementAndGet();
            replacement.put(key, new CacheEntry<>(value, expiresAt, generation));
        }
        entries.clear();
        entries.putAll(replacement);
    }

    public List<CacheSnapshotEntry> snapshotEntries() {
        purgeExpiredLocally();
        long now = System.currentTimeMillis();
        return entries.entrySet().stream()
                .map(entry -> {
                    CacheEntry<V> value = entry.getValue();
                    long ttl = value.infinite() ? FOREVER : Math.max(1L, value.expiresAtMillis() - now);
                    return new CacheSnapshotEntry(entry.getKey(), value.value(), ttl);
                })
                .toList();
    }

    private V putInternal(K key, V value, long ttlMillis, CacheOrigin origin, boolean emit) {
        if (key == null) {
            throw new IllegalArgumentException("Cache key must not be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("Cache value must not be null");
        }
        if (ttlMillis != FOREVER && ttlMillis <= 0) {
            throw new IllegalArgumentException("TTL must be greater than zero or FOREVER");
        }

        long generation = generations.incrementAndGet();
        long expiresAt = expiryAt(ttlMillis, System.currentTimeMillis());
        CacheEntry<V> previous = entries.put(key, new CacheEntry<>(value, expiresAt, generation));

        if (authoritative && ttlMillis != FOREVER) {
            scheduleExpiry(key, generation, ttlMillis);
        }

        events.fire(new CachePutEvent<>(name, key, previous == null ? null : previous.value(), value, origin));

        if (emit) {
            manager.emit(new CacheMutation.Put(name, key, value, ttlMillis));
        }

        return previous == null ? null : previous.value();
    }

    private V removeInternal(K key, CacheOrigin origin, boolean emit) {
        CacheEntry<V> removed = entries.remove(key);
        if (removed == null) {
            return null;
        }

        events.fire(new CacheRemoveEvent<>(name, key, removed.value(), origin));

        if (emit) {
            manager.emit(new CacheMutation.Remove(name, key));
        }
        return removed.value();
    }

    private void clearInternal(CacheOrigin origin, boolean emit) {
        int size = entries.size();
        entries.clear();
        events.fire(new CacheClearEvent(name, size, origin));

        if (emit) {
            manager.emit(new CacheMutation.Clear(name));
        }
    }

    private CacheEntry<V> liveEntry(K key) {
        CacheEntry<V> entry = entries.get(key);
        if (entry == null || entry.infinite()) {
            return entry;
        }
        if (entry.expiresAtMillis() > System.currentTimeMillis()) {
            return entry;
        }

        if (authoritative) {
            expire(key, entry.generation());
        } else {
            entries.remove(key, entry);
        }
        return null;
    }

    private void purgeExpiredLocally() {
        long now = System.currentTimeMillis();
        entries.forEach((key, entry) -> {
            if (entry.infinite() || entry.expiresAtMillis() > now) {
                return;
            }
            if (authoritative) {
                expire(key, entry.generation());
            } else {
                entries.remove(key, entry);
            }
        });
    }

    private void scheduleExpiry(K key, long generation, long ttlMillis) {
        scheduler.schedule(() -> expire(key, generation), ttlMillis, TimeUnit.MILLISECONDS);
    }

    private void expire(K key, long generation) {
        CacheEntry<V> current = entries.get(key);
        if (current == null || current.generation() != generation) {
            return;
        }
        if (!current.infinite() && current.expiresAtMillis() > System.currentTimeMillis()) {
            return;
        }

        // A concurrent put may replace the entry after the generation check.
        if (!entries.remove(key, current)) return;
        events.fire(new CacheExpireEvent<>(name, key, current.value()));
        manager.emit(new CacheMutation.Remove(name, key));
    }

    private static long expiryAt(long ttlMillis, long now) {
        if (ttlMillis == FOREVER) return 0L;
        if (ttlMillis <= 0 || ttlMillis > Long.MAX_VALUE - now) {
            throw new IllegalArgumentException("TTL must be positive and fit into an absolute millisecond timestamp");
        }
        return now + ttlMillis;
    }
}
