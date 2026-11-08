package de.syntaxjason.pulsar.internal.cache;

import de.syntaxjason.pulsar.cache.CacheState;
import de.syntaxjason.pulsar.cache.NetworkCache;
import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.cache.CacheSyncEvent;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

public final class CacheManager implements AutoCloseable {

    private final Map<String, NetworkCacheImpl<?, ?>> caches = new ConcurrentHashMap<>();
    private final EventBus events;
    private final boolean server;
    private final ScheduledExecutorService scheduler;

    private volatile Consumer<CacheMutation> mutationSink = mutation -> {
    };
    private volatile Consumer<String> subscribeSink = cache -> {
    };
    private volatile Consumer<String> registrationSink = cache -> {
    };
    private volatile boolean connected;
    private volatile boolean closed;

    public CacheManager(EventBus events, boolean server) {
        this.events = events;
        this.server = server;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform()
                .daemon()
                .name("pulsar-cache-expiry-", 0)
                .factory());
        this.connected = server;
    }

    @SuppressWarnings("unchecked")
    public <K, V> NetworkCache<K, V> cache(String name, Class<K> keyType, Class<V> valueType) {
        ensureOpen();
        validateName(name);

        NetworkCacheImpl<?, ?> existing = caches.get(name);
        if (existing != null) {
            if (existing.keyType() != keyType || existing.valueType() != valueType) {
                throw new IllegalArgumentException("Cache '" + name + "' already exists with different key/value types");
            }
            return (NetworkCache<K, V>) existing;
        }

        NetworkCacheImpl<K, V> created = new NetworkCacheImpl<>(
                name,
                keyType,
                valueType,
                events,
                this,
                scheduler,
                server
        );

        NetworkCacheImpl<?, ?> raced = caches.putIfAbsent(name, created);
        NetworkCacheImpl<?, ?> result = raced == null ? created : raced;
        if (result.keyType() != keyType || result.valueType() != valueType) {
            throw new IllegalArgumentException("Cache '" + name + "' already exists with different key/value types");
        }

        if (raced == null) {
            registrationSink.accept(name);
            if (!server && connected) {
                created.state(CacheState.SYNCING);
                subscribeSink.accept(name);
            }
        }

        return (NetworkCache<K, V>) result;
    }

    public NetworkCacheImpl<?, ?> raw(String name) {
        return caches.get(name);
    }

    public Collection<NetworkCacheImpl<?, ?>> all() {
        return List.copyOf(caches.values());
    }

    public void mutationSink(Consumer<CacheMutation> sink) {
        mutationSink = sink;
    }

    public void subscribeSink(Consumer<String> sink) {
        subscribeSink = sink;
    }

    public void registrationSink(Consumer<String> sink) {
        registrationSink = sink;
    }

    public void ensureWritable() {
        ensureOpen();
        if (!server && !connected) {
            throw new IllegalStateException("Cannot mutate a network cache while the Pulsar client is disconnected");
        }
    }

    public void emit(CacheMutation mutation) {
        mutationSink.accept(mutation);
    }

    public boolean applyRemote(CacheMutation mutation) {
        NetworkCacheImpl<?, ?> cache = caches.get(mutation.cache());
        if (cache == null) {
            return false;
        }

        if (mutation instanceof CacheMutation.Put put) {
            cache.applyRemotePut(put.key(), put.value(), put.ttlMillis());
            return true;
        }
        if (mutation instanceof CacheMutation.Remove remove) {
            cache.applyRemoteRemove(remove.key());
            return true;
        }
        if (mutation instanceof CacheMutation.Clear) {
            cache.applyRemoteClear();
            return true;
        }
        return false;
    }

    public void applySnapshot(String name, List<CacheSnapshotEntry> entries) {
        NetworkCacheImpl<?, ?> cache = caches.get(name);
        if (cache == null) {
            return;
        }

        cache.applySnapshot(entries);
        cache.state(CacheState.SYNCHRONIZED);
        events.fire(new CacheSyncEvent(name, entries.size()));
    }

    public void connected() {
        connected = true;
        if (server) {
            return;
        }

        caches.values().forEach(cache -> {
            cache.state(CacheState.SYNCING);
            subscribeSink.accept(cache.name());
        });
    }

    public void disconnected() {
        connected = false;
        if (server) {
            return;
        }
        caches.values().forEach(cache -> cache.state(cache.size() == 0 ? CacheState.OFFLINE : CacheState.STALE));
    }

    @Override
    public void close() {
        closed = true;
        connected = false;
        scheduler.shutdownNow();
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("Pulsar cache manager is closed");
    }

    private void validateName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Cache name must not be blank");
        }
        if (name.length() > 128) {
            throw new IllegalArgumentException("Cache name is too long");
        }
    }
}
