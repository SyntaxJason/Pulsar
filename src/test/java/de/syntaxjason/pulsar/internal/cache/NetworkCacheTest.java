package de.syntaxjason.pulsar.internal.cache;

import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class NetworkCacheTest {
    @Test
    void expiryCannotDeleteAReplacementWrittenAfterTheGenerationCheck() throws Exception {
        CountDownLatch removing = new CountDownLatch(1);
        CountDownLatch replaced = new CountDownLatch(1);
        AtomicInteger expiryHashes = new AtomicInteger();
        Object key = new Object() {
            @Override public int hashCode() {
                if (Thread.currentThread().getName().equals("expiry-test") && expiryHashes.incrementAndGet() == 2) {
                    removing.countDown();
                    try {
                        if (!replaced.await(5, TimeUnit.SECONDS)) throw new AssertionError("replacement did not arrive");
                    } catch (InterruptedException failure) { throw new AssertionError(failure); }
                }
                return 42;
            }
        };
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1,
                task -> new Thread(task, "expiry-test"));
        try (CacheManager manager = new CacheManager(new SimpleEventBus(), true)) {
            NetworkCacheImpl<Object, String> cache = new NetworkCacheImpl<>("race", Object.class,
                    String.class, new SimpleEventBus(), manager, scheduler, true);
            cache.put(key, "old", Duration.ofMillis(50));
            assertTrue(removing.await(5, TimeUnit.SECONDS));
            cache.put(key, "replacement");
            replaced.countDown();
            scheduler.submit(() -> { }).get(5, TimeUnit.SECONDS);
            assertEquals("replacement", cache.get(key));
        } finally {
            replaced.countDown();
            scheduler.shutdownNow();
        }
    }

    @Test
    void overflowingTtlIsRejectedWithoutInstallingAnExpiredEntry() {
        try (CacheManager manager = new CacheManager(new SimpleEventBus(), true)) {
            var cache = manager.cache("ttl", String.class, String.class);
            assertThrows(IllegalArgumentException.class, () -> cache.put("key", "value", Duration.ofMillis(Long.MAX_VALUE)));
            assertNull(cache.get("key"));
            assertThrows(IllegalArgumentException.class, () -> cache.put("key", "value", Duration.ofSeconds(Long.MAX_VALUE)));
            assertThrows(IllegalArgumentException.class, () -> cache.put("key", "value", Duration.ofNanos(1)));
        }
    }

    @Test
    void closedServerCacheRejectsFurtherMutationsAndRegistrations() {
        CacheManager manager = new CacheManager(new SimpleEventBus(), true);
        var cache = manager.cache("closed", String.class, String.class);
        cache.put("key", "value");
        manager.close();
        assertEquals("value", cache.get("key"));
        assertThrows(IllegalStateException.class, () -> cache.put("key", "next"));
        assertThrows(IllegalStateException.class, () -> cache.remove("key"));
        assertThrows(IllegalStateException.class, cache::clear);
        assertThrows(IllegalStateException.class, () -> manager.cache("new", String.class, String.class));
    }

    @Test
    void invalidSnapshotLeavesPreviousDataIntact() {
        try (CacheManager manager = new CacheManager(new SimpleEventBus(), false)) {
            var cache = manager.cache("snapshot", String.class, String.class);
            manager.applySnapshot("snapshot", List.of(new CacheSnapshotEntry("old", "value", -1)));
            assertThrows(IllegalArgumentException.class, () -> manager.applySnapshot("snapshot", List.of(
                    new CacheSnapshotEntry("new", "value", -1), new CacheSnapshotEntry("bad", "value", 0))));
            assertEquals("value", cache.get("old"));
            assertEquals(1, cache.size());
        }
    }
}
