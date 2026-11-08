package de.syntaxjason.pulsar;

import de.syntaxjason.pulsar.bootstrap.RuntimeState;
import de.syntaxjason.pulsar.cache.CacheOrigin;
import de.syntaxjason.pulsar.cache.CacheState;
import de.syntaxjason.pulsar.client.PulsarClient;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.cache.CachePutEvent;
import de.syntaxjason.pulsar.event.cache.CacheRemoveEvent;
import de.syntaxjason.pulsar.event.cache.CacheSyncEvent;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.server.PulsarServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class NetworkIntegrationTest {
    record Sequence(int number) implements Packet { }

    @Test
    void asynchronousEndpointsRoundTripAnOrderedThousandPacketBatch() throws Exception {
        CompletableFuture<List<Integer>> received = new CompletableFuture<>();
        Listener echo = new Listener() {
            @EventHandler public void echo(PacketReceiveEvent<Sequence> event) { event.connection().send(event.packet()); }
        };
        Listener collector = new Listener() {
            final List<Integer> values = new ArrayList<>();
            @EventHandler public void collect(PacketReceiveEvent<Sequence> event) {
                values.add(event.packet().number());
                if (values.size() == 1000) received.complete(List.copyOf(values));
            }
        };
        PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false)
                .listener(echo).startAsync().get(5, TimeUnit.SECONDS);
        PulsarClient client = null;
        try {
            client = Pulsar.client().address("127.0.0.1", server.port()).logging(false)
                    .listener(collector).connectAsync().get(5, TimeUnit.SECONDS);
            assertTrue(client.connected());
            assertEquals(RuntimeState.RUNNING, client.state());
            assertTrue(client.packets().frozen());
            assertThrows(IllegalStateException.class, () -> server.events().register(echo));
            client.sendAll(IntStream.range(0, 1000).mapToObj(Sequence::new).toList());
            assertEquals(IntStream.range(0, 1000).boxed().toList(), received.get(5, TimeUnit.SECONDS));
        } finally { closeBoth(server, client); }
    }

    @Test
    void cacheSnapshotClientWriteAndServerTtlPropagateOverTcp() throws Exception {
        CompletableFuture<Void> synced = new CompletableFuture<>();
        BlockingQueue<String> remotePuts = new LinkedBlockingQueue<>();
        CompletableFuture<Void> expired = new CompletableFuture<>();
        Listener listener = new Listener() {
            @EventHandler public void sync(CacheSyncEvent event) { synced.complete(null); }
            @EventHandler public void put(CachePutEvent<?, ?> event) {
                if (event.origin() == CacheOrigin.REMOTE) remotePuts.add((String) event.key());
            }
            @EventHandler public void remove(CacheRemoveEvent<?, ?> event) {
                if (event.key().equals("ttl")) expired.complete(null);
            }
        };
        PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false).start();
        PulsarClient client = null;
        try {
            var authoritative = server.cache("state", String.class, String.class);
            authoritative.put("initial", "snapshot");
            client = Pulsar.client().address("127.0.0.1", server.port()).logging(false).listener(listener).connect();
            var replica = client.cache("state", String.class, String.class);
            synced.get(5, TimeUnit.SECONDS);
            assertEquals(CacheState.SYNCHRONIZED, replica.state());
            assertEquals("snapshot", replica.get("initial"));
            replica.put("client", "written");
            assertEquals("client", remotePuts.poll(5, TimeUnit.SECONDS));
            assertEquals("written", authoritative.get("client"));
            authoritative.put("ttl", "temporary", Duration.ofMillis(200));
            assertEquals("ttl", remotePuts.poll(5, TimeUnit.SECONDS));
            expired.get(5, TimeUnit.SECONDS);
            assertNull(replica.get("ttl"));
            assertNull(authoritative.get("ttl"));
        } finally { closeBoth(server, client); }
    }

    private static void closeBoth(PulsarServer server, PulsarClient client) throws Exception {
        CompletableFuture.allOf(server.closeAsync(), client == null
                ? CompletableFuture.completedFuture(null) : client.closeAsync()).get(6, TimeUnit.SECONDS);
    }
}
