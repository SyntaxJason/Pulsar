package de.syntaxjason.pulsar;

import de.syntaxjason.pulsar.bootstrap.RuntimeState;
import de.syntaxjason.pulsar.client.PulsarClient;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.server.PulsarServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class EndpointLifecycleTest {
    record Message(String text) implements Packet { }

    @Test
    void serverCanCloseFromPacketCallback() throws Exception {
        AtomicReference<PulsarServer> reference = new AtomicReference<>();
        CompletableFuture<Void> callback = new CompletableFuture<>();
        Listener listener = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Message> event) {
                try {
                    reference.get().close();
                    callback.complete(null);
                } catch (Throwable failure) { callback.completeExceptionally(failure); }
            }
        };
        PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false)
                .packet(Message.class).listener(listener).start();
        reference.set(server);
        PulsarClient client = null;
        try {
            client = Pulsar.client().address("127.0.0.1", server.port()).logging(false)
                    .packet(Message.class).connect();
            client.send(new Message("shutdown"));
            callback.get(5, TimeUnit.SECONDS);
            server.closeAsync().get(5, TimeUnit.SECONDS);
            assertEquals(RuntimeState.CLOSED, server.state());
            assertFalse(server.running());
            assertTrue(server.connections().isEmpty());
            server.close();
        } finally { closeBoth(server, client); }
    }

    @Test
    void clientCanCloseFromPacketCallback() throws Exception {
        AtomicReference<PulsarClient> reference = new AtomicReference<>();
        CompletableFuture<Void> callback = new CompletableFuture<>();
        Listener echo = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Message> event) {
                event.connection().send(event.packet());
            }
        };
        Listener listener = new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<Message> event) {
                try {
                    reference.get().close();
                    callback.complete(null);
                } catch (Throwable failure) { callback.completeExceptionally(failure); }
            }
        };
        PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false)
                .listener(echo).start();
        PulsarClient client = null;
        try {
            client = Pulsar.client().address("127.0.0.1", server.port()).logging(false)
                    .listener(listener).connect();
            reference.set(client);
            client.send(new Message("shutdown"));
            callback.get(5, TimeUnit.SECONDS);
            client.closeAsync().get(5, TimeUnit.SECONDS);
            assertEquals(RuntimeState.CLOSED, client.state());
            assertFalse(client.connected());
            assertThrows(IllegalStateException.class, () -> reference.get().send(new Message("closed")));
            client.close();
        } finally { closeBoth(server, client); }
    }

    @Test
    void cancellingShutdownObserverCannotCancelActualShutdown() throws Exception {
        PulsarServer server = Pulsar.server().host("127.0.0.1").port(0).logging(false).start();
        try {
            server.closeAsync().cancel(true);
            server.closeAsync().get(5, TimeUnit.SECONDS);
            assertEquals(RuntimeState.CLOSED, server.state());
        } finally { server.close(); }
    }

    private static void closeBoth(PulsarServer server, PulsarClient client) throws Exception {
        CompletableFuture.allOf(server.closeAsync(), client == null
                ? CompletableFuture.completedFuture(null) : client.closeAsync()).get(6, TimeUnit.SECONDS);
    }
}
