package de.syntaxjason.pulsar.client;

import de.syntaxjason.pulsar.bootstrap.RuntimeState;
import de.syntaxjason.pulsar.internal.bootstrap.PulsarBootstrap;
import de.syntaxjason.pulsar.logging.NoOpPulsarLogger;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ClientStartupTest {
    @Test
    @Timeout(10)
    void connectionFailureClosesResourcesEvenWhenNettyRethrowsACheckedException() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = socket.getLocalPort();
        }
        PacketRegistry packets = new PacketRegistry();
        SerializerRegistry serializers = new SerializerRegistry();
        var bootstrap = PulsarBootstrap.prepare(packets, serializers, List.of(), List.of(), NoOpPulsarLogger.INSTANCE);
        PulsarClient client = new PulsarClient("127.0.0.1", port, bootstrap.events(), packets, serializers,
                bootstrap.serializer(), bootstrap.report(), NoOpPulsarLogger.INSTANCE);
        try {
            assertThrows(java.net.ConnectException.class, client::connectInternal);
            assertEquals(RuntimeState.CLOSED, client.state());
            client.closeAsync().get(3, TimeUnit.SECONDS);
            assertFalse(client.connected());
        } finally { client.close(); }
    }
}
