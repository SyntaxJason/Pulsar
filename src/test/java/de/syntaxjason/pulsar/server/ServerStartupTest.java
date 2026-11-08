package de.syntaxjason.pulsar.server;

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

class ServerStartupTest {
    @Test
    @Timeout(10)
    void bindFailureClosesResourcesEvenWhenNettyRethrowsACheckedException() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            PacketRegistry packets = new PacketRegistry();
            SerializerRegistry serializers = new SerializerRegistry();
            var bootstrap = PulsarBootstrap.prepare(packets, serializers, List.of(), List.of(), NoOpPulsarLogger.INSTANCE);
            PulsarServer server = new PulsarServer("127.0.0.1", occupied.getLocalPort(), bootstrap.events(), packets,
                    serializers, bootstrap.serializer(), bootstrap.report(), NoOpPulsarLogger.INSTANCE);
            try {
                assertThrows(java.net.BindException.class, server::startInternal);
                assertEquals(RuntimeState.CLOSED, server.state());
                server.closeAsync().get(3, TimeUnit.SECONDS);
                assertFalse(server.running());
            } finally { server.close(); }
        }
    }
}
