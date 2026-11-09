package de.syntaxjason.pulsar.protocol;

import de.syntaxjason.pulsar.Pulsar;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ApplicationProtocolTest {
    @ParameterizedTest
    @ValueSource(strings = {"", "pulsar", "HTTP", "with spaces", "../http"})
    void invalidAndReservedProtocolNamesFailBeforeNetworking(String name) {
        assertThrows(IllegalArgumentException.class,
                () -> Pulsar.server().protocol(ApplicationProtocol.withPrefix(name, new byte[]{1}, pipeline -> { })));
    }

    @Test
    void duplicateNamesAndExcessRegistrationsFailBeforeNetworking() {
        var builder = Pulsar.server().protocol(protocol("first"));
        assertThrows(IllegalArgumentException.class, () -> builder.protocol(protocol("first")));
        for (int i = 1; i < 32; i++) builder.protocol(protocol("protocol" + i));
        assertThrows(IllegalArgumentException.class, () -> builder.protocol(protocol("overflow")));
    }

    @Test
    void probeAndTimeoutOptionsRejectInvalidValues() {
        assertThrows(IllegalArgumentException.class, () -> Pulsar.server().maxProtocolProbeBytes(7));
        assertThrows(IllegalArgumentException.class, () -> Pulsar.server().maxProtocolProbeBytes(65_537));
        assertThrows(IllegalArgumentException.class, () -> Pulsar.server().protocolDetectionTimeout(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> Pulsar.server().protocolDetectionTimeout(Duration.ofNanos(1)));
        assertThrows(IllegalArgumentException.class, () -> Pulsar.server().protocolDetectionTimeout(Duration.ofSeconds(Long.MAX_VALUE)));
        assertThrows(IllegalArgumentException.class, () -> Pulsar.server().protocolDetectionTimeout(Duration.ofSeconds(61)));
        assertThrows(NullPointerException.class, () -> Pulsar.server().protocol(null));
        assertThrows(NullPointerException.class, () -> Pulsar.server().protocolDetectionTimeout(null));
    }

    @Test
    void prefixFactoryDefensivelyCopiesSignatureAndUsesTriStateDetection() {
        byte[] signature = {'A', 'B'};
        ApplicationProtocol protocol = ApplicationProtocol.withPrefix("example", signature, pipeline -> { });
        signature[0] = 'X';
        ByteBuf prefix = Unpooled.copiedBuffer("AB-body", StandardCharsets.US_ASCII);
        ByteBuf wrong = Unpooled.copiedBuffer("AX", StandardCharsets.US_ASCII);
        try {
            assertEquals(ProtocolDetection.NEED_MORE_DATA, protocol.detect(prefix.slice(0, 1)));
            assertEquals(ProtocolDetection.MATCH, protocol.detect(prefix));
            assertEquals(ProtocolDetection.NO_MATCH, protocol.detect(wrong));
            assertEquals(0, prefix.readerIndex());
        } finally { prefix.release(); wrong.release(); }
        assertThrows(IllegalArgumentException.class, () -> ApplicationProtocol.withPrefix("empty", new byte[0], pipeline -> { }));
    }

    private static ApplicationProtocol protocol(String name) {
        return ApplicationProtocol.withPrefix(name, new byte[]{1}, pipeline -> { });
    }
}
