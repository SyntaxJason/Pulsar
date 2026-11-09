package de.syntaxjason.pulsar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelPipeline;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * An application protocol selected once per TCP connection, before its decoder is installed.
 * Instances are shared across connections and must be thread-safe. Create fresh stateful
 * channel handlers in {@link #configure(ChannelPipeline)}.
 */
public interface ApplicationProtocol {
    /** A unique name; {@code pulsar} is reserved for Pulsar's built-in protocol. */
    String name();

    /** Creates a prefix-matched protocol; the prefix remains part of the stream passed to its handlers. */
    static ApplicationProtocol withPrefix(String name, byte[] signature, Consumer<ChannelPipeline> configure) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(configure, "configure");
        byte[] expected = Objects.requireNonNull(signature, "signature").clone();
        if (expected.length == 0 || expected.length > 65_536) {
            throw new IllegalArgumentException("Signature length must be 1..65536 bytes");
        }
        return new ApplicationProtocol() {
            @Override public String name() { return name; }
            @Override public ProtocolDetection detect(ByteBuf prefix) {
                for (int i = 0; i < Math.min(prefix.readableBytes(), expected.length); i++) {
                    if (prefix.getByte(i) != expected[i]) return ProtocolDetection.NO_MATCH;
                }
                return prefix.readableBytes() < expected.length
                        ? ProtocolDetection.NEED_MORE_DATA : ProtocolDetection.MATCH;
            }
            @Override public void configure(ChannelPipeline pipeline) { configure.accept(pipeline); }
        };
    }

    /**
     * Inspects a bounded, read-only, non-owning prefix with reader index zero.
     * Do not retain the probe. MATCH and NO_MATCH must remain valid as more bytes arrive.
     */
    ProtocolDetection detect(ByteBuf prefix);

    /** Adds this connection's handlers to the end of the pipeline. */
    void configure(ChannelPipeline pipeline);
}
