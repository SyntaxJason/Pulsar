package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.protocol.ApplicationProtocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.util.List;

public final class PulsarChannelInitializer extends ChannelInitializer<Channel> {

    private final InternalEndpoint endpoint;
    private final List<ApplicationProtocol> protocols;
    private final int maxProbeBytes;
    private final long timeoutMillis;

    public PulsarChannelInitializer(InternalEndpoint endpoint) {
        this(endpoint, List.of(), 64, 5000);
    }

    public PulsarChannelInitializer(InternalEndpoint endpoint, List<ApplicationProtocol> protocols,
                                    int maxProbeBytes, long timeoutMillis) {
        if (maxProbeBytes < 8 || maxProbeBytes > 65_536 || timeoutMillis < 1 || timeoutMillis > 60_000) {
            throw new IllegalArgumentException("Invalid protocol detection limits");
        }
        this.endpoint = java.util.Objects.requireNonNull(endpoint, "endpoint");
        this.protocols = List.copyOf(protocols);
        this.maxProbeBytes = maxProbeBytes;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    protected void initChannel(Channel channel) {
        if (protocols.isEmpty()) {
            installPulsar(channel.pipeline(), endpoint);
        } else {
            channel.pipeline().addLast("pulsar-protocol-selector",
                    new ProtocolMultiplexer(endpoint, protocols, maxProbeBytes, timeoutMillis));
        }
    }

    static void installPulsar(ChannelPipeline pipeline, InternalEndpoint endpoint) {
        pipeline.addLast("pulsar-frame-decoder", new LengthFieldBasedFrameDecoder(
                ProtocolWriter.MAX_FRAME_SIZE,
                0,
                4,
                0,
                4
        ));
        pipeline.addLast("pulsar-frame-encoder", new LengthFieldPrepender(4));
        pipeline.addLast("pulsar-handler", new PulsarInboundHandler(endpoint));
    }
}
