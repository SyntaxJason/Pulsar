package de.syntaxjason.pulsar.internal.netty;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public final class PulsarChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final InternalEndpoint endpoint;

    public PulsarChannelInitializer(InternalEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    protected void initChannel(SocketChannel channel) {
        ChannelPipeline pipeline = channel.pipeline();
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
