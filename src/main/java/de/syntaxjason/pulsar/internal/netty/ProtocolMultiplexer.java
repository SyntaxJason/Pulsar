package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.connection.ProtocolSelectedEvent;
import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.protocol.ApplicationProtocol;
import de.syntaxjason.pulsar.protocol.ProtocolDetection;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Removed after selection: established connections do not pay a per-frame routing cost. */
final class ProtocolMultiplexer extends ByteToMessageDecoder {
    private final InternalEndpoint endpoint;
    private final List<ApplicationProtocol> protocols;
    private final int maxProbeBytes;
    private final long timeoutMillis;
    private ScheduledFuture<?> timeout;
    private boolean selected;

    ProtocolMultiplexer(InternalEndpoint endpoint, List<ApplicationProtocol> protocols,
                        int maxProbeBytes, long timeoutMillis) {
        this.endpoint = endpoint;
        this.protocols = protocols;
        this.maxProbeBytes = maxProbeBytes;
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        if (ctx.channel().isActive()) startTimeout(ctx);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        startTimeout(ctx);
        // Deliver channelActive to the selected protocol before forwarding any buffered bytes.
    }

    private void startTimeout(ChannelHandlerContext ctx) {
        if (timeout == null) {
            timeout = ctx.executor().schedule(() -> {
                if (!selected) ctx.close();
            }, timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf input, List<Object> output) {
        if (selected || !input.isReadable()) return;
        int available = Math.min(input.readableBytes(), maxProbeBytes);
        ProtocolDetection nativeResult = detectPulsar(input, available);
        boolean pending = nativeResult == ProtocolDetection.NEED_MORE_DATA;
        boolean nativeMatch = nativeResult == ProtocolDetection.MATCH;
        int matches = nativeMatch ? 1 : 0;
        ApplicationProtocol chosen = null;

        for (ApplicationProtocol protocol : protocols) {
            ByteBuf probe = Unpooled.unreleasableBuffer(
                    input.slice(input.readerIndex(), available).asReadOnly());
            ProtocolDetection result = protocol.detect(probe);
            if (result == null) throw new ProtocolException("Null detection result from " + protocol.name());
            if (result == ProtocolDetection.MATCH) {
                matches++;
                chosen = protocol;
            } else if (result == ProtocolDetection.NEED_MORE_DATA) {
                pending = true;
            }
        }
        if (matches > 1) throw new ProtocolException("Ambiguous application protocol prefix");
        if (matches == 1 && !pending) {
            selected = true;
            cancelTimeout();
            String name = nativeMatch ? "pulsar" : chosen.name();
            if (nativeMatch) PulsarChannelInitializer.installPulsar(ctx.pipeline(), endpoint);
            else {
                chosen.configure(ctx.pipeline());
                ctx.pipeline().addLast("pulsar-protocol-errors", new ChannelInboundHandlerAdapter() {
                    @Override
                    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
                        context.close();
                    }
                });
            }
            endpoint.eventBus().fire(new ProtocolSelectedEvent(name, ctx.channel().remoteAddress()));
            if (!ctx.channel().isActive()) return;
            ctx.fireChannelActive();
            // ByteToMessageDecoder forwards its untouched cumulation on removal.
            if (!ctx.isRemoved()) ctx.pipeline().remove(this);
        } else if (!pending || available == maxProbeBytes) {
            throw new ProtocolException("Unrecognized application protocol within " + available + " bytes");
        }
    }

    private static ProtocolDetection detectPulsar(ByteBuf input, int available) {
        int start = input.readerIndex();
        if (input.getUnsignedByte(start) != 0) return ProtocolDetection.NO_MATCH;
        if (available < 4) return ProtocolDetection.NEED_MORE_DATA;
        int length = input.getInt(start);
        if (length < 6 || length > ProtocolWriter.MAX_PAYLOAD_SIZE) return ProtocolDetection.NO_MATCH;
        int magicBytes = Math.min(available - 4, 4);
        for (int i = 0; i < magicBytes; i++) {
            int expected = (ProtocolWriter.MAGIC >>> (24 - i * 8)) & 0xff;
            if (input.getUnsignedByte(start + 4 + i) != expected) return ProtocolDetection.NO_MATCH;
        }
        return available < 8 ? ProtocolDetection.NEED_MORE_DATA : ProtocolDetection.MATCH;
    }

    private void cancelTimeout() {
        if (timeout != null) timeout.cancel(false);
    }

    @Override
    protected void handlerRemoved0(ChannelHandlerContext ctx) {
        cancelTimeout();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cancelTimeout();
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        selected = true;
        cancelTimeout();
        try {
            endpoint.logger().debug(LogCategory.NETWORK,
                    "Protocol selection failed from " + ctx.channel().remoteAddress() + ": " + cause.getMessage());
        } finally {
            ctx.close();
        }
    }
}
