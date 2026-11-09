package de.syntaxjason.pulsar.examples.protocol;

import de.syntaxjason.pulsar.protocol.ApplicationProtocol;
import de.syntaxjason.pulsar.protocol.ProtocolDetection;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;

import java.nio.charset.StandardCharsets;

/** Small HTTP/1.x example, deliberately separate from Pulsar's core runtime dependencies. */
public final class HttpStatusProtocol implements ApplicationProtocol {
    private static final String[] METHODS = {"GET ", "HEAD ", "POST ", "PUT ", "PATCH ", "DELETE ", "OPTIONS ", "TRACE ", "CONNECT "};

    @Override
    public String name() { return "http"; }

    @Override
    public ProtocolDetection detect(ByteBuf prefix) {
        boolean pending = false;
        for (String method : METHODS) {
            boolean compatible = true;
            for (int i = 0; i < Math.min(prefix.readableBytes(), method.length()); i++) {
                if (prefix.getUnsignedByte(i) != method.charAt(i)) {
                    compatible = false;
                    break;
                }
            }
            if (compatible) {
                if (prefix.readableBytes() >= method.length()) return ProtocolDetection.MATCH;
                pending = true;
            }
        }
        return pending ? ProtocolDetection.NEED_MORE_DATA : ProtocolDetection.NO_MATCH;
    }

    @Override
    public void configure(ChannelPipeline pipeline) {
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(4096, true));
        pipeline.addLast(new SimpleChannelInboundHandler<FullHttpRequest>() {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) {
                boolean valid = request.decoderResult().isSuccess();
                boolean statusRoute = request.uri().equals("/health")
                        && (request.method().equals(HttpMethod.GET) || request.method().equals(HttpMethod.HEAD));
                HttpResponseStatus status = !valid ? HttpResponseStatus.BAD_REQUEST
                        : statusRoute ? HttpResponseStatus.OK : HttpResponseStatus.NOT_FOUND;
                byte[] body = (status == HttpResponseStatus.OK ? "ok\n" : status.reasonPhrase() + "\n")
                        .getBytes(StandardCharsets.UTF_8);
                boolean head = request.method().equals(HttpMethod.HEAD);
                var response = new DefaultFullHttpResponse(request.protocolVersion(), status,
                        head ? Unpooled.EMPTY_BUFFER : Unpooled.wrappedBuffer(body));
                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");
                HttpUtil.setContentLength(response, body.length);
                boolean keepAlive = valid && HttpUtil.isKeepAlive(request);
                HttpUtil.setKeepAlive(response, keepAlive);
                var write = ctx.writeAndFlush(response);
                if (!keepAlive) write.addListener(ChannelFutureListener.CLOSE);
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { ctx.close(); }
        });
    }
}
