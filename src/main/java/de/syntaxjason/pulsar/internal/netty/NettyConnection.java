package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.connection.Connection;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.cache.NetworkCacheImpl;
import de.syntaxjason.pulsar.packet.Packet;
import io.netty.channel.Channel;
import io.netty.buffer.ByteBuf;

import java.net.SocketAddress;
import java.util.UUID;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public final class NettyConnection implements Connection {

    private final UUID id = UUID.randomUUID();
    private final Channel channel;
    private final InternalEndpoint endpoint;

    NettyConnection(Channel channel, InternalEndpoint endpoint) {
        this.channel = channel;
        this.endpoint = endpoint;
    }

    @Override
    public UUID id() {
        return id;
    }

    @Override
    public SocketAddress remoteAddress() {
        return channel.remoteAddress();
    }

    @Override
    public boolean open() {
        return channel.isActive();
    }

    @Override
    public void send(Packet packet) {
        if (!open()) {
            throw new IllegalStateException("Connection is closed");
        }
        ProtocolWriter.packet(this, packet);
    }

    @Override
    public void sendAll(Iterable<? extends Packet> packets) {
        Objects.requireNonNull(packets, "packets");
        if (!open()) throw new IllegalStateException("Connection is closed");
        List<ByteBuf> buffers = new ArrayList<>();
        try {
            for (Packet packet : packets) buffers.add(ProtocolWriter.encodePacket(this, packet));
        } catch (Throwable failure) {
            buffers.forEach(ByteBuf::release);
            throw failure;
        }
        if (buffers.isEmpty()) return;
        if (channel.eventLoop().inEventLoop()) {
            writeBatch(buffers);
        } else {
            try {
                channel.eventLoop().execute(() -> writeBatch(buffers));
            } catch (RuntimeException failure) {
                buffers.forEach(ByteBuf::release);
                throw failure;
            }
        }
    }

    /** Shares immutable encoded content; each recipient owns an independent reader index. */
    public static void broadcast(Collection<NettyConnection> connections, Packet packet) {
        List<NettyConnection> recipients = connections.stream().filter(NettyConnection::open).toList();
        if (recipients.isEmpty()) return;
        ByteBuf encoded = ProtocolWriter.encodePacket(recipients.getFirst(), packet);
        try {
            for (NettyConnection recipient : recipients) {
                recipient.channel.writeAndFlush(encoded.retainedDuplicate());
            }
        } finally {
            encoded.release();
        }
    }

    private void writeBatch(List<ByteBuf> buffers) {
        int next = 0;
        try {
            while (next < buffers.size()) channel.write(buffers.get(next++));
        } finally {
            while (next < buffers.size()) buffers.get(next++).release();
            channel.flush();
        }
    }

    public void sendCacheMutation(CacheMutation mutation) {
        if (!open()) {
            return;
        }
        ProtocolWriter.mutation(this, mutation);
    }

    public void subscribe(String cache) {
        if (!open()) {
            return;
        }
        ProtocolWriter.subscribe(this, cache);
    }

    public void sendSnapshot(NetworkCacheImpl<?, ?> cache) {
        if (!open()) {
            return;
        }
        ProtocolWriter.snapshot(this, cache);
    }

    @Override
    public void close() {
        channel.close();
    }

    Channel channel() {
        return channel;
    }

    InternalEndpoint endpoint() {
        return endpoint;
    }
}
