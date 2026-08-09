package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.connection.Connection;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.cache.NetworkCacheImpl;
import de.syntaxjason.pulsar.packet.Packet;
import io.netty.channel.Channel;

import java.net.SocketAddress;
import java.util.UUID;

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
