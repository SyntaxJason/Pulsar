package de.syntaxjason.pulsar.internal.netty;

import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.internal.bootstrap.PulsarBootstrap;
import de.syntaxjason.pulsar.internal.cache.CacheManager;
import de.syntaxjason.pulsar.internal.cache.CacheMutation;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.logging.NoOpPulsarLogger;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.openjdk.jmh.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms256m", "-Xmx256m", "-Dio.netty.leakDetection.level=disabled"})
public class BroadcastBenchmark {
    public record Message(String text, List<Integer> values) implements Packet { }
    @Param({"1", "16", "128"}) public int recipients;
    private final List<EmbeddedChannel> channels = new ArrayList<>();
    private final List<NettyConnection> connections = new ArrayList<>();
    private Message packet;
    private BenchmarkEndpoint endpoint;

    @Setup
    public void setup() {
        endpoint = new BenchmarkEndpoint();
        packet = new Message("pulsar".repeat(128), List.of(1, 2, 3, 4, 5, 6, 7, 8));
        for (int i = 0; i < recipients; i++) {
            EmbeddedChannel channel = new EmbeddedChannel();
            channels.add(channel);
            connections.add(new NettyConnection(channel, endpoint));
        }
    }

    /** The old per-recipient serialization strategy, with the current serializer. */
    @Benchmark
    public void previous() {
        for (NettyConnection connection : connections) connection.send(packet);
        drain();
    }

    @Benchmark
    public void current() {
        NettyConnection.broadcast(connections, packet);
        drain();
    }

    private void drain() {
        for (EmbeddedChannel channel : channels) {
            ByteBuf buffer = channel.readOutbound();
            buffer.release();
        }
    }

    @TearDown
    public void tearDown() {
        channels.forEach(EmbeddedChannel::finishAndReleaseAll);
        endpoint.caches.close();
    }

    private static final class BenchmarkEndpoint implements InternalEndpoint {
        final PacketRegistry packets = new PacketRegistry().register(Message.class);
        final EventBus events;
        final BinarySerializer serializer;
        final CacheManager caches;
        BenchmarkEndpoint() {
            var bootstrap = PulsarBootstrap.prepare(packets, new SerializerRegistry(), List.of(), List.of(), NoOpPulsarLogger.INSTANCE);
            events = bootstrap.events();
            serializer = bootstrap.serializer();
            caches = new CacheManager(events, true);
        }
        @Override public boolean server() { return true; }
        @Override public EventBus eventBus() { return events; }
        @Override public PacketRegistry packetRegistry() { return packets; }
        @Override public BinarySerializer binarySerializer() { return serializer; }
        @Override public CacheManager cacheManager() { return caches; }
        @Override public PulsarLogger logger() { return NoOpPulsarLogger.INSTANCE; }
        @Override public void connected(NettyConnection connection) { }
        @Override public void disconnected(NettyConnection connection) { }
        @Override public void cacheSubscribe(NettyConnection connection, String cache) { }
        @Override public void cacheMutation(NettyConnection connection, CacheMutation mutation) { }
    }
}
