package de.syntaxjason.pulsar.internal.serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.openjdk.jmh.annotations.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgsAppend = {"-Xms256m", "-Xmx256m"})
public class StringDecodingBenchmark {
    @Param({"32", "1024", "65536"}) public int size;
    @Param({"heap", "direct"}) public String storage;
    private ByteBuf buffer;

    @Setup
    public void setup() {
        byte[] text = "x".repeat(size).getBytes(StandardCharsets.UTF_8);
        buffer = storage.equals("heap") ? Unpooled.buffer(size + 4) : Unpooled.directBuffer(size + 4);
        buffer.writeInt(text.length).writeBytes(text);
    }

    /** The 0.2.0-BETA custom-reader algorithm, including its intermediate byte array. */
    @Benchmark
    public String previous() {
        buffer.readerIndex(0);
        int length = buffer.readInt();
        if (length < 0 || length > 1_048_576 || length > buffer.readableBytes()) throw new AssertionError();
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Benchmark
    public String current() {
        buffer.readerIndex(0);
        return new BinarySerializationReader(buffer).readString();
    }

    @TearDown
    public void tearDown() { buffer.release(); }
}
