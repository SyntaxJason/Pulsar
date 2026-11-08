package de.syntaxjason.pulsar.internal.serialization;

import de.syntaxjason.pulsar.logging.NoOpPulsarLogger;
import de.syntaxjason.pulsar.serialization.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SerializationTest {
    enum Kind { FIRST, SECOND }
    record Model(String text, UUID id, Kind kind, Integer boxed, Optional<String> optional,
                 List<String> list, Set<Integer> set, Map<String, Long> map, int[] numbers, byte[] bytes) { }
    record Node(String name, Node child) { }
    record Custom(String value) { }

    @Test
    void recordsCollectionsArraysNullsAndRecursiveTypesRoundTrip() {
        SerializerCompiler compiler = compiler(new SerializerRegistry());
        compiler.compile(Model.class);
        compiler.compile(Node.class);
        BinarySerializer serializer = compiler.freeze();
        Model model = new Model("Grüße 🌌", UUID.randomUUID(), Kind.SECOND, null,
                Optional.of("yes"), Arrays.asList("one", null, "two"), Set.of(1, 2),
                Map.of("count", 42L), new int[]{1, -2, Integer.MAX_VALUE}, new byte[]{0, -1, 2});
        ByteBuf buffer = Unpooled.buffer();
        try {
            serializer.write(buffer, Model.class, model);
            Model result = (Model) serializer.read(buffer, Model.class);
            assertEquals(model.text(), result.text());
            assertEquals(model.id(), result.id());
            assertEquals(model.kind(), result.kind());
            assertNull(result.boxed());
            assertEquals(model.optional(), result.optional());
            assertEquals(model.list(), result.list());
            assertEquals(model.set(), result.set());
            assertEquals(model.map(), result.map());
            assertArrayEquals(model.numbers(), result.numbers());
            assertArrayEquals(model.bytes(), result.bytes());
            assertThrows(UnsupportedOperationException.class, () -> result.list().add("immutable"));
            Node tree = new Node("root", new Node("leaf", null));
            serializer.write(buffer, Node.class, tree);
            assertEquals(tree, serializer.read(buffer, Node.class));
            assertFalse(buffer.isReadable());
        } finally { buffer.release(); }
    }

    @Test
    void customBuiltInOverridesAreUsedInsideNestedRecords() {
        AtomicInteger writes = new AtomicInteger();
        SerializerRegistry registry = new SerializerRegistry().register(String.class, new TypeSerializer<>() {
            @Override public void write(SerializationWriter writer, String value) {
                writes.incrementAndGet();
                writer.writeString("custom:" + value);
            }
            @Override public String read(SerializationReader reader) { return reader.readString().substring(7); }
        });
        SerializerCompiler compiler = compiler(registry);
        compiler.compile(Custom.class);
        BinarySerializer serializer = compiler.freeze();
        ByteBuf buffer = Unpooled.buffer();
        try {
            serializer.write(buffer, Custom.class, new Custom("value"));
            assertEquals(1, writes.get());
            assertEquals(new Custom("value"), serializer.read(buffer, Custom.class));
        } finally { buffer.release(); }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ascii", "Grüße", "漢字", "🌌🚀", "\uD800", "\uDC00", "\uD800漢"})
    void stringWireFormatRemainsCompatibleOnHeapAndDirectBuffers(String text) {
        for (boolean direct : List.of(false, true)) {
            ByteBuf buffer = direct ? Unpooled.directBuffer() : Unpooled.buffer();
            try {
                byte[] expected = text.getBytes(StandardCharsets.UTF_8);
                new BinarySerializationWriter(buffer).writeString(text);
                assertEquals(expected.length, buffer.getInt(0));
                byte[] actual = new byte[expected.length];
                buffer.getBytes(4, actual);
                assertArrayEquals(expected, actual);
                assertEquals(new String(expected, StandardCharsets.UTF_8), new BinarySerializationReader(buffer).readString());
                assertFalse(buffer.isReadable());
            } finally { buffer.release(); }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {-2, 100, 1_048_577, Integer.MAX_VALUE})
    void invalidStringLengthsAreRejected(int length) {
        ByteBuf buffer = Unpooled.buffer().writeInt(length);
        try {
            assertThrows(SerializationException.class, () -> new BinarySerializationReader(buffer).readString());
            buffer.readerIndex(0);
            buffer.setInt(0, length);
            BinarySerializer serializer = compiler(new SerializerRegistry()).freeze();
            ByteBuf builtin = Unpooled.buffer().writeBoolean(true).writeInt(length);
            try { assertThrows(SerializationException.class, () -> serializer.read(builtin, String.class)); }
            finally { builtin.release(); }
        } finally { buffer.release(); }
    }

    @Test
    void customSerializerWriterCannotEmitDataThatItsReaderRejects() {
        ByteBuf buffer = Unpooled.buffer();
        try {
            assertThrows(SerializationException.class, () -> new BinarySerializationWriter(buffer).writeString("x".repeat(1_048_577)));
            assertThrows(SerializationException.class, () -> new BinarySerializationWriter(buffer).writeBytes(new byte[16_777_217]));
        } finally { buffer.release(); }
    }

    @Test
    void unregisteredAndUnsupportedTypesFailBeforeNetworking() {
        SerializerCompiler compiler = compiler(new SerializerRegistry());
        assertThrows(SerializationException.class, () -> compiler.compile(Thread.class));
        BinarySerializer serializer = compiler.freeze();
        ByteBuf buffer = Unpooled.buffer();
        try { assertThrows(SerializationException.class, () -> serializer.write(buffer, Node.class, new Node("unregistered", null))); }
        finally { buffer.release(); }
    }

    private static SerializerCompiler compiler(SerializerRegistry registry) {
        return new SerializerCompiler(registry, NoOpPulsarLogger.INSTANCE);
    }
}
