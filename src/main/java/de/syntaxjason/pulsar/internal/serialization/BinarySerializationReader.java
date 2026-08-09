package de.syntaxjason.pulsar.internal.serialization;

import de.syntaxjason.pulsar.serialization.SerializationReader;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

final class BinarySerializationReader implements SerializationReader {

    private static final int MAX_STRING_BYTES = 1_048_576;
    private static final int MAX_BYTE_ARRAY = 16_777_216;

    private final ByteBuf buffer;

    BinarySerializationReader(ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean readBoolean() {
        return buffer.readBoolean();
    }

    @Override
    public byte readByte() {
        return buffer.readByte();
    }

    @Override
    public short readShort() {
        return buffer.readShort();
    }

    @Override
    public int readInt() {
        return buffer.readInt();
    }

    @Override
    public long readLong() {
        return buffer.readLong();
    }

    @Override
    public float readFloat() {
        return buffer.readFloat();
    }

    @Override
    public double readDouble() {
        return buffer.readDouble();
    }

    @Override
    public char readChar() {
        return buffer.readChar();
    }

    @Override
    public String readString() {
        int length = buffer.readInt();
        if (length == -1) {
            return null;
        }
        validateLength(length, MAX_STRING_BYTES, "string");

        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public UUID readUuid() {
        if (!buffer.readBoolean()) {
            return null;
        }
        return new UUID(buffer.readLong(), buffer.readLong());
    }

    @Override
    public byte[] readBytes() {
        int length = buffer.readInt();
        if (length == -1) {
            return null;
        }
        validateLength(length, MAX_BYTE_ARRAY, "byte array");

        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return bytes;
    }

    private void validateLength(int length, int max, String name) {
        if (length < 0 || length > max || length > buffer.readableBytes()) {
            throw new SerializationException("Invalid " + name + " length: " + length);
        }
    }
}
