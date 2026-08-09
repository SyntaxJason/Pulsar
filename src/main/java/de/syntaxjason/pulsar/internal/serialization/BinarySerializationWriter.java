package de.syntaxjason.pulsar.internal.serialization;

import de.syntaxjason.pulsar.serialization.SerializationWriter;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

final class BinarySerializationWriter implements SerializationWriter {

    private final ByteBuf buffer;

    BinarySerializationWriter(ByteBuf buffer) {
        this.buffer = buffer;
    }

    @Override
    public void writeBoolean(boolean value) {
        buffer.writeBoolean(value);
    }

    @Override
    public void writeByte(byte value) {
        buffer.writeByte(value);
    }

    @Override
    public void writeShort(short value) {
        buffer.writeShort(value);
    }

    @Override
    public void writeInt(int value) {
        buffer.writeInt(value);
    }

    @Override
    public void writeLong(long value) {
        buffer.writeLong(value);
    }

    @Override
    public void writeFloat(float value) {
        buffer.writeFloat(value);
    }

    @Override
    public void writeDouble(double value) {
        buffer.writeDouble(value);
    }

    @Override
    public void writeChar(char value) {
        buffer.writeChar(value);
    }

    @Override
    public void writeString(String value) {
        if (value == null) {
            buffer.writeInt(-1);
            return;
        }

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
    }

    @Override
    public void writeUuid(UUID value) {
        if (value == null) {
            buffer.writeBoolean(false);
            return;
        }

        buffer.writeBoolean(true);
        buffer.writeLong(value.getMostSignificantBits());
        buffer.writeLong(value.getLeastSignificantBits());
    }

    @Override
    public void writeBytes(byte[] value) {
        if (value == null) {
            buffer.writeInt(-1);
            return;
        }

        buffer.writeInt(value.length);
        buffer.writeBytes(value);
    }
}
