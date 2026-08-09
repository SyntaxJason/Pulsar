package de.syntaxjason.pulsar.serialization;

import java.util.UUID;

public interface SerializationWriter {

    void writeBoolean(boolean value);

    void writeByte(byte value);

    void writeShort(short value);

    void writeInt(int value);

    void writeLong(long value);

    void writeFloat(float value);

    void writeDouble(double value);

    void writeChar(char value);

    void writeString(String value);

    void writeUuid(UUID value);

    void writeBytes(byte[] value);
}
