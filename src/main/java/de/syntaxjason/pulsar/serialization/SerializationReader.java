package de.syntaxjason.pulsar.serialization;

import java.util.UUID;

public interface SerializationReader {

    boolean readBoolean();

    byte readByte();

    short readShort();

    int readInt();

    long readLong();

    float readFloat();

    double readDouble();

    char readChar();

    String readString();

    UUID readUuid();

    byte[] readBytes();
}
