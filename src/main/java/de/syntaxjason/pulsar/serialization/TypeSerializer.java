package de.syntaxjason.pulsar.serialization;

public interface TypeSerializer<T> {

    void write(SerializationWriter writer, T value);

    T read(SerializationReader reader);
}
