package de.syntaxjason.pulsar.internal.serialization;

final class SerializationLimits {
    static final int MAX_COLLECTION_SIZE = 1_000_000;
    static final int MAX_STRING_BYTES = 1_048_576;
    static final int MAX_BYTE_ARRAY = 16_777_216;

    private SerializationLimits() { }
}
