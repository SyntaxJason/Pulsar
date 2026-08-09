package de.syntaxjason.pulsar.serialization;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SerializerRegistry {

    private final Map<Class<?>, TypeSerializer<?>> serializers = new ConcurrentHashMap<>();

    public <T> SerializerRegistry register(Class<T> type, TypeSerializer<T> serializer) {
        serializers.put(type, serializer);
        return this;
    }

    @SuppressWarnings("unchecked")
    public <T> TypeSerializer<T> serializer(Class<T> type) {
        return (TypeSerializer<T>) serializers.get(type);
    }

    public boolean contains(Class<?> type) {
        return serializers.containsKey(type);
    }
}
