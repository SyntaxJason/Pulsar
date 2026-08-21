package de.syntaxjason.pulsar.serialization;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class SerializerRegistry {

    private final Map<Class<?>, TypeSerializer<?>> serializers = new ConcurrentHashMap<>();
    private volatile boolean frozen;

    public <T> SerializerRegistry register(Class<T> type, TypeSerializer<T> serializer) {
        ensureMutable();
        if (type == null) {
            throw new IllegalArgumentException("Serializer type must not be null");
        }
        if (serializer == null) {
            throw new IllegalArgumentException("Serializer must not be null");
        }
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

    public Collection<Class<?>> types() {
        return List.copyOf(serializers.keySet());
    }

    public int size() {
        return serializers.size();
    }

    public boolean frozen() {
        return frozen;
    }

    public void freeze() {
        frozen = true;
    }

    private void ensureMutable() {
        if (frozen) {
            throw new IllegalStateException("Pulsar serializer registry is frozen. Register serializers before start/connect.");
        }
    }
}
