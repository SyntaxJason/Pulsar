package de.syntaxjason.pulsar.packet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class PacketRegistry {

    private final Map<String, Class<? extends Packet>> byId = new ConcurrentHashMap<>();
    private final Map<Class<? extends Packet>, String> byType = new ConcurrentHashMap<>();

    public PacketRegistry register(Class<? extends Packet> type) {
        String id = resolveId(type);
        Class<? extends Packet> existing = byId.putIfAbsent(id, type);

        if (existing != null && existing != type) {
            throw new IllegalArgumentException("Packet id '" + id + "' is already used by " + existing.getName());
        }

        byType.put(type, id);
        return this;
    }

    @SafeVarargs
    public final PacketRegistry register(Class<? extends Packet>... types) {
        for (Class<? extends Packet> type : types) {
            register(type);
        }
        return this;
    }

    public Class<? extends Packet> type(String id) {
        if (id == null || id.isBlank() || id.length() > 512) {
            return null;
        }

        Class<? extends Packet> known = byId.get(id);
        if (known != null) {
            return known;
        }
        return resolveClassNamedPacket(id);
    }

    public String id(Class<? extends Packet> type) {
        String id = byType.get(type);
        if (id != null) {
            return id;
        }

        register(type);
        return byType.get(type);
    }

    public boolean contains(Class<? extends Packet> type) {
        return byType.containsKey(type);
    }

    public Collection<Class<? extends Packet>> types() {
        return List.copyOf(byType.keySet());
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Packet> resolveClassNamedPacket(String id) {
        try {
            ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
            ClassLoader loader = contextLoader == null ? PacketRegistry.class.getClassLoader() : contextLoader;
            Class<?> loaded = Class.forName(id, false, loader);

            if (!Packet.class.isAssignableFrom(loaded)) {
                return null;
            }

            Class<? extends Packet> packetType = (Class<? extends Packet>) loaded;
            if (!resolveId(packetType).equals(id)) {
                return null;
            }

            register(packetType);
            return packetType;
        } catch (ClassNotFoundException | LinkageError exception) {
            return null;
        }
    }

    private String resolveId(Class<? extends Packet> type) {
        PacketType annotation = type.getAnnotation(PacketType.class);
        if (annotation == null) {
            return type.getName();
        }

        String value = annotation.value().trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("@PacketType value must not be blank: " + type.getName());
        }
        return value;
    }
}
