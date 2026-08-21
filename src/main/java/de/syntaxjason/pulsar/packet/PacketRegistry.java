package de.syntaxjason.pulsar.packet;

import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.PulsarLogger;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class PacketRegistry {

    private final Map<Integer, Class<? extends Packet>> byWireId = new ConcurrentHashMap<>();
    private final Map<Class<? extends Packet>, PacketDefinition> byType = new ConcurrentHashMap<>();
    private volatile boolean frozen;
    private volatile PulsarLogger logger;

    public PacketRegistry() {
        this(null);
    }

    public PacketRegistry(PulsarLogger logger) {
        this.logger = logger;
    }

    public PacketRegistry logger(PulsarLogger logger) {
        ensureMutable();
        this.logger = logger;
        return this;
    }

    public PacketRegistry register(Class<? extends Packet> type) {
        ensureMutable();
        if (type == null) {
            throw new IllegalArgumentException("Packet type must not be null");
        }
        if (byType.containsKey(type)) {
            return this;
        }

        String logicalId = resolveLogicalId(type);
        int wireId = wireId(logicalId);
        Class<? extends Packet> existing = byWireId.putIfAbsent(wireId, type);

        if (existing != null && existing != type) {
            String existingId = byType.get(existing) == null
                    ? resolveLogicalId(existing)
                    : byType.get(existing).logicalId();
            throw new IllegalArgumentException(
                    "Packet wire id collision " + Integer.toUnsignedString(wireId)
                            + " between '" + existingId + "' (" + existing.getName() + ") and '"
                            + logicalId + "' (" + type.getName() + "). Use distinct @PacketType values."
            );
        }

        byType.put(type, new PacketDefinition(wireId, logicalId, type));
        logDebug("Registered packet " + Integer.toUnsignedString(wireId) + " -> " + logicalId
                + " (" + type.getName() + ")");
        return this;
    }

    @SafeVarargs
    public final PacketRegistry register(Class<? extends Packet>... types) {
        for (Class<? extends Packet> type : types) {
            register(type);
        }
        return this;
    }

    public Class<? extends Packet> type(int wireId) {
        return byWireId.get(wireId);
    }

    public int wireId(Class<? extends Packet> type) {
        PacketDefinition definition = byType.get(type);
        if (definition == null) {
            throw new IllegalStateException(
                    "Packet " + type.getName() + " is not registered. Register packets before start/connect."
            );
        }
        return definition.wireId();
    }

    public String id(Class<? extends Packet> type) {
        PacketDefinition definition = byType.get(type);
        if (definition == null) {
            throw new IllegalStateException(
                    "Packet " + type.getName() + " is not registered. Register packets before start/connect."
            );
        }
        return definition.logicalId();
    }

    public boolean contains(Class<? extends Packet> type) {
        return byType.containsKey(type);
    }

    public Collection<Class<? extends Packet>> types() {
        return List.copyOf(byType.keySet());
    }

    public int size() {
        return byType.size();
    }

    public boolean frozen() {
        return frozen;
    }

    public void freeze() {
        frozen = true;
        logDebug("Packet registry frozen with " + byType.size() + " packets");
    }

    public static int wireId(String logicalId) {
        byte[] bytes = logicalId.getBytes(StandardCharsets.UTF_8);
        int hash = 0x811C9DC5;
        for (byte value : bytes) {
            hash ^= value & 0xFF;
            hash *= 0x01000193;
        }
        return hash;
    }

    private String resolveLogicalId(Class<? extends Packet> type) {
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

    private void ensureMutable() {
        if (frozen) {
            throw new IllegalStateException("Pulsar packet registry is frozen. Register packets before start/connect.");
        }
    }

    private void logDebug(String message) {
        PulsarLogger current = logger;
        if (current != null) {
            current.debug(LogCategory.REGISTRY, message);
        }
    }

    private record PacketDefinition(
            int wireId,
            String logicalId,
            Class<? extends Packet> type
    ) {
    }
}
