package de.syntaxjason.pulsar.internal.serialization;

import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;
import io.netty.buffer.ByteBuf;

import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class BinarySerializer {

    private static final int MAX_COLLECTION_SIZE = 1_000_000;

    private final SerializerRegistry registry;

    public BinarySerializer(SerializerRegistry registry) {
        this.registry = registry;
    }

    public void write(ByteBuf buffer, Type type, Object value) {
        Class<?> rawType = rawType(type);

        if (!rawType.isPrimitive()) {
            buffer.writeBoolean(value != null);
            if (value == null) {
                return;
            }
        }

        writeNonNull(buffer, type, rawType, value);
    }

    public Object read(ByteBuf buffer, Type type) {
        Class<?> rawType = rawType(type);

        if (!rawType.isPrimitive() && !buffer.readBoolean()) {
            return null;
        }

        return readNonNull(buffer, type, rawType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeNonNull(ByteBuf buffer, Type type, Class<?> rawType, Object value) {
        TypeSerializer custom = registry.serializer(rawType);
        if (custom != null) {
            custom.write(new BinarySerializationWriter(buffer), value);
            return;
        }

        if (rawType == boolean.class || rawType == Boolean.class) {
            buffer.writeBoolean((boolean) value);
            return;
        }
        if (rawType == byte.class || rawType == Byte.class) {
            buffer.writeByte((byte) value);
            return;
        }
        if (rawType == short.class || rawType == Short.class) {
            buffer.writeShort((short) value);
            return;
        }
        if (rawType == int.class || rawType == Integer.class) {
            buffer.writeInt((int) value);
            return;
        }
        if (rawType == long.class || rawType == Long.class) {
            buffer.writeLong((long) value);
            return;
        }
        if (rawType == float.class || rawType == Float.class) {
            buffer.writeFloat((float) value);
            return;
        }
        if (rawType == double.class || rawType == Double.class) {
            buffer.writeDouble((double) value);
            return;
        }
        if (rawType == char.class || rawType == Character.class) {
            buffer.writeChar((char) value);
            return;
        }
        if (rawType == String.class) {
            new BinarySerializationWriter(buffer).writeString((String) value);
            return;
        }
        if (rawType == UUID.class) {
            UUID uuid = (UUID) value;
            buffer.writeLong(uuid.getMostSignificantBits());
            buffer.writeLong(uuid.getLeastSignificantBits());
            return;
        }
        if (rawType == byte[].class) {
            byte[] bytes = (byte[]) value;
            buffer.writeInt(bytes.length);
            buffer.writeBytes(bytes);
            return;
        }
        if (rawType.isEnum()) {
            new BinarySerializationWriter(buffer).writeString(((Enum<?>) value).name());
            return;
        }
        if (rawType == Optional.class) {
            Optional<?> optional = (Optional<?>) value;
            buffer.writeBoolean(optional.isPresent());
            if (optional.isPresent()) {
                write(buffer, typeArgument(type, 0), optional.get());
            }
            return;
        }
        if (List.class.isAssignableFrom(rawType)) {
            List<?> list = (List<?>) value;
            writeCollectionSize(buffer, list.size());
            Type elementType = typeArgument(type, 0);
            for (Object element : list) {
                write(buffer, elementType, element);
            }
            return;
        }
        if (Set.class.isAssignableFrom(rawType)) {
            Set<?> set = (Set<?>) value;
            writeCollectionSize(buffer, set.size());
            Type elementType = typeArgument(type, 0);
            for (Object element : set) {
                write(buffer, elementType, element);
            }
            return;
        }
        if (Map.class.isAssignableFrom(rawType)) {
            Map<?, ?> map = (Map<?, ?>) value;
            writeCollectionSize(buffer, map.size());
            Type keyType = typeArgument(type, 0);
            Type valueType = typeArgument(type, 1);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                write(buffer, keyType, entry.getKey());
                write(buffer, valueType, entry.getValue());
            }
            return;
        }
        if (rawType.isArray()) {
            int length = java.lang.reflect.Array.getLength(value);
            writeCollectionSize(buffer, length);
            Type componentType = rawType.getComponentType();
            for (int i = 0; i < length; i++) {
                write(buffer, componentType, java.lang.reflect.Array.get(value, i));
            }
            return;
        }
        if (rawType.isRecord()) {
            writeRecord(buffer, rawType, value);
            return;
        }

        throw unsupported(type);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object readNonNull(ByteBuf buffer, Type type, Class<?> rawType) {
        TypeSerializer custom = registry.serializer(rawType);
        if (custom != null) {
            return custom.read(new BinarySerializationReader(buffer));
        }

        if (rawType == boolean.class || rawType == Boolean.class) {
            return buffer.readBoolean();
        }
        if (rawType == byte.class || rawType == Byte.class) {
            return buffer.readByte();
        }
        if (rawType == short.class || rawType == Short.class) {
            return buffer.readShort();
        }
        if (rawType == int.class || rawType == Integer.class) {
            return buffer.readInt();
        }
        if (rawType == long.class || rawType == Long.class) {
            return buffer.readLong();
        }
        if (rawType == float.class || rawType == Float.class) {
            return buffer.readFloat();
        }
        if (rawType == double.class || rawType == Double.class) {
            return buffer.readDouble();
        }
        if (rawType == char.class || rawType == Character.class) {
            return buffer.readChar();
        }
        if (rawType == String.class) {
            return readRequiredString(buffer);
        }
        if (rawType == UUID.class) {
            return new UUID(buffer.readLong(), buffer.readLong());
        }
        if (rawType == byte[].class) {
            return new BinarySerializationReader(buffer).readBytes();
        }
        if (rawType.isEnum()) {
            return Enum.valueOf((Class<? extends Enum>) rawType, readRequiredString(buffer));
        }
        if (rawType == Optional.class) {
            if (!buffer.readBoolean()) {
                return Optional.empty();
            }
            return Optional.ofNullable(read(buffer, typeArgument(type, 0)));
        }
        if (List.class.isAssignableFrom(rawType)) {
            int size = readCollectionSize(buffer);
            List<Object> values = new ArrayList<>(size);
            Type elementType = typeArgument(type, 0);
            for (int i = 0; i < size; i++) {
                values.add(read(buffer, elementType));
            }
            return Collections.unmodifiableList(values);
        }
        if (Set.class.isAssignableFrom(rawType)) {
            int size = readCollectionSize(buffer);
            Set<Object> values = new LinkedHashSet<>(capacity(size));
            Type elementType = typeArgument(type, 0);
            for (int i = 0; i < size; i++) {
                values.add(read(buffer, elementType));
            }
            return Collections.unmodifiableSet(values);
        }
        if (Map.class.isAssignableFrom(rawType)) {
            int size = readCollectionSize(buffer);
            Map<Object, Object> values = new LinkedHashMap<>(capacity(size));
            Type keyType = typeArgument(type, 0);
            Type valueType = typeArgument(type, 1);
            for (int i = 0; i < size; i++) {
                values.put(read(buffer, keyType), read(buffer, valueType));
            }
            return Collections.unmodifiableMap(values);
        }
        if (rawType.isArray()) {
            int size = readCollectionSize(buffer);
            Class<?> component = rawType.getComponentType();
            Object array = java.lang.reflect.Array.newInstance(component, size);
            for (int i = 0; i < size; i++) {
                java.lang.reflect.Array.set(array, i, read(buffer, component));
            }
            return array;
        }
        if (rawType.isRecord()) {
            return readRecord(buffer, rawType);
        }

        throw unsupported(type);
    }

    private void writeRecord(ByteBuf buffer, Class<?> type, Object value) {
        for (RecordComponent component : type.getRecordComponents()) {
            try {
                Object componentValue = component.getAccessor().invoke(value);
                write(buffer, component.getGenericType(), componentValue);
            } catch (ReflectiveOperationException exception) {
                throw new SerializationException("Cannot serialize record " + type.getName(), exception);
            }
        }
    }

    private Object readRecord(ByteBuf buffer, Class<?> type) {
        RecordComponent[] components = type.getRecordComponents();
        Class<?>[] parameterTypes = new Class<?>[components.length];
        Object[] values = new Object[components.length];

        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            parameterTypes[i] = component.getType();
            values[i] = read(buffer, component.getGenericType());
        }

        try {
            Constructor<?> constructor = type.getDeclaredConstructor(parameterTypes);
            if (!constructor.trySetAccessible()) {
                throw new SerializationException("Cannot access canonical constructor of " + type.getName());
            }
            return constructor.newInstance(values);
        } catch (ReflectiveOperationException exception) {
            throw new SerializationException("Cannot deserialize record " + type.getName(), exception);
        }
    }

    private String readRequiredString(ByteBuf buffer) {
        String value = new BinarySerializationReader(buffer).readString();
        if (value == null) {
            throw new SerializationException("Unexpected null string");
        }
        return value;
    }

    private void writeCollectionSize(ByteBuf buffer, int size) {
        if (size < 0 || size > MAX_COLLECTION_SIZE) {
            throw new SerializationException("Collection is too large: " + size);
        }
        buffer.writeInt(size);
    }

    private int readCollectionSize(ByteBuf buffer) {
        int size = buffer.readInt();
        if (size < 0 || size > MAX_COLLECTION_SIZE) {
            throw new SerializationException("Invalid collection size: " + size);
        }
        return size;
    }

    private int capacity(int size) {
        if (size < 3) {
            return size + 1;
        }
        return Math.min(Integer.MAX_VALUE, (int) ((float) size / 0.75f) + 1);
    }

    private Type typeArgument(Type type, int index) {
        if (!(type instanceof ParameterizedType parameterized)) {
            throw new SerializationException("Generic type information is required for " + type.getTypeName());
        }
        return parameterized.getActualTypeArguments()[index];
    }

    private Class<?> rawType(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof GenericArrayType arrayType) {
            Class<?> component = rawType(arrayType.getGenericComponentType());
            return java.lang.reflect.Array.newInstance(component, 0).getClass();
        }
        throw unsupported(type);
    }

    private SerializationException unsupported(Type type) {
        return new SerializationException("Unsupported type: " + type.getTypeName() + ". Use a record or register a TypeSerializer.");
    }
}
