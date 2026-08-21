package de.syntaxjason.pulsar.internal.serialization;

import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;
import de.syntaxjason.pulsar.serialization.TypeSerializer;
import io.netty.buffer.ByteBuf;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public final class SerializerCompiler {

    private static final int MAX_COLLECTION_SIZE = 1_000_000;
    private static final int MAX_STRING_BYTES = 1_048_576;
    private static final int MAX_BYTE_ARRAY = 16_777_216;

    private final SerializerRegistry registry;
    private final PulsarLogger logger;
    private final Map<Type, RuntimeCodec> codecs = new HashMap<>();

    public SerializerCompiler(SerializerRegistry registry, PulsarLogger logger) {
        this.registry = registry;
        this.logger = logger;
        registerBuiltins();
        registry.types().forEach(this::compile);
    }

    public void compile(Type type) {
        if (codecs.containsKey(type)) {
            return;
        }

        DeferredCodec deferred = new DeferredCodec(type);
        codecs.put(type, deferred);

        try {
            RuntimeCodec codec = build(type);
            deferred.delegate(codec);
            codecs.put(type, codec);
            logger.debug(LogCategory.SERIALIZATION, "Compiled codec plan for " + type.getTypeName());
        } catch (RuntimeException exception) {
            codecs.remove(type);
            throw exception;
        }
    }

    public BinarySerializer freeze() {
        codecs.values().forEach(codec -> {
            if (codec instanceof DeferredCodec deferred && !deferred.ready()) {
                throw new SerializationException("Unresolved recursive codec plan for " + deferred.type().getTypeName());
            }
        });
        return new BinarySerializer(codecs);
    }

    public int size() {
        return codecs.size();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RuntimeCodec build(Type type) {
        Class<?> rawType = rawType(type);
        TypeSerializer custom = registry.serializer(rawType);
        if (custom != null) {
            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object value) {
                    custom.write(new BinarySerializationWriter(buffer), value);
                }

                @Override
                public Object read(ByteBuf buffer) {
                    return custom.read(new BinarySerializationReader(buffer));
                }
            });
        }

        if (rawType.isEnum()) {
            Map<String, Object> constants = new HashMap<>();
            for (Object constant : rawType.getEnumConstants()) {
                constants.put(((Enum<?>) constant).name(), constant);
            }
            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object value) {
                    writeString(buffer, ((Enum<?>) value).name());
                }

                @Override
                public Object read(ByteBuf buffer) {
                    String name = readString(buffer);
                    Object value = constants.get(name);
                    if (value == null) {
                        throw new SerializationException("Unknown " + rawType.getName() + " enum constant: " + name);
                    }
                    return value;
                }
            });
        }

        if (rawType == Optional.class) {
            RuntimeCodec element = child(typeArgument(type, 0));
            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object value) {
                    Optional<?> optional = (Optional<?>) value;
                    buffer.writeBoolean(optional.isPresent());
                    optional.ifPresent(elementValue -> element.write(buffer, elementValue));
                }

                @Override
                public Object read(ByteBuf buffer) {
                    return buffer.readBoolean() ? Optional.ofNullable(element.read(buffer)) : Optional.empty();
                }
            });
        }

        if (List.class.isAssignableFrom(rawType)) {
            RuntimeCodec element = child(typeArgument(type, 0));
            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object value) {
                    List<?> list = (List<?>) value;
                    writeCollectionSize(buffer, list.size());
                    list.forEach(elementValue -> element.write(buffer, elementValue));
                }

                @Override
                public Object read(ByteBuf buffer) {
                    int size = readCollectionSize(buffer);
                    List<Object> values = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        values.add(element.read(buffer));
                    }
                    return Collections.unmodifiableList(values);
                }
            });
        }

        if (Set.class.isAssignableFrom(rawType)) {
            RuntimeCodec element = child(typeArgument(type, 0));
            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object value) {
                    Set<?> set = (Set<?>) value;
                    writeCollectionSize(buffer, set.size());
                    set.forEach(elementValue -> element.write(buffer, elementValue));
                }

                @Override
                public Object read(ByteBuf buffer) {
                    int size = readCollectionSize(buffer);
                    Set<Object> values = new LinkedHashSet<>(capacity(size));
                    for (int i = 0; i < size; i++) {
                        values.add(element.read(buffer));
                    }
                    return Collections.unmodifiableSet(values);
                }
            });
        }

        if (Map.class.isAssignableFrom(rawType)) {
            RuntimeCodec key = child(typeArgument(type, 0));
            RuntimeCodec value = child(typeArgument(type, 1));
            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object input) {
                    Map<?, ?> map = (Map<?, ?>) input;
                    writeCollectionSize(buffer, map.size());
                    map.forEach((mapKey, mapValue) -> {
                        key.write(buffer, mapKey);
                        value.write(buffer, mapValue);
                    });
                }

                @Override
                public Object read(ByteBuf buffer) {
                    int size = readCollectionSize(buffer);
                    Map<Object, Object> values = new LinkedHashMap<>(capacity(size));
                    for (int i = 0; i < size; i++) {
                        values.put(key.read(buffer), value.read(buffer));
                    }
                    return Collections.unmodifiableMap(values);
                }
            });
        }

        if (rawType.isArray()) {
            return compileArray(rawType, componentType(type, rawType));
        }

        if (rawType.isRecord()) {
            return compileRecord(rawType);
        }

        throw unsupported(type);
    }

    private RuntimeCodec compileRecord(Class<?> type) {
        RecordComponent[] components = type.getRecordComponents();
        RuntimeCodec[] componentCodecs = new RuntimeCodec[components.length];
        MethodHandle[] getters = new MethodHandle[components.length];
        Class<?>[] parameterTypes = new Class<?>[components.length];

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();

            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                componentCodecs[i] = child(component.getGenericType());
                parameterTypes[i] = component.getType();

                Method accessor = component.getAccessor();
                if (!accessor.trySetAccessible()) {
                    throw new SerializationException("Cannot access record accessor " + accessor);
                }
                getters[i] = lookup.unreflect(accessor)
                        .asType(MethodType.methodType(Object.class, Object.class));
            }

            Constructor<?> constructor = type.getDeclaredConstructor(parameterTypes);
            if (!constructor.trySetAccessible()) {
                throw new SerializationException("Cannot access canonical constructor of " + type.getName());
            }
            MethodHandle constructorHandle = lookup.unreflectConstructor(constructor)
                    .asSpreader(Object[].class, components.length)
                    .asType(MethodType.methodType(Object.class, Object[].class));

            return nullable(type, new RecordRuntimeCodec(type, componentCodecs, getters, constructorHandle));
        } catch (ReflectiveOperationException exception) {
            throw new SerializationException("Cannot compile record codec for " + type.getName(), exception);
        }
    }

    private RuntimeCodec compileArray(Class<?> rawType, Type componentType) {
        RuntimeCodec component = child(componentType);

        try {
            MethodHandle constructor = MethodHandles.arrayConstructor(rawType)
                    .asType(MethodType.methodType(Object.class, int.class));
            MethodHandle getter = MethodHandles.arrayElementGetter(rawType)
                    .asType(MethodType.methodType(Object.class, Object.class, int.class));
            MethodHandle setter = MethodHandles.arrayElementSetter(rawType)
                    .asType(MethodType.methodType(void.class, Object.class, int.class, Object.class));
            MethodHandle length = MethodHandles.arrayLength(rawType)
                    .asType(MethodType.methodType(int.class, Object.class));

            return nullable(rawType, new RuntimeCodec() {
                @Override
                public void write(ByteBuf buffer, Object value) {
                    int size;
                    try {
                        size = (int) length.invokeExact(value);
                    } catch (Throwable throwable) {
                        throw new SerializationException("Cannot read array length for " + rawType.getName(), throwable);
                    }

                    writeCollectionSize(buffer, size);
                    for (int i = 0; i < size; i++) {
                        try {
                            component.write(buffer, getter.invokeExact(value, i));
                        } catch (Throwable throwable) {
                            throw new SerializationException("Cannot serialize array " + rawType.getName(), throwable);
                        }
                    }
                }

                @Override
                public Object read(ByteBuf buffer) {
                    int size = readCollectionSize(buffer);
                    try {
                        Object array = constructor.invokeExact(size);
                        for (int i = 0; i < size; i++) {
                            setter.invokeExact(array, i, component.read(buffer));
                        }
                        return array;
                    } catch (Throwable throwable) {
                        throw new SerializationException("Cannot deserialize array " + rawType.getName(), throwable);
                    }
                }
            });
        } catch (IllegalArgumentException exception) {
            throw new SerializationException("Cannot compile array codec for " + rawType.getName(), exception);
        }
    }

    private RuntimeCodec child(Type type) {
        RuntimeCodec existing = codecs.get(type);
        if (existing != null) {
            return existing;
        }
        compile(type);
        return codecs.get(type);
    }

    private RuntimeCodec nullable(Class<?> rawType, RuntimeCodec delegate) {
        if (rawType.isPrimitive()) {
            return delegate;
        }
        return new RuntimeCodec() {
            @Override
            public void write(ByteBuf buffer, Object value) {
                buffer.writeBoolean(value != null);
                if (value != null) {
                    delegate.write(buffer, value);
                }
            }

            @Override
            public Object read(ByteBuf buffer) {
                return buffer.readBoolean() ? delegate.read(buffer) : null;
            }
        };
    }

    private void registerBuiltins() {
        codecs.put(boolean.class, primitive((buffer, value) -> buffer.writeBoolean((boolean) value), ByteBuf::readBoolean));
        codecs.put(Boolean.class, nullable(Boolean.class, primitive((buffer, value) -> buffer.writeBoolean((boolean) value), ByteBuf::readBoolean)));
        codecs.put(byte.class, primitive((buffer, value) -> buffer.writeByte((byte) value), ByteBuf::readByte));
        codecs.put(Byte.class, nullable(Byte.class, primitive((buffer, value) -> buffer.writeByte((byte) value), ByteBuf::readByte)));
        codecs.put(short.class, primitive((buffer, value) -> buffer.writeShort((short) value), ByteBuf::readShort));
        codecs.put(Short.class, nullable(Short.class, primitive((buffer, value) -> buffer.writeShort((short) value), ByteBuf::readShort)));
        codecs.put(int.class, primitive((buffer, value) -> buffer.writeInt((int) value), ByteBuf::readInt));
        codecs.put(Integer.class, nullable(Integer.class, primitive((buffer, value) -> buffer.writeInt((int) value), ByteBuf::readInt)));
        codecs.put(long.class, primitive((buffer, value) -> buffer.writeLong((long) value), ByteBuf::readLong));
        codecs.put(Long.class, nullable(Long.class, primitive((buffer, value) -> buffer.writeLong((long) value), ByteBuf::readLong)));
        codecs.put(float.class, primitive((buffer, value) -> buffer.writeFloat((float) value), ByteBuf::readFloat));
        codecs.put(Float.class, nullable(Float.class, primitive((buffer, value) -> buffer.writeFloat((float) value), ByteBuf::readFloat)));
        codecs.put(double.class, primitive((buffer, value) -> buffer.writeDouble((double) value), ByteBuf::readDouble));
        codecs.put(Double.class, nullable(Double.class, primitive((buffer, value) -> buffer.writeDouble((double) value), ByteBuf::readDouble)));
        codecs.put(char.class, primitive((buffer, value) -> buffer.writeChar((char) value), ByteBuf::readChar));
        codecs.put(Character.class, nullable(Character.class, primitive((buffer, value) -> buffer.writeChar((char) value), ByteBuf::readChar)));

        codecs.put(String.class, nullable(String.class, new RuntimeCodec() {
            @Override
            public void write(ByteBuf buffer, Object value) {
                writeString(buffer, (String) value);
            }

            @Override
            public Object read(ByteBuf buffer) {
                return readString(buffer);
            }
        }));

        codecs.put(UUID.class, nullable(UUID.class, new RuntimeCodec() {
            @Override
            public void write(ByteBuf buffer, Object value) {
                UUID uuid = (UUID) value;
                buffer.writeLong(uuid.getMostSignificantBits());
                buffer.writeLong(uuid.getLeastSignificantBits());
            }

            @Override
            public Object read(ByteBuf buffer) {
                return new UUID(buffer.readLong(), buffer.readLong());
            }
        }));

        codecs.put(byte[].class, nullable(byte[].class, new RuntimeCodec() {
            @Override
            public void write(ByteBuf buffer, Object value) {
                byte[] bytes = (byte[]) value;
                if (bytes.length > MAX_BYTE_ARRAY) {
                    throw new SerializationException("Byte array is too large: " + bytes.length);
                }
                buffer.writeInt(bytes.length);
                buffer.writeBytes(bytes);
            }

            @Override
            public Object read(ByteBuf buffer) {
                int length = buffer.readInt();
                validateLength(buffer, length, MAX_BYTE_ARRAY, "byte array");
                byte[] bytes = new byte[length];
                buffer.readBytes(bytes);
                return bytes;
            }
        }));
    }

    private RuntimeCodec primitive(Writer writer, Reader reader) {
        return new RuntimeCodec() {
            @Override
            public void write(ByteBuf buffer, Object value) {
                writer.write(buffer, value);
            }

            @Override
            public Object read(ByteBuf buffer) {
                return reader.read(buffer);
            }
        };
    }

    private Type typeArgument(Type type, int index) {
        if (!(type instanceof ParameterizedType parameterized)) {
            throw new SerializationException("Generic type information is required for " + type.getTypeName());
        }
        return parameterized.getActualTypeArguments()[index];
    }

    private Type componentType(Type type, Class<?> rawType) {
        if (type instanceof GenericArrayType genericArray) {
            return genericArray.getGenericComponentType();
        }
        return rawType.getComponentType();
    }

    private Class<?> rawType(Type type) {
        if (type instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof ParameterizedType parameterized && parameterized.getRawType() instanceof Class<?> clazz) {
            return clazz;
        }
        if (type instanceof GenericArrayType genericArray) {
            Type component = genericArray.getGenericComponentType();
            Class<?> rawComponent = rawType(component);
            try {
                return MethodHandles.arrayConstructor(rawComponent.arrayType()).type().returnType();
            } catch (IllegalArgumentException exception) {
                throw unsupported(type);
            }
        }
        throw unsupported(type);
    }

    private void writeString(ByteBuf buffer, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new SerializationException("String is too large: " + bytes.length + " bytes");
        }
        buffer.writeInt(bytes.length);
        buffer.writeBytes(bytes);
    }

    private String readString(ByteBuf buffer) {
        int length = buffer.readInt();
        validateLength(buffer, length, MAX_STRING_BYTES, "string");
        byte[] bytes = new byte[length];
        buffer.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
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

    private void validateLength(ByteBuf buffer, int length, int max, String name) {
        if (length < 0 || length > max || length > buffer.readableBytes()) {
            throw new SerializationException("Invalid " + name + " length: " + length);
        }
    }

    private int capacity(int size) {
        if (size < 3) {
            return size + 1;
        }
        return Math.min(Integer.MAX_VALUE, (int) ((float) size / 0.75f) + 1);
    }

    private SerializationException unsupported(Type type) {
        return new SerializationException(
                "Unsupported type: " + type.getTypeName()
                        + ". Use a record, register a TypeSerializer, or register a supported generic type."
        );
    }

    private static final class RecordRuntimeCodec implements RuntimeCodec {

        private final Class<?> type;
        private final RuntimeCodec[] codecs;
        private final MethodHandle[] getters;
        private final MethodHandle constructor;

        private RecordRuntimeCodec(
                Class<?> type,
                RuntimeCodec[] codecs,
                MethodHandle[] getters,
                MethodHandle constructor
        ) {
            this.type = type;
            this.codecs = codecs;
            this.getters = getters;
            this.constructor = constructor;
        }

        @Override
        public void write(ByteBuf buffer, Object value) {
            try {
                for (int i = 0; i < codecs.length; i++) {
                    Object component = getters[i].invokeExact(value);
                    codecs[i].write(buffer, component);
                }
            } catch (Throwable throwable) {
                throw new SerializationException("Cannot serialize record " + type.getName(), throwable);
            }
        }

        @Override
        public Object read(ByteBuf buffer) {
            Object[] values = new Object[codecs.length];
            for (int i = 0; i < codecs.length; i++) {
                values[i] = codecs[i].read(buffer);
            }

            try {
                return constructor.invokeExact(values);
            } catch (Throwable throwable) {
                throw new SerializationException("Cannot deserialize record " + type.getName(), throwable);
            }
        }
    }

    private static final class DeferredCodec implements RuntimeCodec {

        private final Type type;
        private RuntimeCodec delegate;

        private DeferredCodec(Type type) {
            this.type = type;
        }

        private void delegate(RuntimeCodec delegate) {
            this.delegate = delegate;
        }

        private boolean ready() {
            return delegate != null;
        }

        private Type type() {
            return type;
        }

        @Override
        public void write(ByteBuf buffer, Object value) {
            RuntimeCodec current = delegate;
            if (current == null) {
                throw new SerializationException("Codec plan for " + type.getTypeName() + " is not ready");
            }
            current.write(buffer, value);
        }

        @Override
        public Object read(ByteBuf buffer) {
            RuntimeCodec current = delegate;
            if (current == null) {
                throw new SerializationException("Codec plan for " + type.getTypeName() + " is not ready");
            }
            return current.read(buffer);
        }
    }

    @FunctionalInterface
    private interface Writer {
        void write(ByteBuf buffer, Object value);
    }

    @FunctionalInterface
    private interface Reader {
        Object read(ByteBuf buffer);
    }
}
