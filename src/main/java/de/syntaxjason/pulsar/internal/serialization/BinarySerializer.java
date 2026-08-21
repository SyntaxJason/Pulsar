package de.syntaxjason.pulsar.internal.serialization;

import io.netty.buffer.ByteBuf;

import java.lang.reflect.Type;
import java.util.Map;

public final class BinarySerializer {

    private final Map<Type, RuntimeCodec> codecs;

    BinarySerializer(Map<Type, RuntimeCodec> codecs) {
        this.codecs = Map.copyOf(codecs);
    }

    public void write(ByteBuf buffer, Type type, Object value) {
        requireCodec(type).write(buffer, value);
    }

    public Object read(ByteBuf buffer, Type type) {
        return requireCodec(type).read(buffer);
    }

    public boolean supports(Type type) {
        return codecs.containsKey(type);
    }

    public int compiledTypes() {
        return codecs.size();
    }

    private RuntimeCodec requireCodec(Type type) {
        RuntimeCodec codec = codecs.get(type);
        if (codec != null) {
            return codec;
        }
        throw new SerializationException(
                "Type " + type.getTypeName()
                        + " was not compiled during Pulsar bootstrap. Register it with .serializable(...) before start/connect."
        );
    }
}
