package de.syntaxjason.pulsar.internal.serialization;

import io.netty.buffer.ByteBuf;

interface RuntimeCodec {

    void write(ByteBuf buffer, Object value);

    Object read(ByteBuf buffer);
}
