package de.syntaxjason.pulsar.examples.shared;

import de.syntaxjason.pulsar.serialization.SerializationReader;
import de.syntaxjason.pulsar.serialization.SerializationWriter;
import de.syntaxjason.pulsar.serialization.TypeSerializer;

public final class LocationSerializer implements TypeSerializer<Location> {

    @Override
    public void write(SerializationWriter writer, Location value) {
        writer.writeString(value.world());
        writer.writeDouble(value.x());
        writer.writeDouble(value.y());
        writer.writeDouble(value.z());
    }

    @Override
    public Location read(SerializationReader reader) {
        return new Location(
                reader.readString(),
                reader.readDouble(),
                reader.readDouble(),
                reader.readDouble()
        );
    }
}
