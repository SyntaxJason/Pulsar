package de.syntaxjason.pulsar.internal.bootstrap;

import de.syntaxjason.pulsar.bootstrap.BootstrapReport;
import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;

public record BootstrapArtifacts(
        SimpleEventBus events,
        BinarySerializer serializer,
        BootstrapReport report
) {
}
