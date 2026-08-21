package de.syntaxjason.pulsar.internal.bootstrap;

import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.bootstrap.BootstrapReport;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.internal.event.SimpleEventBus;
import de.syntaxjason.pulsar.internal.serialization.BinarySerializer;
import de.syntaxjason.pulsar.internal.serialization.SerializerCompiler;
import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.Packet;
import de.syntaxjason.pulsar.packet.PacketRegistry;
import de.syntaxjason.pulsar.serialization.SerializerRegistry;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public final class PulsarBootstrap {

    private PulsarBootstrap() {
    }

    public static BootstrapArtifacts prepare(
            PacketRegistry packets,
            SerializerRegistry serializers,
            Collection<Listener> listeners,
            Collection<Type> serializableTypes,
            PulsarLogger logger
    ) {
        long started = System.nanoTime();
        logger.info(LogCategory.BOOTSTRAP, "Bootstrapping Pulsar " + Pulsar.VERSION);

        long registrationStarted = System.nanoTime();
        SimpleEventBus events = new SimpleEventBus(logger);
        listeners.forEach(events::register);
        events.packetTypes().forEach(packets::register);
        long registrationNanos = System.nanoTime() - registrationStarted;

        long compilationStarted = System.nanoTime();
        SerializerCompiler compiler = new SerializerCompiler(serializers, logger);
        Set<Type> types = new LinkedHashSet<>(serializableTypes);
        types.addAll(serializers.types());
        types.addAll(packets.types());
        types.forEach(compiler::compile);
        BinarySerializer binarySerializer = compiler.freeze();
        long compilationNanos = System.nanoTime() - compilationStarted;

        long validationStarted = System.nanoTime();
        for (Class<? extends Packet> packet : packets.types()) {
            if (!binarySerializer.supports(packet)) {
                throw new IllegalStateException("No compiled codec for packet " + packet.getName());
            }
        }
        long validationNanos = System.nanoTime() - validationStarted;

        packets.freeze();
        serializers.freeze();
        events.freeze();

        long totalNanos = System.nanoTime() - started;
        BootstrapReport report = new BootstrapReport(
                packets.size(),
                serializers.size(),
                compiler.size(),
                events.listenerCount(),
                events.handlerCount(),
                registrationNanos,
                compilationNanos,
                validationNanos,
                totalNanos
        );

        logger.info(LogCategory.BOOTSTRAP,
                "Ready: " + report.packets() + " packets, "
                        + report.eventHandlers() + " handlers, "
                        + report.serializableTypes() + " codec plans in "
                        + String.format("%.2f", report.totalMillis()) + " ms; runtime reflection disabled");

        return new BootstrapArtifacts(events, binarySerializer, report);
    }
}
