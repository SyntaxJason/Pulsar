package de.syntaxjason.pulsar.internal.event;

import de.syntaxjason.pulsar.event.Event;
import de.syntaxjason.pulsar.event.EventPriority;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;

import java.lang.reflect.Method;

record RegisteredHandler(
        Listener listener,
        Method method,
        Class<? extends Event> eventType,
        Class<? extends Packet> packetFilter,
        EventPriority priority,
        boolean ignoreCancelled
) {

    boolean acceptsPacket(Event event) {
        if (packetFilter == null) {
            return true;
        }
        if (!(event instanceof PacketReceiveEvent<?> packetEvent)) {
            return false;
        }
        return packetFilter.isInstance(packetEvent.packet());
    }
}
