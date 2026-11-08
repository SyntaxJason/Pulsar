package de.syntaxjason.pulsar.internal.event;

import de.syntaxjason.pulsar.event.*;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SimpleEventBusTest {
    record Ping() implements Event { }
    record First() implements Packet { }
    record Second() implements Packet { }

    @Test
    void registrationAndRemovalInvalidatePreviouslyWarmedDispatch() {
        SimpleEventBus bus = new SimpleEventBus();
        AtomicInteger calls = new AtomicInteger();
        Listener listener = new Listener() {
            @EventHandler public void ping(Ping event) { calls.incrementAndGet(); }
        };
        bus.fire(new Ping());
        bus.register(listener);
        bus.fire(new Ping());
        assertEquals(1, calls.get());
        bus.unregister(listener);
        bus.fire(new Ping());
        assertEquals(1, calls.get());
        bus.register(listener);
        bus.register(listener);
        bus.freeze();
        bus.fire(new Ping());
        assertEquals(2, calls.get());
        assertEquals(1, bus.handlerCount());
        assertThrows(IllegalStateException.class, () -> bus.unregister(listener));
        assertThrows(IllegalStateException.class, () -> bus.register(listener));
    }

    @Test
    void typedPacketListenersOnlyReceiveMatchingPackets() {
        SimpleEventBus bus = new SimpleEventBus();
        AtomicInteger calls = new AtomicInteger();
        bus.register(new Listener() {
            @EventHandler public void receive(PacketReceiveEvent<First> event) { calls.incrementAndGet(); }
        });
        bus.freeze();
        bus.fire(new PacketReceiveEvent<>(null, new Second()));
        bus.fire(new PacketReceiveEvent<>(null, new First()));
        assertEquals(1, calls.get());
        assertEquals(List.of(First.class), List.copyOf(bus.packetTypes()));
    }

    @Test
    void priorityAndCancellationAreRespected() {
        class CancelEvent implements Event, Cancellable {
            boolean cancelled;
            @Override public boolean cancelled() { return cancelled; }
            @Override public void cancelled(boolean value) { cancelled = value; }
        }
        SimpleEventBus bus = new SimpleEventBus();
        List<String> calls = new ArrayList<>();
        bus.register(new Listener() {
            @EventHandler(priority = EventPriority.MONITOR) public void monitor(CancelEvent event) { calls.add("monitor"); }
            @EventHandler(priority = EventPriority.HIGH, ignoreCancelled = true) public void skipped(CancelEvent event) { fail("cancelled"); }
            @EventHandler(priority = EventPriority.LOW) public void cancel(CancelEvent event) { calls.add("cancel"); event.cancelled(true); }
        });
        bus.freeze();
        bus.fire(new CancelEvent());
        assertEquals(List.of("cancel", "monitor"), calls);
    }

    @Test
    void invalidStaticHandlersFailAtRegistrationWithUsefulError() {
        SimpleEventBus bus = new SimpleEventBus();
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> bus.register(new StaticListener()));
        assertTrue(failure.getMessage().contains("static"));
        assertEquals(0, bus.handlerCount());
    }

    static class StaticListener implements Listener {
        @EventHandler public static void ping(Ping event) { }
    }
}
