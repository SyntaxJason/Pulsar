package de.syntaxjason.pulsar.internal.event;

import de.syntaxjason.pulsar.event.Cancellable;
import de.syntaxjason.pulsar.event.Event;
import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.packet.Packet;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SimpleEventBus implements EventBus {

    private final List<RegisteredHandler> handlers = new CopyOnWriteArrayList<>();

    @Override
    public void register(Listener listener) {
        unregister(listener);

        for (Method method : listener.getClass().getDeclaredMethods()) {
            EventHandler annotation = method.getAnnotation(EventHandler.class);
            if (annotation == null) {
                continue;
            }

            if (method.getParameterCount() != 1) {
                throw new IllegalArgumentException("@EventHandler method must have exactly one parameter: " + method);
            }

            Class<?> parameter = method.getParameterTypes()[0];
            if (!Event.class.isAssignableFrom(parameter)) {
                throw new IllegalArgumentException("@EventHandler parameter must implement Event: " + method);
            }

            if (!method.trySetAccessible()) {
                throw new IllegalArgumentException("Cannot access @EventHandler method: " + method);
            }

            Class<? extends Packet> packetFilter = resolvePacketFilter(method, parameter);
            handlers.add(new RegisteredHandler(
                    listener,
                    method,
                    parameter.asSubclass(Event.class),
                    packetFilter,
                    annotation.priority(),
                    annotation.ignoreCancelled()
            ));
        }

        handlers.sort(Comparator.comparing(RegisteredHandler::priority));
    }

    @Override
    public void unregister(Listener listener) {
        handlers.removeIf(handler -> handler.listener() == listener);
    }

    @Override
    public <T extends Event> T fire(T event) {
        for (RegisteredHandler handler : handlers) {
            if (!handler.eventType().isAssignableFrom(event.getClass())) {
                continue;
            }

            if (handler.ignoreCancelled() && event instanceof Cancellable cancellable && cancellable.cancelled()) {
                continue;
            }

            if (!handler.acceptsPacket(event)) {
                continue;
            }

            try {
                handler.method().invoke(handler.listener(), event);
            } catch (ReflectiveOperationException exception) {
                throw new EventDispatchException("Failed to dispatch " + event.getClass().getName(), exception);
            }
        }
        return event;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Packet> resolvePacketFilter(Method method, Class<?> parameter) {
        if (parameter != PacketReceiveEvent.class) {
            return null;
        }

        Type generic = method.getGenericParameterTypes()[0];
        if (!(generic instanceof ParameterizedType parameterized)) {
            return null;
        }

        Type packetType = parameterized.getActualTypeArguments()[0];
        if (packetType instanceof Class<?> type && Packet.class.isAssignableFrom(type)) {
            return (Class<? extends Packet>) type;
        }

        return null;
    }
}
