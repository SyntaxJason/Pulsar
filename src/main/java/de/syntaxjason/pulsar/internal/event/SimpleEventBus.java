package de.syntaxjason.pulsar.internal.event;

import de.syntaxjason.pulsar.event.Cancellable;
import de.syntaxjason.pulsar.event.Event;
import de.syntaxjason.pulsar.event.EventBus;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.logging.LogCategory;
import de.syntaxjason.pulsar.logging.NoOpPulsarLogger;
import de.syntaxjason.pulsar.logging.PulsarLogger;
import de.syntaxjason.pulsar.packet.Packet;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

public final class SimpleEventBus implements EventBus {

    private final List<RegisteredHandler> handlers = new CopyOnWriteArrayList<>();
    private final Set<Listener> listeners = new LinkedHashSet<>();
    private final Set<Class<? extends Packet>> packetTypes = new LinkedHashSet<>();
    private volatile ClassValue<RegisteredHandler[]> dispatch = newDispatchCache();
    private final PulsarLogger logger;
    private volatile boolean frozen;

    public SimpleEventBus() {
        this(NoOpPulsarLogger.INSTANCE);
    }

    public SimpleEventBus(PulsarLogger logger) {
        this.logger = logger;
    }

    @Override
    public synchronized void register(Listener listener) {
        ensureMutable();
        if (listener == null) {
            throw new IllegalArgumentException("Listener must not be null");
        }

        List<RegisteredHandler> compiled = new ArrayList<>();
        MethodHandles.Lookup lookup = MethodHandles.lookup();

        for (Method method : listener.getClass().getDeclaredMethods()) {
            EventHandler annotation = method.getAnnotation(EventHandler.class);
            if (annotation == null) {
                continue;
            }

            if (Modifier.isStatic(method.getModifiers())) {
                throw new IllegalArgumentException("@EventHandler method must not be static: " + method);
            }
            if (method.getParameterCount() != 1) {
                throw new IllegalArgumentException("@EventHandler method must have exactly one parameter: " + method);
            }
            if (method.getReturnType() != void.class) {
                throw new IllegalArgumentException("@EventHandler method must return void: " + method);
            }

            Class<?> parameter = method.getParameterTypes()[0];
            if (!Event.class.isAssignableFrom(parameter)) {
                throw new IllegalArgumentException("@EventHandler parameter must implement Event: " + method);
            }

            if (!method.trySetAccessible()) {
                throw new IllegalArgumentException("Cannot access @EventHandler method: " + method);
            }

            Class<? extends Packet> packetFilter = resolvePacketFilter(method, parameter);
            try {
                MethodHandle handle = lookup.unreflect(method)
                        .bindTo(listener)
                        .asType(MethodType.methodType(void.class, Event.class));
                EventInvoker invoker = handle::invoke;

                compiled.add(new RegisteredHandler(
                        listener,
                        invoker,
                        parameter.asSubclass(Event.class),
                        packetFilter,
                        annotation.priority(),
                        annotation.ignoreCancelled(),
                        listener.getClass().getName() + "#" + method.getName()
                ));
            } catch (IllegalAccessException exception) {
                throw new IllegalArgumentException("Cannot compile @EventHandler method: " + method, exception);
            }

            logger.debug(LogCategory.EVENT,
                    "Compiled event handler " + listener.getClass().getName() + "#" + method.getName()
                            + "(" + parameter.getSimpleName() + ")");
        }

        unregisterInternal(listener);
        handlers.addAll(compiled);
        listeners.add(listener);
        handlers.sort(Comparator.comparing(RegisteredHandler::priority));
        rebuildPacketTypes();
        dispatch = newDispatchCache();
    }

    @Override
    public synchronized void unregister(Listener listener) {
        ensureMutable();
        unregisterInternal(listener);
        dispatch = newDispatchCache();
    }

    @Override
    public <T extends Event> T fire(T event) {
        for (RegisteredHandler handler : dispatch.get(event.getClass())) {
            if (handler.ignoreCancelled() && event instanceof Cancellable cancellable && cancellable.cancelled()) {
                continue;
            }
            if (!handler.acceptsPacket(event)) {
                continue;
            }

            try {
                handler.invoker().invoke(event);
            } catch (Throwable throwable) {
                throw new EventDispatchException(
                        "Failed to dispatch " + event.getClass().getName() + " to " + handler.source(),
                        throwable
                );
            }
        }
        return event;
    }

    public synchronized void freeze() {
        frozen = true;
        logger.debug(LogCategory.EVENT,
                "Event registry frozen with " + handlers.size() + " handlers from " + listeners.size() + " listeners");
    }

    public boolean frozen() {
        return frozen;
    }

    public int handlerCount() {
        return handlers.size();
    }

    public int listenerCount() {
        return listeners.size();
    }

    public Collection<Class<? extends Packet>> packetTypes() {
        return List.copyOf(packetTypes);
    }

    private ClassValue<RegisteredHandler[]> newDispatchCache() {
        return new ClassValue<>() {
            @Override
            protected RegisteredHandler[] computeValue(Class<?> type) {
                return handlers.stream()
                        .filter(handler -> handler.eventType().isAssignableFrom(type))
                        .toArray(RegisteredHandler[]::new);
            }
        };
    }

    private void unregisterInternal(Listener listener) {
        handlers.removeIf(handler -> handler.listener() == listener);
        listeners.remove(listener);
        rebuildPacketTypes();
    }

    private void rebuildPacketTypes() {
        packetTypes.clear();
        handlers.forEach(handler -> {
            if (handler.packetFilter() != null) {
                packetTypes.add(handler.packetFilter());
            }
        });
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

    private void ensureMutable() {
        if (frozen) {
            throw new IllegalStateException(
                    "Pulsar event registry is frozen. Register reflective listeners with the builder before start/connect."
            );
        }
    }
}
