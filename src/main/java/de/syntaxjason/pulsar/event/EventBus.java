package de.syntaxjason.pulsar.event;

public interface EventBus {

    void register(Listener listener);

    void unregister(Listener listener);

    <T extends Event> T fire(T event);
}
