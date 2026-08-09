package de.syntaxjason.pulsar.event;

public interface Cancellable {

    boolean cancelled();

    void cancelled(boolean cancelled);

    default void cancel() {
        cancelled(true);
    }
}
