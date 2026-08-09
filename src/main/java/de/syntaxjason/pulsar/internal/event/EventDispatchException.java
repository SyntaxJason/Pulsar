package de.syntaxjason.pulsar.internal.event;

public final class EventDispatchException extends RuntimeException {

    public EventDispatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
