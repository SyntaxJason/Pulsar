package de.syntaxjason.pulsar.internal.event;

import de.syntaxjason.pulsar.event.Event;

@FunctionalInterface
interface EventInvoker {

    void invoke(Event event) throws Throwable;
}
