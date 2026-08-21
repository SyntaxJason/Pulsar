package de.syntaxjason.pulsar.bootstrap;

public enum RuntimeState {

    NEW,
    REGISTERING,
    COMPILING,
    VALIDATING,
    FROZEN,
    RUNNING,
    CLOSED
}
