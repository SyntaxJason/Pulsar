package de.syntaxjason.pulsar.logging;

public enum LogLevel {

    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    OFF;

    public boolean allows(LogLevel level) {
        return this != OFF && level.ordinal() >= ordinal();
    }
}
