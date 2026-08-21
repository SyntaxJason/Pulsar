package de.syntaxjason.pulsar.logging;

public enum NoOpPulsarLogger implements PulsarLogger {

    INSTANCE;

    @Override
    public boolean enabled(LogLevel level, LogCategory category) {
        return false;
    }

    @Override
    public void log(LogLevel level, LogCategory category, String message, Throwable throwable) {
    }
}
