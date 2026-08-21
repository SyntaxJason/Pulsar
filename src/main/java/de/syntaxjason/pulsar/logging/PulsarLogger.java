package de.syntaxjason.pulsar.logging;

public interface PulsarLogger {

    boolean enabled(LogLevel level, LogCategory category);

    void log(LogLevel level, LogCategory category, String message, Throwable throwable);

    default void log(LogLevel level, LogCategory category, String message) {
        log(level, category, message, null);
    }

    default void trace(LogCategory category, String message) {
        if (enabled(LogLevel.TRACE, category)) {
            log(LogLevel.TRACE, category, message, null);
        }
    }

    default void debug(LogCategory category, String message) {
        if (enabled(LogLevel.DEBUG, category)) {
            log(LogLevel.DEBUG, category, message, null);
        }
    }

    default void info(LogCategory category, String message) {
        if (enabled(LogLevel.INFO, category)) {
            log(LogLevel.INFO, category, message, null);
        }
    }

    default void warn(LogCategory category, String message) {
        if (enabled(LogLevel.WARN, category)) {
            log(LogLevel.WARN, category, message, null);
        }
    }

    default void error(LogCategory category, String message, Throwable throwable) {
        if (enabled(LogLevel.ERROR, category)) {
            log(LogLevel.ERROR, category, message, throwable);
        }
    }
}
