package de.syntaxjason.pulsar.logging;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumSet;
import java.util.Set;

public final class ConsolePulsarLogger implements PulsarLogger {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private volatile LogLevel level;
    private final Set<LogCategory> categories;

    public ConsolePulsarLogger() {
        this(LogLevel.INFO);
    }

    public ConsolePulsarLogger(LogLevel level) {
        this(level, EnumSet.allOf(LogCategory.class));
    }

    public ConsolePulsarLogger(LogLevel level, Set<LogCategory> categories) {
        this.level = level;
        this.categories = categories.isEmpty()
                ? EnumSet.noneOf(LogCategory.class)
                : EnumSet.copyOf(categories);
    }

    public ConsolePulsarLogger level(LogLevel level) {
        this.level = level;
        return this;
    }

    public ConsolePulsarLogger enable(LogCategory category) {
        categories.add(category);
        return this;
    }

    public ConsolePulsarLogger disable(LogCategory category) {
        categories.remove(category);
        return this;
    }

    @Override
    public boolean enabled(LogLevel level, LogCategory category) {
        return categories.contains(category) && this.level.allows(level);
    }

    @Override
    public void log(LogLevel level, LogCategory category, String message, Throwable throwable) {
        if (!enabled(level, category)) {
            return;
        }

        String line = TIME.format(LocalTime.now())
                + " " + pad(level.name(), 5)
                + " [Pulsar/" + category.name() + "] "
                + message;

        if (level.ordinal() >= LogLevel.WARN.ordinal()) {
            System.err.println(line);
        } else {
            System.out.println(line);
        }

        if (throwable != null) {
            throwable.printStackTrace(level.ordinal() >= LogLevel.WARN.ordinal() ? System.err : System.out);
        }
    }

    private String pad(String value, int size) {
        if (value.length() >= size) {
            return value;
        }
        return value + " ".repeat(size - value.length());
    }
}
