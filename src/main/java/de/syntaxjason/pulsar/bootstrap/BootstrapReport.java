package de.syntaxjason.pulsar.bootstrap;

public record BootstrapReport(
        int packets,
        int serializers,
        int serializableTypes,
        int listeners,
        int eventHandlers,
        long registrationNanos,
        long compilationNanos,
        long validationNanos,
        long totalNanos
) {

    public double totalMillis() {
        return totalNanos / 1_000_000.0;
    }

    public double registrationMillis() {
        return registrationNanos / 1_000_000.0;
    }

    public double compilationMillis() {
        return compilationNanos / 1_000_000.0;
    }

    public double validationMillis() {
        return validationNanos / 1_000_000.0;
    }
}
