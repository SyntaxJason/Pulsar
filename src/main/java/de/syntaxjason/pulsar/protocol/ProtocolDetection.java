package de.syntaxjason.pulsar.protocol;

/** Result of inspecting the currently available connection prefix. */
public enum ProtocolDetection {
    MATCH,
    NEED_MORE_DATA,
    NO_MATCH
}
