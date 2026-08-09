package de.syntaxjason.pulsar.internal.netty;

enum FrameType {
    PACKET(1),
    CACHE_SUBSCRIBE(10),
    CACHE_SNAPSHOT(11),
    CACHE_PUT(12),
    CACHE_REMOVE(13),
    CACHE_CLEAR(14);

    private final int id;

    FrameType(int id) {
        this.id = id;
    }

    int id() {
        return id;
    }

    static FrameType byId(int id) {
        for (FrameType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new ProtocolException("Unknown frame type: " + id);
    }
}
