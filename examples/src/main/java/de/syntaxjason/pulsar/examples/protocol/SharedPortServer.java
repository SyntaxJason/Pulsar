package de.syntaxjason.pulsar.examples.protocol;

import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.examples.shared.ChatPacket;

/** Accepts ordinary Pulsar clients and HTTP requests on the same TCP port. */
public final class SharedPortServer {
    private SharedPortServer() { }

    public static void main(String[] args) {
        var server = Pulsar.server().port(8080)
                .protocol(new HttpStatusProtocol())
                .listener(new Listener() {
                    @EventHandler
                    public void onChat(PacketReceiveEvent<ChatPacket> event) {
                        event.connection().send(event.packet());
                    }
                }).start();
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        System.out.println("Pulsar and HTTP /health listening on port " + server.port());
    }
}
