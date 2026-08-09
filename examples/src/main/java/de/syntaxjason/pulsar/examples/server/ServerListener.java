package de.syntaxjason.pulsar.examples.server;

import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.cache.CachePutEvent;
import de.syntaxjason.pulsar.event.connection.ConnectionCloseEvent;
import de.syntaxjason.pulsar.event.connection.ConnectionOpenEvent;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.examples.shared.ChatPacket;
import de.syntaxjason.pulsar.examples.shared.LocationPacket;
import de.syntaxjason.pulsar.examples.shared.PlayerData;

import java.util.UUID;

public final class ServerListener implements Listener {

    @EventHandler
    public void onConnect(ConnectionOpenEvent event) {
        System.out.println("Connected: " + event.connection().remoteAddress());
    }

    @EventHandler
    public void onDisconnect(ConnectionCloseEvent event) {
        System.out.println("Disconnected: " + event.connection().remoteAddress());
    }

    @EventHandler
    public void onChat(PacketReceiveEvent<ChatPacket> event) {
        ChatPacket packet = event.packet();
        System.out.println(packet.sender() + ": " + packet.message());

        event.connection().send(new ChatPacket(
                "Server",
                "Received your message, " + packet.sender() + "!"
        ));
    }

    @EventHandler
    public void onLocation(PacketReceiveEvent<LocationPacket> event) {
        LocationPacket packet = event.packet();
        System.out.println(packet.player() + " is at " + packet.location());
    }

    @EventHandler
    public void onPlayerCacheUpdate(CachePutEvent<UUID, PlayerData> event) {
        if (!event.cache().equals("players")) {
            return;
        }

        System.out.println("Player cache updated: " + event.key() + " -> " + event.value());
    }
}
