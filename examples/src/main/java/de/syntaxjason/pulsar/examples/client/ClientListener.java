package de.syntaxjason.pulsar.examples.client;

import de.syntaxjason.pulsar.event.EventHandler;
import de.syntaxjason.pulsar.event.Listener;
import de.syntaxjason.pulsar.event.cache.CachePutEvent;
import de.syntaxjason.pulsar.event.packet.PacketReceiveEvent;
import de.syntaxjason.pulsar.examples.shared.ChatPacket;
import de.syntaxjason.pulsar.examples.shared.PlayerData;

import java.util.UUID;

public final class ClientListener implements Listener {

    @EventHandler
    public void onChat(PacketReceiveEvent<ChatPacket> event) {
        ChatPacket packet = event.packet();
        System.out.println(packet.sender() + ": " + packet.message());
    }

    @EventHandler
    public void onPlayerCacheUpdate(CachePutEvent<UUID, PlayerData> event) {
        if (!event.cache().equals("players")) {
            return;
        }

        System.out.println("Network cache update: " + event.value());
    }
}
