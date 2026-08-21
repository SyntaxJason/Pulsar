package de.syntaxjason.pulsar.examples.server;

import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.cache.NetworkCache;
import de.syntaxjason.pulsar.examples.shared.ChatPacket;
import de.syntaxjason.pulsar.examples.shared.Location;
import de.syntaxjason.pulsar.examples.shared.LocationPacket;
import de.syntaxjason.pulsar.examples.shared.LocationSerializer;
import de.syntaxjason.pulsar.examples.shared.PlayerData;
import de.syntaxjason.pulsar.server.PulsarServer;

import java.util.UUID;

public final class ServerExample {

    private ServerExample() {
    }

    public static void main(String[] args) {
        PulsarServer server = Pulsar.server()
                .port(8080)
                .packets(ChatPacket.class, LocationPacket.class)
                .serializable(PlayerData.class)
                .serializer(Location.class, new LocationSerializer())
                .listener(new ServerListener())
                .start();

        NetworkCache<UUID, PlayerData> players = server.cache(
                "players",
                UUID.class,
                PlayerData.class
        );

        UUID examplePlayer = UUID.randomUUID();
        players.put(examplePlayer, new PlayerData(
                examplePlayer,
                "SyntaxJason",
                1_000
        ));

        System.out.println("Pulsar listening on port " + server.port());
        System.out.println("The players cache entry above lives FOREVER until replaced or removed.");
    }
}
