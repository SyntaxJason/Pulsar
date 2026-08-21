package de.syntaxjason.pulsar.examples.client;

import de.syntaxjason.pulsar.Pulsar;
import de.syntaxjason.pulsar.cache.NetworkCache;
import de.syntaxjason.pulsar.client.PulsarClient;
import de.syntaxjason.pulsar.examples.shared.ChatPacket;
import de.syntaxjason.pulsar.examples.shared.Location;
import de.syntaxjason.pulsar.examples.shared.LocationPacket;
import de.syntaxjason.pulsar.examples.shared.LocationSerializer;
import de.syntaxjason.pulsar.examples.shared.PlayerData;

import java.time.Duration;
import java.util.UUID;

public final class ClientExample {

    private ClientExample() {
    }

    public static void main(String[] args) {
        PulsarClient client = Pulsar.client()
                .address("127.0.0.1", 8080)
                .packets(ChatPacket.class, LocationPacket.class)
                .serializable(PlayerData.class)
                .serializer(Location.class, new LocationSerializer())
                .listener(new ClientListener())
                .connect();

        NetworkCache<UUID, PlayerData> players = client.cache(
                "players",
                UUID.class,
                PlayerData.class
        );

        client.send(new ChatPacket("Client", "Moin from Pulsar!"));
        client.send(new LocationPacket(
                "Client",
                new Location("world", 100.5, 64.0, -32.25)
        ));

        UUID playerId = UUID.randomUUID();

        // No TTL: this value remains until it is replaced, removed or the cache is cleared.
        players.put(playerId, new PlayerData(playerId, "ForeverPlayer", 500));

        // TTL is always explicit.
        UUID temporaryPlayerId = UUID.randomUUID();
        players.put(
                temporaryPlayerId,
                new PlayerData(temporaryPlayerId, "TemporaryPlayer", 10),
                Duration.ofSeconds(30)
        );
    }
}
