package org.apache.seatunnel.engine.server.storage;

import com.hazelcast.core.HazelcastInstance;

import java.util.Objects;

public class MapManager {
    private static MapFactory mapFactory;

    public static void init(HazelcastInstance hazelcastInstance) {
        Objects.requireNonNull(hazelcastInstance, "hazelcastInstance");
        if (mapFactory == null) {
            mapFactory = new IMapFactory(hazelcastInstance);
        }
    }

    public static <K, V> MapStorage<K, V> getMap(String mapName) {
        if (mapFactory == null) {
            throw new IllegalStateException("MapManager is not initialized");
        }
        return mapFactory.getMap(mapName);
    }
}
