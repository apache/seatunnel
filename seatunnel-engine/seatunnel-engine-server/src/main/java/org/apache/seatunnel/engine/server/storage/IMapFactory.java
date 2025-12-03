package org.apache.seatunnel.engine.server.storage;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class IMapFactory implements MapFactory {
    private final HazelcastInstance hazelcastInstance;

    public IMapFactory(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public <K, V> MapStorage<K, V> getMap(String mapName) {
        IMap<K, V> iMap = hazelcastInstance.getMap(mapName);
        return new IMapStorage<>(iMap);
    }
}
