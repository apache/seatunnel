package org.apache.seatunnel.engine.server.storage;

public interface MapFactory {
    <K, V> MapStorage<K, V> getMap(String mapName);
}
