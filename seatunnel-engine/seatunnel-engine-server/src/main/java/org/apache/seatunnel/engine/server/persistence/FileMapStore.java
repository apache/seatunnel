package org.apache.seatunnel.engine.server.persistence;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class FileMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {
    //TODO Wait for the file Kv storage development to complete

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
    }

    @Override
    public void destroy() {

    }

    @Override
    public void store(Object key, Object value) {
    }

    @Override
    public void storeAll(Map<Object, Object> map) {

    }

    @Override
    public void delete(Object key) {

    }

    @Override
    public void deleteAll(Collection<Object> keys) {

    }

    @Override
    public String load(Object key) {
        return null;
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        return null;
    }

    @Override
    public Iterable<Object> loadAllKeys() {
        return null;
    }

}
