package org.apache.seatunnel.engine.server.storage;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.EventListener;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface MapStorage<K, V> {
    V putIfAbsent(K key, V value);

    V compute(K key, BiFunction<K, V, V> remappingFunction);

    void remove(Object key);

    V get(Object key);

    void put(K key, V value);

    void set(K key, V value);

    Set<Map.Entry<K, V>> entrySet();

    void forEach(BiConsumer<? super K, ? super V> action);

    UUID addEntryListener(@Nonnull EventListener listener, boolean includeValue);

    Collection<V> values();

    V getOrDefault(Object key, V defaultValue);

    V computeIfAbsent(@Nonnull K key, @Nonnull Function<? super K, ? extends V> func);

    V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeUnit);

    boolean isEmpty();

    boolean containsKey(@Nonnull Object key);

    int size();
}
