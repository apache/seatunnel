/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.common.statestore.counter;

/**
 * Store for counter-like state.
 *
 * <p>This interface is intended for states with explicit increment semantics, such as checkpoint ID
 * counters, instead of exposing a generic {@code compute()} operation.
 *
 * @param <K> key type
 */
public interface CounterStateStore<K> {

    /**
     * Atomically initializes the counter with the given value if no value is currently stored.
     *
     * @param key key to initialize
     * @param initialValue initial counter value to store when absent
     * @return {@code true} if the counter was initialized by this call, {@code false} if it already
     *     existed
     */
    boolean initializeIfAbsent(K key, long initialValue);

    /**
     * Returns the current counter value.
     *
     * @param key key to look up
     * @return stored value, or {@code null} if absent
     */
    Long get(K key);

    /**
     * Increments the counter by one and returns the updated value.
     *
     * @param key key to increment
     * @return updated value, or {@code null} if absent
     * @implSpec Implementations must preserve the checkpoint-counter contract for absent keys: they
     *     must not auto-initialize the counter and must return {@code null} when the key is absent.
     */
    Long incrementAndGet(K key);

    /**
     * Adds the given delta to the counter and returns the updated value.
     *
     * @param key key to update
     * @param delta value to add
     * @return updated value, or {@code null} if absent
     * @implSpec Implementations must not auto-initialize absent counters.
     */
    Long addAndGet(K key, long delta);

    /**
     * Overwrites the counter with a specific value.
     *
     * @param key key to store
     * @param value value to store
     */
    void set(K key, long value);

    /**
     * Removes the counter.
     *
     * @param key key to remove
     */
    void remove(K key);
}
