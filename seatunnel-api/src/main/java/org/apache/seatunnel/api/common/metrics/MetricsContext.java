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

package org.apache.seatunnel.api.common.metrics;

public interface MetricsContext {

    /**
     * registers a {@link ThreadSafeCounter} with SeaTunnel.
     *
     * @param name name of the counter
     * @return the created counter
     */
    Counter counter(String name);

    /**
     * Registers a {@link Counter} with SeaTunnel.
     *
     * @param name name of the counter
     * @param counter counter to register
     * @param <C> counter type
     * @return the given counter
     */
    <C extends Counter> C counter(String name, C counter);

    /**
     * Registers a {@link ThreadSafeQPSMeter} with SeaTunnel.
     *
     * @param name name of the meter
     * @return the registered meter
     */
    Meter meter(String name);

    /**
     * Registers a new {@link Meter} with SeaTunnel.
     *
     * @param name name of the meter
     * @param meter meter to register
     * @param <M> meter type
     * @return the registered meter
     */
    <M extends Meter> M meter(String name, M meter);
}
