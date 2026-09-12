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

package org.apache.seatunnel.connectors.seatunnel.kafka;

import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;

/** Utility methods for executing Kafka client operations inside the connector classloader. */
public final class KafkaClientUtils {

    private KafkaClientUtils() {}

    /**
     * Runs a Kafka client action with the connector classloader as the thread context classloader.
     *
     * <p>Kafka clients can lazily resolve cleanup implementation classes while closing producers,
     * consumers, and admin clients. SeaTunnel may call connector cleanup from engine threads whose
     * context classloader no longer points at the Kafka connector, so cleanup must restore the
     * connector classloader instead of preloading Kafka private classes by name.
     */
    public static void runWithConnectorClassLoader(KafkaClientAction action) {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(KafkaClientUtils.class.getClassLoader())) {
            action.run();
        }
    }

    /** Action executed while the Kafka connector classloader is installed. */
    @FunctionalInterface
    public interface KafkaClientAction {

        void run();
    }
}
