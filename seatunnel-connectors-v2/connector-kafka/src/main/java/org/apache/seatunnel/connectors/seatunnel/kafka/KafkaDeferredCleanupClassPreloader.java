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

import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kafka.exception.KafkaConnectorException;

import java.util.ArrayList;
import java.util.List;

/** Preloads Kafka cleanup-only classes before Flink recycles the connector classloader. */
public final class KafkaDeferredCleanupClassPreloader {

    private static final String[] DEFERRED_CLEANUP_CLASS_NAMES = {
        "org.apache.kafka.common.network.Selector$CloseMode"
    };

    private KafkaDeferredCleanupClassPreloader() {}

    /**
     * Kafka may initialize these runtime helpers lazily from cleanup threads after the job already
     * finished. Loading them eagerly avoids cleanup-time {@code NoClassDefFoundError} once Flink
     * starts recycling the user-code classloader.
     */
    public static List<Class<?>> preloadDeferredCleanupClasses() {
        ClassLoader classLoader = KafkaDeferredCleanupClassPreloader.class.getClassLoader();
        List<Class<?>> loadedClasses = new ArrayList<>(DEFERRED_CLEANUP_CLASS_NAMES.length);
        for (String className : DEFERRED_CLEANUP_CLASS_NAMES) {
            try {
                loadedClasses.add(Class.forName(className, false, classLoader));
            } catch (ClassNotFoundException e) {
                throw new KafkaConnectorException(
                        KafkaConnectorErrorCode.VERSION_INCOMPATIBLE,
                        String.format(
                                "Failed to preload Kafka cleanup runtime class '%s'.", className),
                        e);
            }
        }
        return loadedClasses;
    }
}
