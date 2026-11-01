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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.factory.FactoryUtil;

import java.util.Locale;

/**
 * Creates independently initialized collectors from explicit configuration. Custom providers are
 * discovered through the current invocation's context loader and never retained in a global cache.
 */
public class DirtyRecordCollectorFactory {

    /**
     * Creates an active collector or the explicitly selected no-op implementation. A present block
     * must declare its policy so malformed configuration cannot silently disable dirty handling.
     */
    public static DirtyRecordCollector createCollector(Config config) {
        String type = collectorType(config);
        if ("noop".equals(type) || "none".equals(type)) {
            return NoOpDirtyRecordCollector.INSTANCE;
        }
        DirtyRecordCollector collector;
        if ("log".equals(type)) {
            collector = new LogDirtyRecordCollector();
        } else {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            if (classLoader == null) {
                classLoader = DirtyRecordCollectorFactory.class.getClassLoader();
            }
            DirtyRecordCollectorProvider selected = null;
            for (DirtyRecordCollectorProvider provider :
                    FactoryUtil.discoverFactories(
                            classLoader, DirtyRecordCollectorProvider.class)) {
                String providerType = provider.getType();
                if (providerType != null
                        && type.equals(providerType.trim().toLowerCase(Locale.ROOT))) {
                    if (selected != null) {
                        throw new IllegalArgumentException(
                                "Multiple dirty.collector providers for type '"
                                        + type
                                        + "': "
                                        + selected.getClass().getName()
                                        + ", "
                                        + provider.getClass().getName());
                    }
                    selected = provider;
                }
            }
            if (selected == null) {
                throw new IllegalArgumentException(
                        "Unknown dirty.collector type '"
                                + type
                                + "'. Register a DirtyRecordCollectorProvider via SPI.");
            }
            collector = selected.createCollector();
            if (collector == null) {
                throw new IllegalArgumentException(
                        "dirty.collector provider returned null for type '" + type + "'.");
            }
        }
        try {
            collector.init(config);
            return collector;
        } catch (Throwable failure) {
            try {
                collector.close();
            } catch (Throwable closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw new IllegalArgumentException(
                    "Failed to initialize dirty record collector of type '" + type + "'", failure);
        }
    }

    /**
     * Parses the configured policy without constructing providers or allocating resources. Planning
     * guards share this validation so malformed blocks cannot bypass the factory's strict contract.
     */
    static String collectorType(Config config) {
        if (config == null) {
            return "noop";
        }
        if (!config.hasPath(DirtyCollectorConfigProcessor.DIRTY_TYPE.key())) {
            throw new IllegalArgumentException(
                    "dirty.collector is configured but missing required 'type' field.");
        }
        String type =
                config.getString(DirtyCollectorConfigProcessor.DIRTY_TYPE.key())
                        .trim()
                        .toLowerCase(Locale.ROOT);
        if (type.isEmpty()) {
            throw new IllegalArgumentException("dirty.collector type must not be blank.");
        }
        return type;
    }
}
