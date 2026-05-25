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

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for creating dirty record collectors.
 *
 * <p>Built-in types ("log", "noop") are instantiated directly without SPI. Custom collector types
 * are discovered via {@link FactoryUtil#discoverFactories}.
 */
@Slf4j
public class DirtyRecordCollectorFactory {

    private static final Map<String, DirtyRecordCollectorProvider> PROVIDERS =
            new ConcurrentHashMap<>();

    private static volatile boolean discovered = false;

    static {
        PROVIDERS.put("log", new LogDirtyRecordCollectorProvider());
    }

    private static void discoverCustomProviders() {
        if (discovered) {
            return;
        }
        synchronized (DirtyRecordCollectorFactory.class) {
            if (discovered) {
                return;
            }
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try {
                List<DirtyRecordCollectorProvider> providers =
                        FactoryUtil.discoverFactories(
                                classLoader, DirtyRecordCollectorProvider.class);
                for (DirtyRecordCollectorProvider provider : providers) {
                    String type = provider.getType().toLowerCase();
                    PROVIDERS.putIfAbsent(type, provider);
                }
            } catch (Exception e) {
                log.warn("Failed to discover custom DirtyRecordCollectorProviders via SPI", e);
            }
            log.info(
                    "DirtyRecordCollectorProvider discovery complete. Available types: {}",
                    PROVIDERS.keySet());
            discovered = true;
        }
    }

    public static DirtyRecordCollector createCollector(Config config) {
        if (config == null || !config.hasPath("type")) {
            return NoOpDirtyRecordCollector.INSTANCE;
        }

        String type = config.getString("type").toLowerCase();

        if ("noop".equals(type) || "none".equals(type)) {
            return NoOpDirtyRecordCollector.INSTANCE;
        }

        DirtyRecordCollectorProvider provider = PROVIDERS.get(type);
        if (provider == null) {
            discoverCustomProviders();
            provider = PROVIDERS.get(type);
        }

        if (provider == null) {
            throw new IllegalArgumentException(
                    "Unknown dirty.collector type '"
                            + type
                            + "'. Available built-in types: "
                            + PROVIDERS.keySet()
                            + ". Register a custom DirtyRecordCollectorProvider via SPI.");
        }

        DirtyRecordCollector collector = provider.createCollector();
        try {
            collector.init(config);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialize dirty record collector of type '" + type + "'", e);
        }

        log.info(
                "Created dirty record collector: type='{}', class={}",
                type,
                collector.getClass().getSimpleName());
        return collector;
    }
}
