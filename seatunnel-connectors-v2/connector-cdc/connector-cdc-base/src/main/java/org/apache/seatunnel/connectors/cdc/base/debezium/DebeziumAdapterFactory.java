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

package org.apache.seatunnel.connectors.cdc.base.debezium;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/** Factory for loading connector-specific Debezium adapters via ServiceLoader. */
public class DebeziumAdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DebeziumAdapterFactory.class);

    private static final Map<String, DebeziumAdapter> ADAPTERS = new ConcurrentHashMap<>();

    public static DebeziumAdapter getAdapter(String connectorType) {
        return ADAPTERS.computeIfAbsent(
                connectorType,
                type -> {
                    LOG.info("Loading DebeziumAdapter for connector type: {}", type);
                    ServiceLoader<DebeziumAdapter> loader =
                            ServiceLoader.load(DebeziumAdapter.class);

                    for (DebeziumAdapter adapter : loader) {
                        if (adapter.supports(type)) {
                            LOG.info(
                                    "Found DebeziumAdapter for {}: {} (Debezium version: {})",
                                    type,
                                    adapter.getClass().getName(),
                                    adapter.getDebeziumVersion());
                            return adapter;
                        }
                    }

                    throw new IllegalStateException(
                            "No DebeziumAdapter found for connector type: "
                                    + type
                                    + ". Ensure META-INF/services configuration is present.");
                });
    }

    public static void clearCache() {
        ADAPTERS.clear();
    }
}
